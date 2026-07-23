// Package vault, HashiCorp Vault'un yalnız bu projenin ihtiyaç duyduğu
// iki uç noktasını konuşan asgari bir istemcidir: Kubernetes ile giriş
// ve KV v2'den okuma.
//
// NEDEN RESMÎ İSTEMCİ DEĞİL? Projenin kuralı gereği (Faz 0'dan beri)
// mekanizmayı önce elle yazıyoruz. Burada öğrenilecek şey bir kütüphane
// API'si değil, "SIR SIFIRI" (secret zero) probleminin nasıl çözüldüğü —
// ve o çözüm iki HTTP çağrısında tamamen görünür hale geliyor.
//
// # Sır sıfırı problemi
//
// Uygulama sırları Vault'tan alacak. Peki Vault'a nasıl kimlik
// kanıtlayacak? Bir parola verirsek problemi bir adım öteledik: o parola
// nerede duracak? Sonsuz gerileme.
//
// Kubernetes çözümü şudur: Pod'un ServiceAccount token'ı zaten VARDIR,
// kubelet onu dosya sistemine bağlar ve kube-apiserver imzalamıştır.
// Uygulama o token'ı Vault'a sunar; Vault, TokenReview API'siyle
// apiserver'a "bu token gerçek mi, hangi ServiceAccount'a ait?" diye
// sorar ve cevaba göre Vault token'ı basar.
//
// Yani kimlik yine İŞ YÜKÜNÜN KENDİSİNDEN geliyor, paylaşılan bir
// sırdan değil. Faz 6'nın mesh adımındaki mTLS kimliğiyle (SPIFFE)
// birebir aynı fikir: "söylediğine güvenme, kanıtı iste" — ve kanıtı
// veren, iş yükünü zaten tanıyan kube-apiserver.
//
// # Bu istemcinin sınırları (dürüstçe)
//
//   - Yalnız KV v2 okur; yazma, dinamik sırlar, transit, PKI yok.
//   - Token yenileme (renew) yerine SÜRESİ DOLMADAN YENİDEN GİRİŞ
//     yapar. Daha basit ve bizim erişim düzenimizde (dakikada bir okuma)
//     maliyeti ihmal edilebilir.
//   - TLS doğrulaması http.Client'ın varsayılanına bırakılmıştır;
//     kümede Vault dev modunda düz HTTP konuşuyor. Üretimde Vault'un
//     kendi CA'sı taşınır — bu kurulumun bilinçli eksiği.
package vault

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"
)

// saTokenPath, kubelet'in Pod'a bağladığı ServiceAccount token'ı.
const saTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"

type Client struct {
	addr     string
	role     string
	jwtPath  string
	http     *http.Client
	mu       sync.Mutex
	token    string
	tokenExp time.Time
}

type Options struct {
	// Addr, Vault adresi (örn "http://vault.shardlands:8200").
	Addr string
	// Role, Vault'taki Kubernetes auth rolü.
	Role string
	// JWTPath boşsa Pod'un standart ServiceAccount token yolu.
	JWTPath string
	// HTTPClient boşsa 10sn zaman aşımlı varsayılan.
	HTTPClient *http.Client
}

func New(opts Options) (*Client, error) {
	if opts.Addr == "" {
		return nil, fmt.Errorf("vault: addr gerekli")
	}
	if opts.Role == "" {
		return nil, fmt.Errorf("vault: role gerekli")
	}
	c := &Client{
		addr:    opts.Addr,
		role:    opts.Role,
		jwtPath: opts.JWTPath,
		http:    opts.HTTPClient,
	}
	if c.jwtPath == "" {
		c.jwtPath = saTokenPath
	}
	if c.http == nil {
		c.http = &http.Client{Timeout: 10 * time.Second}
	}
	return c, nil
}

// login, ServiceAccount token'ını Vault token'ına çevirir.
func (c *Client) login(ctx context.Context) error {
	jwt, err := os.ReadFile(c.jwtPath)
	if err != nil {
		return fmt.Errorf("vault: serviceaccount token okunamadı: %w", err)
	}
	body, _ := json.Marshal(map[string]string{
		"role": c.role,
		"jwt":  string(bytes.TrimSpace(jwt)),
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.addr+"/v1/auth/kubernetes/login", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("vault: giriş isteği: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("vault: giriş başarısız: %s", resp.Status)
	}

	var out struct {
		Auth struct {
			ClientToken   string `json:"client_token"`
			LeaseDuration int    `json:"lease_duration"`
		} `json:"auth"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return fmt.Errorf("vault: giriş yanıtı: %w", err)
	}
	if out.Auth.ClientToken == "" {
		return fmt.Errorf("vault: giriş yanıtında token yok")
	}

	c.token = out.Auth.ClientToken
	// Süreden ÖNCE yenile: kiranın son anına kadar beklemek, saat
	// kayması ve ağ gecikmesiyle birleşince yetkisiz istek üretir.
	// Faz 3'teki kilit kirasında da aynı payı bırakmıştık.
	ttl := time.Duration(out.Auth.LeaseDuration) * time.Second
	if ttl <= 0 {
		ttl = time.Hour
	}
	c.tokenExp = time.Now().Add(ttl - ttl/5)
	return nil
}

// ensureToken, geçerli bir Vault token'ı olmasını garanti eder.
func (c *Client) ensureToken(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.token != "" && time.Now().Before(c.tokenExp) {
		return c.token, nil
	}
	if err := c.login(ctx); err != nil {
		return "", err
	}
	return c.token, nil
}

// ReadKV, KV v2 motorundan bir sırrın alanlarını okur.
//
// mount genelde "secret", path ise motor içindeki yol. KV v2'nin URL
// biçimi ilk bakışta şaşırtır: okuma /v1/<mount>/data/<path> adresinden
// yapılır ve cevap {"data":{"data":{...}}} biçiminde İKİ KAT sarılıdır —
// dıştaki zarf sürüm meta verisi taşır (KV v2 sürümlüdür, v1 değildi).
func (c *Client) ReadKV(ctx context.Context, mount, path string) (map[string]string, error) {
	token, err := c.ensureToken(ctx)
	if err != nil {
		return nil, err
	}
	url := fmt.Sprintf("%s/v1/%s/data/%s", c.addr, mount, path)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Vault-Token", token)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("vault: okuma isteği: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusForbidden {
		// Token süresi beklenmedik şekilde bitmiş olabilir: bir kez
		// yeniden giriş yapıp tekrar dene.
		c.mu.Lock()
		c.token = ""
		c.mu.Unlock()
		return nil, fmt.Errorf("vault: yetkisiz (403), token tazelenecek")
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("vault: okuma başarısız: %s", resp.Status)
	}

	var out struct {
		Data struct {
			Data map[string]any `json:"data"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("vault: okuma yanıtı: %w", err)
	}
	fields := make(map[string]string, len(out.Data.Data))
	for k, v := range out.Data.Data {
		if s, ok := v.(string); ok {
			fields[k] = s
		}
	}
	return fields, nil
}
