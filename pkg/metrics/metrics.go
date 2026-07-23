// Package metrics, Shardlands'in Prometheus metriklerini TEK YERDE
// tanımlar.
//
// # Neden merkezî?
//
// Metrikleri kullanıldıkları yerde tanımlamak dağınık ama kolaydır.
// Merkezde toplamanın iki somut kazancı var:
//
//   - Ad ve etiket disiplini tek dosyadan denetlenir. Metrik adları
//     bir SÖZLEŞMEDİR: panolar, alarmlar ve sorgular onlara bağlanır;
//     dağınık tanımlarda "login_count" ile "logins_total" yan yana
//     yaşamaya başlar.
//   - KARDİNALİTE kararları görünür olur (aşağıya bak).
//
// # Kardinalite: metriklerin en pahalı hatası
//
// Prometheus'ta her benzersiz etiket KOMBİNASYONU ayrı bir zaman
// serisidir. `player_id` gibi bir etiket eklemek, oyuncu sayısı kadar
// seri demektir — 10 bin oyuncu, 10 bin seri, ve bu seriler oyuncular
// gittikten sonra da bellekte kalır.
//
// Bu yüzden buradaki hiçbir metrik oyuncu, oturum, maç ya da arena
// kimliği taşımaz. Kimlik başına soru sormak metriklerin işi değil;
// o soru LOG ve İZLEME (trace) katmanının işidir. Üç sinyalin iş
// bölümü tam olarak budur:
//
//	metrik  → "ne kadar, ne kadar hızlı, ne oranda bozuk?" (toplu)
//	log     → "bu tekil olayda ne oldu?"
//	trace   → "bu tekil istek hangi servislerde ne kadar durdu?"
//
// # Neden histogram, neden ortalama değil
//
// Ortalama gecikme yalan söyler: 100 istekten 99'u 5ms, biri 2 saniye
// sürerse ortalama 25ms çıkar ve "iyi" görünür — oysa bir kullanıcı
// 2 saniye bekledi. Histogram kova sayar; p50/p95/p99 sorulabilir.
// Kova sınırları ELLE seçilir, çünkü her metrik farklı ölçekte yaşar:
// giriş yolu milisaniye, dünya tick'i mikrosaniye mertebesinde.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Reg, bu sürecin metrik kaydı. Varsayılan global kayıt yerine kendi
// kaydımızı kullanıyoruz: hangi metriklerin dışarı çıktığı açıkça
// listelenmiş olsun ve kütüphanelerin habersiz eklediği metrikler
// panolara sızmasın.
var Reg = prometheus.NewRegistry()

var (
	// ---- Giriş yolu (RED: Rate, Errors, Duration) ----

	// LoginTotal, sonuç etiketiyle giriş sayısı.
	//
	// `result` etiketi SABİT bir kümeden gelir: ok | client_error |
	// rate_limited | shed | error. Serbest metin (örneğin hata mesajı)
	// koymak kardinaliteyi patlatırdı.
	LoginTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "shardlands_login_total",
		Help: "Giriş denemeleri, sonuca göre.",
	}, []string{"result"})

	// LoginDuration, giriş yolunun uçtan uca süresi (player gRPC
	// çağrısı dahil). Kovalar ağ atlaması olan bir yol için seçildi.
	LoginDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "shardlands_login_duration_seconds",
		Help:    "Giriş isteğinin uçtan uca süresi.",
		Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5},
	})

	// ---- Oturumlar (USE: Utilization) ----

	Sessions = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "shardlands_ws_sessions",
		Help: "Anlık açık WebSocket oturumu.",
	})

	// CommandsShed, hız sınırı yüzünden atılan komut sayısı.
	// Faz 4'te yazdığımız yük atmanın gözlenebilir hali: 429'lar
	// artıyorsa sistem ÇALIŞIYOR ama baskı altında demektir.
	CommandsShed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "shardlands_commands_shed_total",
		Help: "Hız sınırı nedeniyle atılan komut sayısı.",
	})

	// ---- Dünya döngüsü ----

	// WorldTickDuration, bir bölge tick'inin süresi.
	//
	// Hub'ın sağlığının EN DOĞRUDAN göstergesi bu: 20Hz'de koşan
	// döngünün bütçesi 50ms. Tick süresi bütçeye yaklaşıyorsa dünya
	// yavaşlıyor demektir ve bunu oyuncular hissetmeden önce görmek
	// isteriz. Kovalar mikrosaniye mertebesinde başlıyor.
	WorldTickDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "shardlands_world_tick_duration_seconds",
		Help:    "Bir bölge tick'inin süresi (bütçe: 50ms @ 20Hz).",
		Buckets: []float64{.00005, .0001, .00025, .0005, .001, .005, .01, .025, .05},
	})

	// ---- Maç saga'sı ----

	// MatchTotal, saga sonuçları. Telafi yollarının gerçekten
	// çalıştığını (ve ne sıklıkta çalıştığını) buradan görürüz.
	MatchTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "shardlands_match_total",
		Help: "Maç saga'sı sonuçları.",
	}, []string{"result"})

	// ---- Sır yönetimi ----

	// KeyRefreshTotal, Vault'tan anahtar tazeleme sonuçları.
	// Faz 6'da "tazeleme hatası ölümcül değil" dedik; o kararın bedeli
	// sessiz kalmasıdır — metrik onu görünür kılar.
	KeyRefreshTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "shardlands_key_refresh_total",
		Help: "İmzalama anahtarı tazeleme sonuçları.",
	}, []string{"result"})
)

func init() {
	Reg.MustRegister(
		// Go çalışma zamanı ve süreç metrikleri: GC duraklamaları,
		// goroutine sayısı, dosya tanıtıcıları. Uygulama metriklerinden
		// önce buraya bakılır — "yavaşlık" çoğu zaman GC ya da goroutine
		// sızıntısıdır.
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),

		LoginTotal, LoginDuration, Sessions, CommandsShed,
		WorldTickDuration, MatchTotal, KeyRefreshTotal,
	)
}

// Handler, /metrics uç noktası.
func Handler() http.Handler {
	return promhttp.HandlerFor(Reg, promhttp.HandlerOpts{
		// Toplama sırasında hata olursa 500 dönmek yerine hatayı
		// metrik olarak bildir: gözlem katmanı, gözlediği sistemi
		// düşürmemeli.
		ErrorHandling: promhttp.ContinueOnError,
	})
}
