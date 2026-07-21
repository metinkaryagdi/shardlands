# pkg/hashring — Consistent Hashing

Anahtarları (oyuncu/bölge id'leri) shard'lara minimal yeniden eşlemeyle
dağıtır. Faz 3'te bölge→shard eşlemesinin temeli.

## Neden naif modulo değil?

`hash(key) % N` basittir ama N değişince (shard ekle/çıkar) neredeyse
**tüm** anahtarların sahibi değişir. Stateful bir sistemde bu, her
oyuncunun aynı anda göç etmesi = kitlesel state transferi + kesinti.

Consistent hashing hem düğümleri hem anahtarları bir **halkaya** (0..2³²)
koyar; anahtar, saat yönünde ilk düğüme aittir. Bir düğüm eklendiğinde
yalnızca o düğümün yayına düşen anahtarlar (~1/N) taşınır; çıkarıldığında
yalnız onun anahtarları komşuya geçer. Gerisi **yerinde kalır**.

## Virtual node'lar (vnode)

Bir düğümü halkada tek noktaya koymak dengesiz yaylar → dengesiz yük
verir. Her düğümü V noktaya (vnode) dağıtmak hem yükü hem de değişimdeki
taşınmayı düzgünleştirir. V=100-200 tipik; büyüdükçe dağılım iyileşir,
bellek/GetN maliyeti artar.

## API

- `Add(nodes...)` / `Remove(node)` — topoloji değişimi.
- `Get(key) node` — anahtarın sahibi (saat yönünde ilk vnode).
- `GetN(key, n) []node` — ilk n FARKLI düğüm (replika yerleşimi:
  bir shard'ı Raft ile n düğüme çoğaltmak için — Faz 3'ün sonraki adımı).

Thread-safe değil: topoloji tek koordinatörden değişir (Faz 3'te bu
koordinatör, shard atamasını Raft ile replike edecek).

## Test edilen garantiler

- **Minimal remap**: düğüm eklenince taşınan oran ~1/(N+1) VE taşınanlar
  yalnız yeni düğüme gider (eski düğümler arası taşınma yok). Naif
  modulo'da bu oran ~1 olurdu — testte kanıtlı fark.
- **Denge**: 200 vnode ile 4 düğüm, 40k anahtarda her düğüm ortalamanın
  ±%30 bandında.
- Determinizm, boş halka, tek düğüm, GetN farklı-düğüm, idempotent Add.

## Learnings

- **vnode sayısı bir denge ayarıdır.** Az vnode → hızlı ama dengesiz;
  çok vnode → dengeli ama halka büyür. 200 pratik bir orta nokta.
- **Consistent hashing "minimal" der, "sıfır" demez.** Ekleme hâlâ ~1/N
  anahtar taşır; sıfır taşınma isteniyorsa açık atama tablosu (directory)
  gerekir — o da merkezi state (Raft) demek. Faz 3 ikisini birleştirecek:
  ring hızlı varsayılan eşleme, Raft ise otoriter atama/handoff kaydı.
