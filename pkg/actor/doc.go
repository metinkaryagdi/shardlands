// Package actor, Shardlands için sıfırdan yazılmış minimal bir actor
// framework'üdür.
//
// Model:
//   - Her aktör kendi goroutine'inde çalışır ve mesajlarını TEK TEK,
//     sıralı işler. Aktör durumuna (state) yalnızca kendi goroutine'i
//     dokunur; bu yüzden aktör içinde lock gerekmez.
//   - Mesajlar iki kuyruktan akar: kullanıcı mailbox'ı (bounded channel)
//     ve kontrol kuyruğu (unbounded). Kontrol mesajları (stop, çocuk
//     bildirimleri, escalation) her zaman kullanıcı mesajlarından önce
//     işlenir — dolu bir mailbox'ın arkasında Stop beklemez.
//   - Aktörler bir ağaç oluşturur: Spawn eden ebeveyn olur. Bir aktör
//     panic'lerse Props.Supervision stratejisi karar verir:
//     Restart (yeni instance, mailbox korunur), Resume (state korunur),
//     Stop veya Escalate (ebeveyne hata olarak yükselir).
//   - Bir aktör dururken önce çocukları durdurulur (aşağıdan yukarıya
//     temizlik), sonra PostStop çağrılır. Durmuş bir aktöre gönderilen
//     mesajlar dead letter sayacına düşer.
//
// Context yalnızca Receive/PreStart/PostStop çağrısı süresince geçerlidir;
// başka bir goroutine'e taşınıp saklanmamalıdır.
package actor
