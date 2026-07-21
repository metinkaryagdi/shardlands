package actor

// Producer her başlatmada (ve her restart'ta) TAZE bir Actor instance'ı
// üretir. Restart'ın state'i sıfırlaması bu sayede olur: eski instance
// atılır, Producer yenisini yaratır. Producer'ın kapattığı (closure)
// değişkenler restart'lar arasında yaşamaya devam eder — bunu bilinçli
// kullan (örn. metrik sayaçları), aktör state'ini oraya sızdırma.
type Producer func() Actor

// Props, bir aktörün nasıl yaratılacağını ve yönetileceğini tanımlar.
type Props struct {
	// Producer zorunludur.
	Producer Producer
	// Name boşsa otomatik üretilir ("actor-N"). Aynı ebeveyn altında
	// benzersiz olmalıdır; '/' içeremez.
	Name string
	// MailboxSize <= 0 ise 64 kullanılır.
	MailboxSize int
	// Overflow, mailbox dolduğundaki davranış (varsayılan: Block).
	Overflow OverflowPolicy
	// Supervision, bu aktörün kendi hatalarına uygulanacak strateji.
	Supervision Strategy
}
