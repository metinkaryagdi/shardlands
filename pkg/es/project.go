package es

import "log"

// Project, standart projection döngüsüdür: checkpoint'ten oku → uygula
// → sinyal bekle → tekrar. stop kapanana dek bloklar (kendi
// goroutine'inde çağır). apply, sırayla ve TEK goroutine'den çağrılır;
// read model içinde ek senkronizasyon yalnızca sorgu tarafı için
// gerekir. Her read model kendi süzgecini apply içinde uygular — log
// paylaşımlıdır, ilgisiz event görmek normaldir.
func Project(s *Store, stop <-chan struct{}, apply func([]Event)) {
	notify, cancel := s.Subscribe()
	defer cancel()

	var checkpoint uint64
	for {
		evs, err := s.ReadAll(checkpoint+1, 256)
		if err != nil {
			log.Printf("es: projection read: %v", err)
		}
		if len(evs) > 0 {
			apply(evs)
			checkpoint = evs[len(evs)-1].Global
			continue // aynı turda devamı olabilir
		}
		select {
		case <-notify:
		case <-stop:
			return
		}
	}
}
