package main

import (
	"fmt"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	arenav1 "shardlands/operator/api/v1alpha1"
	"shardlands/services/matchmaking"
)

// k8sProvisioner, ARENA_NAMESPACE tanımlıysa Kubernetes sağlayıcısını
// kurar; tanımlı değilse nil döner ve sunucu yerel (aynı süreç) arena
// sağlayıcısına düşer.
//
// Tek anahtar (namespace) ile mod seçmenin nedeni: sunucu binary'si
// "kümede miyim" sorusunu kendi başına doğru cevaplayamaz — testler de
// bir kubeconfig'in yanında koşabilir. Açık bir ortam değişkeni,
// yanlışlıkla küme moduna düşmeyi imkânsız kılar.
func k8sProvisioner() (matchmaking.Provisioner, error) {
	ns := os.Getenv("ARENA_NAMESPACE")
	if ns == "" {
		return nil, nil
	}

	scheme := runtime.NewScheme()
	if err := arenav1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("arena scheme: %w", err)
	}
	cfg, err := ctrl.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("kubeconfig: %w", err)
	}
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return nil, fmt.Errorf("k8s client: %w", err)
	}

	image := os.Getenv("ARENA_IMAGE")
	if image == "" {
		image = "shardlands/arena:dev"
	}
	return &matchmaking.K8sProvisioner{
		Client:    c,
		Namespace: ns,
		Image:     image,
	}, nil
}
