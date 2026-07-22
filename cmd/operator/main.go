// Shardlands Arena Operator.
//
//	go run ./cmd/operator                 # kubeconfig'deki kümeye bağlanır
//	kubectl apply -f operator/config/crd/ # önce CRD kurulmalı
package main

import (
	"flag"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	arenav1 "shardlands/operator/api/v1alpha1"
	"shardlands/operator/controller"
)

func main() {
	var metricsAddr, probeAddr, namespace string
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8081", "metrics adresi")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8082", "sağlık probu adresi")
	flag.StringVar(&namespace, "namespace", "", "izlenecek namespace (boş = tümü)")
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	log := ctrl.Log.WithName("setup")

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		log.Error(err, "core scheme")
		os.Exit(1)
	}
	if err := arenav1.AddToScheme(scheme); err != nil {
		log.Error(err, "arena scheme")
		os.Exit(1)
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
	})
	if err != nil {
		log.Error(err, "manager")
		os.Exit(1)
	}

	if err := (&controller.ArenaReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		log.Error(err, "arena controller")
		os.Exit(1)
	}

	log.Info("arena operator başlıyor")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Error(err, "manager çalışırken hata")
		os.Exit(1)
	}
}
