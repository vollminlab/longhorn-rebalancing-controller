package main

import (
	"flag"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/vollminlab/longhorn-rebalancing-controller/internal/controller"
	longhornv1beta2 "github.com/vollminlab/longhorn-rebalancing-controller/internal/longhorn"
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(longhornv1beta2.AddToScheme(scheme))
}

func main() {
	var metricsAddr string
	var configMapName string
	var configMapNamespace string
	var syncInterval time.Duration

	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "Address for the metrics endpoint")
	flag.StringVar(&configMapName, "config-map-name", "longhorn-rebalancing-controller", "ConfigMap name containing controller config")
	flag.StringVar(&configMapNamespace, "config-map-namespace", "longhorn-system", "Namespace of the controller ConfigMap")
	flag.DurationVar(&syncInterval, "sync-interval", 5*time.Minute, "Interval between periodic cluster checks")

	opts := zap.Options{Development: false}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	setupLog := ctrl.Log.WithName("setup")

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: metricsAddr,
		},
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	r := &controller.RebalancingReconciler{
		Client:             mgr.GetClient(),
		Scheme:             mgr.GetScheme(),
		ConfigMapName:      configMapName,
		ConfigMapNamespace: configMapNamespace,
		SyncInterval:       syncInterval,
	}

	if err = r.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
