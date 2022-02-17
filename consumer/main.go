package main

import (
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/boxidau/sampled/consumer/config"
	"github.com/boxidau/sampled/consumer/consumer"
	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	FLAG_sampledConfig    = flag.String("sampled_config", "config.yaml", "Sampled config file")
	FLAG_prometheusListen = flag.String("prometheus_listen", ":9100", "Listen address for prometheus metrics exposition")
)

func init() {
	flag.Parse()
}

func main() {
	go func() {
		glog.Infof("Prometheus HTTP exposition starting on %s", *FLAG_prometheusListen)
		http.Handle("/metrics", promhttp.Handler())
		err := http.ListenAndServe(*FLAG_prometheusListen, nil)
		if err != nil {
			glog.Errorf("Unable to start prometheus metrics HTTP server")
		}
	}()

	cfg, err := config.LoadConfig(*FLAG_sampledConfig)
	if err != nil {
		glog.Fatal(err)
	}

	consumer, err := consumer.NewConsumer(cfg)
	if err != nil {
		glog.Fatal(err)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		glog.Info("Shutting down")
		consumer.Shutdown()
	}()

	glog.Info("Starting sampled consumer")
	err = consumer.Run()
	if err != nil {
		glog.Fatal("Unable to run sampled consumer %v", err)
	}
}
