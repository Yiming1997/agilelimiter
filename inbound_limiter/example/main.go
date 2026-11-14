package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/Yiming1997/agilelimiter"
	"github.com/Yiming1997/agilelimiter/inbound_limiter/inbound"
)

func main() {
	adaptive := agilelimiter.NewConcurrency(agilelimiter.ConcurrencyConfig{
		InitialLimit: 10,
		MaxLimit:     100,
	})
	work := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	handler, err := inbound.NewHandler(work, adaptive, inbound.Config{
		Enabled: true,
		// Each IP address can make a maximum of 3 requests per window.
		Tokens: 3,
		// The window duration is 10 seconds.
		Interval: 10 * time.Second,
	})
	if err != nil {
		log.Print(err)
		return
	}
	defer handler.Close(context.Background())
	adaptive.Start()
	defer adaptive.Stop()

	server := &http.Server{
		Addr:              "127.0.0.1:8080",
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Println("Test address: http://127.0.0.1:8080, each IP is allowed 3 requests every 10 seconds.")
	if err := server.ListenAndServe(); err != nil {
		log.Print(err)
	}
}
