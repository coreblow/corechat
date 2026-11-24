package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/coreblow/corechat"
)

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func main() {
	gatewayFlag := flag.String("gateway", getEnv("COREBLOW_GATEWAY", "ws://127.0.0.1:18789"), "CoreBlow Gateway URL (ws:// or http://)")
	tokenFlag := flag.String("token", getEnv("COREBLOW_TOKEN", ""), "CoreBlow Gateway Auth Token")
	sessionFlag := flag.String("session", getEnv("COREBLOW_SESSION", "default"), "Session Key")
	promptFlag := flag.String("prompt", "", "Headless prompt to send immediately")
	healthFlag := flag.Bool("health", false, "Check Gateway health and exit")
	flag.Parse()

	client := corechat.NewClient(corechat.ClientOptions{
		GatewayURL: *gatewayFlag,
		Token:      *tokenFlag,
		SessionKey: *sessionFlag,
		Timeout:    30 * time.Second,
	})
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle OS interrupt signals for graceful exit
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
		fmt.Println("\nReceived interrupt, exiting CoreChat...")
		os.Exit(0)
	}()

	if *healthFlag {
		status, err := client.CheckHealth(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: gateway health check failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("CoreBlow Gateway Health: OK=%v Status=%s Version=%s\n", status.OK, status.Status, status.Version)
		return
	}

	// Headless prompt mode
	if *promptFlag != "" {
		res, err := client.StreamMessage(ctx, *promptFlag, func(chunk string) {
			fmt.Print(chunk)
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "\nError: %v\n", err)
			os.Exit(1)
		}
		if res.Message == "" {
			fmt.Println()
		} else {
			fmt.Println()
		}
		return
	}

	// Interactive REPL mode
	fmt.Println("CoreBlow Chat Terminal (v2.1.2)")
	fmt.Printf("Connected to Gateway: %s | Session: %s\n", *gatewayFlag, *sessionFlag)
	fmt.Println("Type your message and press Enter. Press Ctrl+C or type 'exit' to quit.")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("corechat> ")
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if line == "exit" || line == "quit" {
			break
		}
		if line == "/health" {
			st, err := client.CheckHealth(ctx)
			if err != nil {
				fmt.Printf("[health check error: %v]\n", err)
			} else {
				fmt.Printf("[gateway status: %s, ok: %v]\n", st.Status, st.OK)
			}
			continue
		}

		fmt.Print("CoreBlow: ")
		_, err := client.StreamMessage(ctx, line, func(chunk string) {
			fmt.Print(chunk)
		})
		if err != nil {
			fmt.Printf("\n[Error: %v]\n", err)
		} else {
			fmt.Println()
		}
	}
}
