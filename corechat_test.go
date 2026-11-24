package corechat

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestAppendMessageNormalizesMessage(t *testing.T) {
	thread := AppendMessage(Thread{ID: " t1 "}, Message{ID: " m1 ", Role: " User ", Content: " hello "})
	if thread.ID != "t1" || len(thread.Messages) != 1 {
		t.Fatalf("unexpected thread: %#v", thread)
	}
	if thread.Messages[0].Role != "user" || thread.Messages[0].Content != "hello" {
		t.Fatalf("unexpected message: %#v", thread.Messages[0])
	}
}

func TestHTTPHealthCheck(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("Authorization") != "Bearer test-secret-token" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(HealthStatus{
			OK:      true,
			Status:  "ok",
			Version: "2.1.2",
			Uptime:  12345,
		})
	}))
	defer server.Close()

	client := NewClient(ClientOptions{
		GatewayURL: server.URL,
		Token:      "test-secret-token",
		Timeout:    2 * time.Second,
	})

	ctx := context.Background()
	st, err := client.CheckHealth(ctx)
	if err != nil {
		t.Fatalf("health check failed: %v", err)
	}
	if !st.OK || st.Status != "ok" || st.Version != "2.1.2" {
		t.Fatalf("unexpected health status: %#v", st)
	}

	// Unauthorized test
	badClient := NewClient(ClientOptions{
		GatewayURL: server.URL,
		Token:      "wrong-token",
		Timeout:    2 * time.Second,
	})
	if _, err := badClient.CheckHealth(ctx); err == nil {
		t.Fatal("expected error with invalid token, got nil")
	}
}

func TestHTTPSendMessage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/chat" {
			http.NotFound(w, r)
			return
		}
		var req RequestFrame
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}

		params, _ := req.Params.(map[string]any)
		msg := params["message"].(string)

		res := ResponseFrame{
			Type: "res",
			ID:   req.ID,
			OK:   true,
			Payload: json.RawMessage(fmt.Sprintf(
				`{"sessionKey":"%s","message":"Echo: %s","role":"assistant"}`,
				params["sessionKey"], msg,
			)),
		}
		_ = json.NewEncoder(w).Encode(res)
	}))
	defer server.Close()

	client := NewClient(ClientOptions{
		GatewayURL: server.URL,
		SessionKey: "sess-1",
		Timeout:    2 * time.Second,
	})

	var streamed string
	resp, err := client.StreamMessage(context.Background(), "hello world", func(chunk string) {
		streamed += chunk
	})
	if err != nil {
		t.Fatalf("StreamMessage failed: %v", err)
	}
	if resp.Message != "Echo: hello world" {
		t.Fatalf("unexpected response message: %s", resp.Message)
	}
	if streamed != "Echo: hello world" {
		t.Fatalf("unexpected streamed chunk: %s", streamed)
	}
}

func TestMockWebSocketChat(t *testing.T) {
	// Simple RFC 6455 Mock WebSocket Server
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer ln.Close()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		reader := bufio.NewReader(conn)
		// Read HTTP GET request
		var wsKey string
		for {
			line, err := reader.ReadString('\n')
			if err != nil || line == "\r\n" {
				break
			}
			if strings.HasPrefix(line, "Sec-WebSocket-Key:") {
				wsKey = strings.TrimSpace(strings.TrimPrefix(line, "Sec-WebSocket-Key:"))
			}
		}

		// Compute accept key
		h := sha1.New()
		h.Write([]byte(wsKey + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
		acceptKey := base64.StdEncoding.EncodeToString(h.Sum(nil))

		// Send 101 Switching Protocols
		resp := "HTTP/1.1 101 Switching Protocols\r\n" +
			"Upgrade: websocket\r\n" +
			"Connection: Upgrade\r\n" +
			"Sec-WebSocket-Accept: " + acceptKey + "\r\n\r\n"
		_, _ = conn.Write([]byte(resp))

		// Read request frame
		frame, err := readWSFrame(reader)
		if err != nil {
			return
		}

		var req RequestFrame
		_ = json.Unmarshal(frame, &req)

		// Send streaming event frame
		evPayload := []byte(`{"type":"event","event":"chat.stream","payload":{"delta":"CoreBlow: live streaming chunk"}}`)
		_ = writeServerWSFrame(conn, evPayload)

		// Send final response frame
		resPayload := []byte(fmt.Sprintf(`{"type":"res","id":"%s","ok":true,"payload":{"sessionKey":"test-ws","message":"CoreBlow: live streaming chunk","role":"assistant"}}`, req.ID))
		_ = writeServerWSFrame(conn, resPayload)
	}()

	client := NewClient(ClientOptions{
		GatewayURL: fmt.Sprintf("ws://%s", ln.Addr().String()),
		SessionKey: "test-ws",
		Timeout:    2 * time.Second,
	})
	defer client.Close()

	var streamOutput string
	res, err := client.StreamMessage(context.Background(), "hello via websocket", func(c string) {
		streamOutput += c
	})
	if err != nil {
		t.Fatalf("StreamMessage over ws failed: %v", err)
	}

	if streamOutput != "CoreBlow: live streaming chunk" {
		t.Fatalf("unexpected stream output: %q", streamOutput)
	}
	if res.Message != "CoreBlow: live streaming chunk" {
		t.Fatalf("unexpected result message: %q", res.Message)
	}

	wg.Wait()
}

func writeServerWSFrame(conn net.Conn, payload []byte) error {
	// Server to client frames are NOT masked
	var header bytes.Buffer
	header.WriteByte(0x81) // FIN + text opcode
	length := len(payload)
	if length <= 125 {
		header.WriteByte(byte(length))
	} else if length <= 65535 {
		header.WriteByte(126)
		_ = binary.Write(&header, binary.BigEndian, uint16(length))
	} else {
		header.WriteByte(127)
		_ = binary.Write(&header, binary.BigEndian, uint64(length))
	}
	if _, err := conn.Write(header.Bytes()); err != nil {
		return err
	}
	_, err := conn.Write(payload)
	return err
}
