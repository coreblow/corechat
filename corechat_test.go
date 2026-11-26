package corechat

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
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

		// Read request frames (connect handshake first, then chat.send)
		for {
			frame, err := readWSFrame(reader)
			if err != nil {
				return
			}

			var req RequestFrame
			_ = json.Unmarshal(frame, &req)
			if req.Method == "connect" {
				ack := []byte(fmt.Sprintf(`{"type":"res","id":"%s","ok":true,"payload":{}}`, req.ID))
				_ = writeServerWSFrame(conn, ack)
				continue
			}

			// Send streaming event frame
			evPayload := []byte(`{"type":"event","event":"chat.stream","payload":{"delta":"CoreBlow: live streaming chunk"}}`)
			_ = writeServerWSFrame(conn, evPayload)

			// Send final response frame
			resPayload := []byte(fmt.Sprintf(`{"type":"res","id":"%s","ok":true,"payload":{"sessionKey":"test-ws","message":"CoreBlow: live streaming chunk","role":"assistant"}}`, req.ID))
			_ = writeServerWSFrame(conn, resPayload)
			return
		}
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

func TestGatewayV3Ed25519StreamingAndErrors(t *testing.T) {
	t.Run("Ed25519ChallengeStreaming", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen failed: %v", err)
		}
		defer ln.Close()

		const challengeNonce = "nonce-v3-12345"
		const authToken = "gw-v3-token"

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
			var wsKey, authHeader string
			for {
				line, err := reader.ReadString('\n')
				if err != nil || line == "\r\n" {
					break
				}
				if strings.HasPrefix(line, "Sec-WebSocket-Key:") {
					wsKey = strings.TrimSpace(strings.TrimPrefix(line, "Sec-WebSocket-Key:"))
				}
				if strings.HasPrefix(line, "Authorization:") {
					authHeader = strings.TrimSpace(strings.TrimPrefix(line, "Authorization:"))
				}
			}
			if authHeader != "Bearer "+authToken {
				t.Errorf("unexpected Authorization header: %q", authHeader)
				return
			}

			h := sha1.New()
			h.Write([]byte(wsKey + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
			acceptKey := base64.StdEncoding.EncodeToString(h.Sum(nil))
			_, _ = conn.Write([]byte("HTTP/1.1 101 Switching Protocols\r\n" +
				"Upgrade: websocket\r\n" +
				"Connection: Upgrade\r\n" +
				"Sec-WebSocket-Accept: " + acceptKey + "\r\n\r\n"))

			// Emit Gateway v3 connect.challenge
			challenge := fmt.Sprintf(`{"type":"event","event":"connect.challenge","payload":{"nonce":%q}}`, challengeNonce)
			if err := writeServerWSFrame(conn, []byte(challenge)); err != nil {
				t.Errorf("write challenge failed: %v", err)
				return
			}

			for {
				frame, err := readWSFrame(reader)
				if err != nil {
					return
				}
				var req RequestFrame
				if err := json.Unmarshal(frame, &req); err != nil {
					t.Errorf("unmarshal request failed: %v", err)
					return
				}

				if req.Method == "connect" {
					rawParams, _ := json.Marshal(req.Params)
					var params struct {
						MinProtocol int `json:"minProtocol"`
						MaxProtocol int `json:"maxProtocol"`
						Auth        struct {
							Token string `json:"token"`
						} `json:"auth"`
						Device struct {
							ID        string `json:"id"`
							PublicKey string `json:"publicKey"`
							Signature string `json:"signature"`
							SignedAt  int64  `json:"signedAt"`
							Nonce     string `json:"nonce"`
						} `json:"device"`
					}
					if err := json.Unmarshal(rawParams, &params); err != nil {
						t.Errorf("unmarshal connect params failed: %v", err)
						return
					}
					if params.MinProtocol != 3 || params.MaxProtocol != 3 {
						t.Errorf("unexpected protocol range: %d..%d", params.MinProtocol, params.MaxProtocol)
						return
					}
					if params.Auth.Token != authToken {
						t.Errorf("unexpected connect auth token: %q", params.Auth.Token)
						return
					}
					if params.Device.Nonce != challengeNonce {
						t.Errorf("unexpected device nonce: %q", params.Device.Nonce)
						return
					}
					pubBytes, err := base64.RawURLEncoding.DecodeString(params.Device.PublicKey)
					if err != nil || len(pubBytes) != ed25519.PublicKeySize {
						t.Errorf("invalid ed25519 public key: %v", err)
						return
					}
					sum := sha256.Sum256(pubBytes)
					if hex.EncodeToString(sum[:]) != params.Device.ID {
						t.Errorf("device id mismatch: got %q", params.Device.ID)
						return
					}
					sigBytes, err := base64.RawURLEncoding.DecodeString(params.Device.Signature)
					if err != nil {
						t.Errorf("invalid signature base64: %v", err)
						return
					}
					expectedSignPayload := strings.Join([]string{
						"v3",
						params.Device.ID,
						"cli",
						"cli",
						"operator",
						"operator.admin",
						fmt.Sprintf("%d", params.Device.SignedAt),
						authToken,
						challengeNonce,
						runtime.GOOS,
						"",
					}, "|")
					if !ed25519.Verify(ed25519.PublicKey(pubBytes), []byte(expectedSignPayload), sigBytes) {
						t.Errorf("ed25519 signature verification failed")
						return
					}
					ack := fmt.Sprintf(`{"type":"res","id":%q,"ok":true,"payload":{}}`, req.ID)
					_ = writeServerWSFrame(conn, []byte(ack))
					continue
				}

				if req.Method == "chat.send" {
					_ = writeServerWSFrame(conn, []byte(`{"type":"event","event":"chat.stream","payload":{"delta":"Hello "}}`))
					time.Sleep(10 * time.Millisecond)
					_ = writeServerWSFrame(conn, []byte(`{"type":"event","event":"chat.stream","payload":{"delta":"CoreBlow!"}}`))
					time.Sleep(10 * time.Millisecond)
					res := fmt.Sprintf(`{"type":"res","id":%q,"ok":true,"payload":{"sessionKey":"gw-v3","message":"Hello CoreBlow!","role":"assistant"}}`, req.ID)
					_ = writeServerWSFrame(conn, []byte(res))
					return
				}
			}
		}()

		client := NewClient(ClientOptions{
			GatewayURL: fmt.Sprintf("ws://%s", ln.Addr().String()),
			Token:      authToken,
			SessionKey: "gw-v3",
			Timeout:    2 * time.Second,
		})
		defer client.Close()

		var chunks []string
		res, err := client.StreamMessage(context.Background(), "hello v3", func(chunk string) {
			chunks = append(chunks, chunk)
		})
		if err != nil {
			t.Fatalf("StreamMessage failed: %v", err)
		}
		if res.Message != "Hello CoreBlow!" {
			t.Fatalf("unexpected message: %q", res.Message)
		}
		if strings.Join(chunks, "") != "Hello CoreBlow!" {
			t.Fatalf("unexpected streamed chunks: %#v", chunks)
		}
		wg.Wait()
	})

	t.Run("RejectedHandshakeError", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen failed: %v", err)
		}
		defer ln.Close()

		go func() {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			defer conn.Close()

			reader := bufio.NewReader(conn)
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
			h := sha1.New()
			h.Write([]byte(wsKey + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
			acceptKey := base64.StdEncoding.EncodeToString(h.Sum(nil))
			_, _ = conn.Write([]byte("HTTP/1.1 101 Switching Protocols\r\n" +
				"Upgrade: websocket\r\n" +
				"Connection: Upgrade\r\n" +
				"Sec-WebSocket-Accept: " + acceptKey + "\r\n\r\n"))

			_ = writeServerWSFrame(conn, []byte(`{"type":"event","event":"connect.challenge","payload":{"nonce":"bad-nonce"}}`))
			frame, err := readWSFrame(reader)
			if err != nil {
				return
			}
			var req RequestFrame
			_ = json.Unmarshal(frame, &req)
			rej := fmt.Sprintf(`{"type":"res","id":%q,"ok":false,"error":{"code":"unauthorized","message":"invalid device signature"}}`, req.ID)
			_ = writeServerWSFrame(conn, []byte(rej))
		}()

		client := NewClient(ClientOptions{
			GatewayURL: fmt.Sprintf("ws://%s", ln.Addr().String()),
			Timeout:    2 * time.Second,
		})
		defer client.Close()

		err = client.ConnectWebSocket(context.Background())
		if err == nil || !strings.Contains(err.Error(), "invalid device signature") {
			t.Fatalf("expected invalid device signature error, got: %v", err)
		}
	})

	t.Run("DisconnectDuringStreamError", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen failed: %v", err)
		}
		defer ln.Close()

		go func() {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			defer conn.Close()

			reader := bufio.NewReader(conn)
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
			h := sha1.New()
			h.Write([]byte(wsKey + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
			acceptKey := base64.StdEncoding.EncodeToString(h.Sum(nil))
			_, _ = conn.Write([]byte("HTTP/1.1 101 Switching Protocols\r\n" +
				"Upgrade: websocket\r\n" +
				"Connection: Upgrade\r\n" +
				"Sec-WebSocket-Accept: " + acceptKey + "\r\n\r\n"))

			for {
				frame, err := readWSFrame(reader)
				if err != nil {
					return
				}
				var req RequestFrame
				_ = json.Unmarshal(frame, &req)
				if req.Method == "connect" {
					ack := fmt.Sprintf(`{"type":"res","id":%q,"ok":true,"payload":{}}`, req.ID)
					_ = writeServerWSFrame(conn, []byte(ack))
					continue
				}
				// Close abruptly on chat.send
				return
			}
		}()

		client := NewClient(ClientOptions{
			GatewayURL: fmt.Sprintf("ws://%s", ln.Addr().String()),
			Timeout:    2 * time.Second,
		})
		defer client.Close()

		if err := client.ConnectWebSocket(context.Background()); err != nil {
			t.Fatalf("ConnectWebSocket failed: %v", err)
		}
		_, err = client.StreamMessage(context.Background(), "ping", nil)
		if err == nil {
			t.Fatal("expected error on disconnect during stream, got nil")
		}
	})
}
