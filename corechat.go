package corechat

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Legacy / compatibility message model
type Message struct {
	ID      string `json:"id"`
	Role    string `json:"role"`
	Content string `json:"content"`
}

type Thread struct {
	ID       string    `json:"id"`
	Messages []Message `json:"messages"`
}

func NormalizeMessage(message Message) Message {
	return Message{
		ID:      strings.TrimSpace(message.ID),
		Role:    strings.ToLower(strings.TrimSpace(message.Role)),
		Content: strings.TrimSpace(message.Content),
	}
}

func AppendMessage(thread Thread, message Message) Thread {
	next := Thread{ID: strings.TrimSpace(thread.ID)}
	next.Messages = append(next.Messages, thread.Messages...)
	next.Messages = append(next.Messages, NormalizeMessage(message))
	return next
}

// CoreBlow Gateway Protocol Frames
type RequestFrame struct {
	Type   string `json:"type"` // "req"
	ID     string `json:"id"`
	Method string `json:"method"`
	Params any    `json:"params,omitempty"`
}

type ErrorShape struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type ResponseFrame struct {
	Type    string          `json:"type"` // "res"
	ID      string          `json:"id"`
	OK      bool            `json:"ok"`
	Payload json.RawMessage `json:"payload,omitempty"`
	Error   *ErrorShape     `json:"error,omitempty"`
}

type EventFrame struct {
	Type    string          `json:"type"` // "event"
	Event   string          `json:"event"`
	Payload json.RawMessage `json:"payload,omitempty"`
	Seq     int64           `json:"seq,omitempty"`
}

// Client Options and Configuration
type ClientOptions struct {
	GatewayURL string
	Token      string
	SessionKey string
	Timeout    time.Duration
}

// HealthStatus represents CoreBlow Gateway health status
type HealthStatus struct {
	OK      bool   `json:"ok"`
	Status  string `json:"status"`
	Version string `json:"version,omitempty"`
	Uptime  int64  `json:"uptime,omitempty"`
}

// ChatResponse contains the completed chat assistant reply
type ChatResponse struct {
	SessionKey string `json:"sessionKey"`
	Message    string `json:"message"`
	Role       string `json:"role"`
}

// CoreChat Client
type Client struct {
	opts ClientOptions

	reqCounter uint64

	mu         sync.Mutex
	wsConn     net.Conn
	wsReader   *bufio.Reader
	pendingReq map[string]chan ResponseFrame
	eventCh    chan EventFrame
	closed     bool
}

func NewClient(opts ClientOptions) *Client {
	if opts.GatewayURL == "" {
		opts.GatewayURL = "http://127.0.0.1:18789"
	}
	if opts.SessionKey == "" {
		opts.SessionKey = "default"
	}
	if opts.Timeout == 0 {
		opts.Timeout = 15 * time.Second
	}
	return &Client{
		opts:       opts,
		pendingReq: make(map[string]chan ResponseFrame),
		eventCh:    make(chan EventFrame, 64),
	}
}

// CheckHealth queries the gateway /health endpoint or sends a health RPC
func (c *Client) CheckHealth(ctx context.Context) (*HealthStatus, error) {
	u, err := url.Parse(c.opts.GatewayURL)
	if err != nil {
		return nil, fmt.Errorf("invalid gateway url: %w", err)
	}

	httpScheme := "http"
	if u.Scheme == "https" || u.Scheme == "wss" {
		httpScheme = "https"
	}

	healthURL := fmt.Sprintf("%s://%s/health", httpScheme, u.Host)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
	if err != nil {
		return nil, err
	}
	if c.opts.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.opts.Token)
	}

	client := &http.Client{Timeout: c.opts.Timeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("health check request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("health check returned %d: %s", resp.StatusCode, string(body))
	}

	var status HealthStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return &HealthStatus{OK: true, Status: "ok"}, nil
	}
	return &status, nil
}

// ConnectWebSocket establishes a raw RFC 6455 WebSocket connection to the gateway
func (c *Client) ConnectWebSocket(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.wsConn != nil && !c.closed {
		return nil
	}

	u, err := url.Parse(c.opts.GatewayURL)
	if err != nil {
		return fmt.Errorf("invalid gateway url: %w", err)
	}

	host := u.Host
	if !strings.Contains(host, ":") {
		if u.Scheme == "https" || u.Scheme == "wss" {
			host += ":443"
		} else {
			host += ":80"
		}
	}

	path := u.Path
	if path == "" {
		path = "/"
	}

	dialer := net.Dialer{Timeout: c.opts.Timeout}
	conn, err := dialer.DialContext(ctx, "tcp", host)
	if err != nil {
		return fmt.Errorf("tcp dial failed: %w", err)
	}

	// Generate Sec-WebSocket-Key
	keyBytes := make([]byte, 16)
	_, _ = rand.Read(keyBytes)
	wsKey := base64.StdEncoding.EncodeToString(keyBytes)

	reqBuf := fmt.Sprintf(
		"GET %s HTTP/1.1\r\n"+
			"Host: %s\r\n"+
			"Upgrade: websocket\r\n"+
			"Connection: Upgrade\r\n"+
			"Sec-WebSocket-Key: %s\r\n"+
			"Sec-WebSocket-Version: 13\r\n",
		path, u.Host, wsKey,
	)
	if c.opts.Token != "" {
		reqBuf += fmt.Sprintf("Authorization: Bearer %s\r\n", c.opts.Token)
	}
	reqBuf += "\r\n"

	if _, err := conn.Write([]byte(reqBuf)); err != nil {
		conn.Close()
		return fmt.Errorf("ws handshake write failed: %w", err)
	}

	reader := bufio.NewReader(conn)
	statusLine, err := reader.ReadString('\n')
	if err != nil {
		conn.Close()
		return fmt.Errorf("failed reading handshake status: %w", err)
	}
	if !strings.Contains(statusLine, "101") {
		conn.Close()
		return fmt.Errorf("ws upgrade rejected: %s", strings.TrimSpace(statusLine))
	}

	// Read remaining headers
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			conn.Close()
			return fmt.Errorf("reading handshake headers: %w", err)
		}
		if line == "\r\n" || line == "\n" {
			break
		}
	}

	c.wsConn = conn
	c.wsReader = reader
	c.closed = false

	go c.readLoop()
	return nil
}

// readLoop reads frames from the WebSocket
func (c *Client) readLoop() {
	for {
		msgBytes, err := readWSFrame(c.wsReader)
		if err != nil {
			c.mu.Lock()
			c.closed = true
			if c.wsConn != nil {
				_ = c.wsConn.Close()
				c.wsConn = nil
			}
			for _, ch := range c.pendingReq {
				close(ch)
			}
			c.pendingReq = make(map[string]chan ResponseFrame)
			c.mu.Unlock()
			return
		}

		var generic map[string]any
		if err := json.Unmarshal(msgBytes, &generic); err != nil {
			continue
		}

		frameType, _ := generic["type"].(string)
		switch frameType {
		case "res":
			var res ResponseFrame
			if err := json.Unmarshal(msgBytes, &res); err == nil {
				c.mu.Lock()
				ch, ok := c.pendingReq[res.ID]
				if ok {
					delete(c.pendingReq, res.ID)
					ch <- res
					close(ch)
				}
				c.mu.Unlock()
			}
		case "event":
			var ev EventFrame
			if err := json.Unmarshal(msgBytes, &ev); err == nil {
				select {
				case c.eventCh <- ev:
				default:
				}
			}
		}
	}
}

// SendMessage sends a chat message either over WebSocket or HTTP JSON-RPC fallback
func (c *Client) SendMessage(ctx context.Context, text string) (*ChatResponse, error) {
	return c.StreamMessage(ctx, text, nil)
}

// StreamMessage sends a chat message and invokes onChunk for streaming delta tokens
func (c *Client) StreamMessage(ctx context.Context, text string, onChunk func(string)) (*ChatResponse, error) {
	// Try WebSocket first if available or configured
	if strings.HasPrefix(c.opts.GatewayURL, "ws://") || strings.HasPrefix(c.opts.GatewayURL, "wss://") {
		if err := c.ConnectWebSocket(ctx); err == nil {
			return c.streamMessageWS(ctx, text, onChunk)
		}
	}

	// Fallback to HTTP JSON-RPC
	return c.sendMessageHTTP(ctx, text, onChunk)
}

func (c *Client) streamMessageWS(ctx context.Context, text string, onChunk func(string)) (*ChatResponse, error) {
	reqID := fmt.Sprintf("req-%d", atomic.AddUint64(&c.reqCounter, 1))
	reqFrame := RequestFrame{
		Type:   "req",
		ID:     reqID,
		Method: "chat.send",
		Params: map[string]any{
			"sessionKey": c.opts.SessionKey,
			"message":    text,
		},
	}

	payload, err := json.Marshal(reqFrame)
	if err != nil {
		return nil, err
	}

	respCh := make(chan ResponseFrame, 1)
	c.mu.Lock()
	c.pendingReq[reqID] = respCh
	conn := c.wsConn
	c.mu.Unlock()

	if conn == nil {
		return nil, errors.New("websocket not connected")
	}

	if err := writeWSFrame(conn, payload); err != nil {
		c.mu.Lock()
		delete(c.pendingReq, reqID)
		c.mu.Unlock()
		return nil, fmt.Errorf("websocket write failed: %w", err)
	}

	var accumulated strings.Builder
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case ev := <-c.eventCh:
			if ev.Event == "chat.stream" || ev.Event == "chat" {
				var p struct {
					Delta string `json:"delta"`
					Text  string `json:"text"`
				}
				if err := json.Unmarshal(ev.Payload, &p); err == nil {
					chunk := p.Delta
					if chunk == "" {
						chunk = p.Text
					}
					if chunk != "" {
						accumulated.WriteString(chunk)
						if onChunk != nil {
							onChunk(chunk)
						}
					}
				}
			}
		case res, ok := <-respCh:
			if !ok {
				return nil, errors.New("connection closed while waiting for response")
			}
			if !res.OK {
				errMsg := "request failed"
				if res.Error != nil {
					errMsg = res.Error.Message
				}
				return nil, errors.New(errMsg)
			}
			var result struct {
				Message    string `json:"message"`
				Role       string `json:"role"`
				SessionKey string `json:"sessionKey"`
			}
			if len(res.Payload) > 0 {
				_ = json.Unmarshal(res.Payload, &result)
			}
			finalMsg := result.Message
			if finalMsg == "" {
				finalMsg = accumulated.String()
			}
			return &ChatResponse{
				SessionKey: c.opts.SessionKey,
				Message:    finalMsg,
				Role:       result.Role,
			}, nil
		}
	}
}

func (c *Client) sendMessageHTTP(ctx context.Context, text string, onChunk func(string)) (*ChatResponse, error) {
	u, err := url.Parse(c.opts.GatewayURL)
	if err != nil {
		return nil, fmt.Errorf("invalid gateway url: %w", err)
	}

	scheme := "http"
	if u.Scheme == "https" || u.Scheme == "wss" {
		scheme = "https"
	}
	apiURL := fmt.Sprintf("%s://%s/api/chat", scheme, u.Host)

	reqID := fmt.Sprintf("req-%d", atomic.AddUint64(&c.reqCounter, 1))
	reqBody := RequestFrame{
		Type:   "req",
		ID:     reqID,
		Method: "chat.send",
		Params: map[string]any{
			"sessionKey": c.opts.SessionKey,
			"message":    text,
		},
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if c.opts.Token != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.opts.Token)
	}

	client := &http.Client{Timeout: c.opts.Timeout}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("gateway returned %d: %s", resp.StatusCode, string(respBody))
	}

	var res ResponseFrame
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, fmt.Errorf("decoding gateway response: %w", err)
	}

	if !res.OK {
		errMsg := "gateway returned error"
		if res.Error != nil {
			errMsg = res.Error.Message
		}
		return nil, errors.New(errMsg)
	}

	var chatRes ChatResponse
	if len(res.Payload) > 0 {
		_ = json.Unmarshal(res.Payload, &chatRes)
	}
	if chatRes.SessionKey == "" {
		chatRes.SessionKey = c.opts.SessionKey
	}
	if onChunk != nil && chatRes.Message != "" {
		onChunk(chatRes.Message)
	}
	return &chatRes, nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	if c.wsConn != nil {
		err := c.wsConn.Close()
		c.wsConn = nil
		return err
	}
	return nil
}

// RFC 6455 Client WebSocket frame helpers
func writeWSFrame(w io.Writer, payload []byte) error {
	var header bytes.Buffer
	// FIN = 1, Opcode = 1 (Text frame) -> 0x81
	header.WriteByte(0x81)

	length := len(payload)
	// Client-to-server frames MUST be masked (bit 7 = 1)
	if length <= 125 {
		header.WriteByte(byte(length) | 0x80)
	} else if length <= 65535 {
		header.WriteByte(126 | 0x80)
		_ = binary.Write(&header, binary.BigEndian, uint16(length))
	} else {
		header.WriteByte(127 | 0x80)
		_ = binary.Write(&header, binary.BigEndian, uint64(length))
	}

	// 4-byte random masking key
	mask := make([]byte, 4)
	_, _ = rand.Read(mask)
	header.Write(mask)

	// Mask the payload
	masked := make([]byte, length)
	for i := 0; i < length; i++ {
		masked[i] = payload[i] ^ mask[i%4]
	}

	if _, err := w.Write(header.Bytes()); err != nil {
		return err
	}
	_, err := w.Write(masked)
	return err
}

func readWSFrame(r *bufio.Reader) ([]byte, error) {
	b0, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	b1, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	masked := (b1 & 0x80) != 0
	length := int(b1 & 0x7F)

	if length == 126 {
		var l uint16
		if err := binary.Read(r, binary.BigEndian, &l); err != nil {
			return nil, err
		}
		length = int(l)
	} else if length == 127 {
		var l uint64
		if err := binary.Read(r, binary.BigEndian, &l); err != nil {
			return nil, err
		}
		length = int(l)
	}

	var mask [4]byte
	if masked {
		if _, err := io.ReadFull(r, mask[:]); err != nil {
			return nil, err
		}
	}

	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}

	if masked {
		for i := 0; i < length; i++ {
			payload[i] ^= mask[i%4]
		}
	}

	opcode := b0 & 0x0F
	// Opcode 8 is Close, 9 Ping, 10 Pong
	if opcode == 8 {
		return nil, io.EOF
	}

	return payload, nil
}
