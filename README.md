# CoreChat

Real-time Go terminal chat client and SDK for the CoreBlow Gateway.

## Overview

CoreChat is part of the CoreBlow public repository family. It provides both an interactive terminal REPL and a headless CLI for communicating with the CoreBlow Gateway over WebSocket and HTTP JSON-RPC protocols.

This repository follows the same ecosystem split that CoreBlow uses to keep release surfaces small, auditable, and independently governed.

## Repository Role

- Status: Active
- Phase: 4
- Priority: app
- Kind: chat
- Family: CoreBlow public repository family
- Branding: CoreBlow

## Features

- **Real-Time Streaming**: Connects via WebSocket or HTTP to stream CoreBlow agent responses token-by-token.
- **Interactive REPL**: Terminal chat interface with session management and live health checks.
- **Headless Mode**: Execute single prompt workflows via `--prompt` with standard input/output piping.
- **Gateway Health Client**: Validates CoreBlow Gateway connectivity, version, and uptime.
- **Zero External Dependencies**: Pure Go 1.22+ implementation with standard library RFC 6455 WebSocket framing.

## Usage

### Interactive Terminal REPL

```sh
go run ./cmd/corechat --gateway ws://127.0.0.1:18789 --session default
```

### Headless Single Prompt

```sh
go run ./cmd/corechat --gateway ws://127.0.0.1:18789 --prompt "Check disk space on /var"
```

### Health Check

```sh
go run ./cmd/corechat --gateway http://127.0.0.1:18789 --health
```

### Configuration Flags & Environment Variables

| Flag | Environment Variable | Default | Description |
| :--- | :--- | :--- | :--- |
| `--gateway` | `COREBLOW_GATEWAY` | `ws://127.0.0.1:18789` | Gateway WebSocket/HTTP endpoint |
| `--token` | `COREBLOW_TOKEN` | `""` | Optional Bearer authorization token |
| `--session` | `COREBLOW_SESSION` | `default` | Session key for conversation persistence |
| `--prompt` | — | `""` | Headless prompt to execute and stream to stdout |
| `--health` | — | `false` | Inspect gateway health snapshot and exit |

## Development & Testing

Run the full integration test suite:

```sh
go test -v ./...
```

Build the CLI binary:

```sh
go build ./cmd/corechat
```

## Release Policy

Do not publish packages, tags, installers, or release artifacts from this repository without explicit CoreBlow release approval.

Version changes must follow the coordinated CoreBlow release plan.

## Links

- [CoreBlow](https://github.com/coreblow/coreblow)
- [Documentation](https://docs.coreblow.com)
- [Website](https://coreblow.com)
- [Security Policy](SECURITY.md)
- [Contributing](CONTRIBUTING.md)
