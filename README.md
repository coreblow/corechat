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
| `--version` | — | `false` | Print CoreChat version and exit |

## Release Binaries

Tagged releases (`v*`) publish 6 static binaries (`CGO_ENABLED=0`) with `SHA256SUMS.txt`, a CycloneDX SBOM (`sbom.cdx.json`), and GitHub Actions build provenance attestations:

- `corechat-darwin-arm64` (macOS Apple Silicon)
- `corechat-darwin-amd64` (macOS Intel)
- `corechat-linux-amd64` (Linux x86_64)
- `corechat-linux-arm64` (Linux ARM64)
- `corechat-windows-amd64.exe` (Windows x86_64)
- `corechat-windows-arm64.exe` (Windows ARM64)

Verify downloaded binaries with:

```sh
sha256sum -c SHA256SUMS.txt --ignore-missing
```

## Subpackages

- `pkg/corekit`: Endpoint normalization (`NormalizeEndpoint`) and local-host detection (`IsLocal`) helpers.
- `pkg/coredex`: Contact normalization (`NormalizeContact`) and searchable display/address lookup helpers (`SearchableName`).

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
