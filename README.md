
# SheshAOS

**A governance-first, event-sourced AI operating system in Rust** — models propose
actions, the kernel validates and records every state change in an append-only
audit trail, and the whole thing runs local-first with replaceable AI providers.

![Rust](https://img.shields.io/badge/Rust-2024-important?logo=rust)
![License](https://img.shields.io/badge/License-GPL--3.0--or--later-blue)
![CI](https://github.com/gaganjainse/shesh-aos/actions/workflows/ci.yml/badge.svg)

- **License:** GPL-3.0-or-later
- **Owner:** Gagan Jain ([@gaganjainse](https://github.com/gaganjainse))
- **Target:** CachyOS/Arch · Linux-native · Rust

[ Docs](https://github.com/gaganjainse/shesh-docs) · [ Architecture](docs/architecture.md) · [ Contributing](CONTRIBUTING.md) · [ Security](SECURITY.md) · [ Changelog](CHANGELOG.md)

---

## Quick start

```bash
git clone https://github.com/gaganjainse/shesh-aos.git
cd SheshAOS
cargo build --release
./target/release/shesh init
./target/release/shesh run "describe the project structure"
```
Runs on Linux (primary target: **CachyOS/Arch + Hyprland**), offline-capable.

## What it is

SheshAOS is a **microkernel-style AI operating environment**: tasks route to
specialist local models (planner, coder, vision), a policy engine validates every
action, and an event-sourced store keeps an append-only, hash-verifiable record
of every state change. It ships with a stock **Wave Terminal** frontend
(ADR-0016), a full CLI, and a JSON-RPC control socket.

| Attribute | Value |
| --- | --- |
| **Language** | Rust (edition 2021) |
| **License** | GPL-3.0-or-later |
| **Target** | CachyOS/Arch · Linux-native · Rust |

## Why SheshAOS

| Problem | SheshAOS |
| --- | --- |
| AI tools act without oversight |  **Governance-first** — the kernel validates every action |
| State is mutable |  **Event-sourced** — append-only, hash-chained audit log |
| Cloud-dependent |  **Local-first** — works fully offline |
| Locked to one model |  **Provider interface** — OpenAI-compatible + Anthropic, LiteLLM routing |
| No terminal integration |  **Wave Terminal (stock)** + PTY/VT100 + SSH multiplexing |

## Architecture

```mermaid
---
title: SheshAOS system layers
---
graph TB
    subgraph interface["Interface Layer"]
        CLI[" CLI<br/>shesh-cli"]
        WAVE[" Wave Terminal<br/>(stock, ADR-0016)"]
        RPC[" RPC<br/>shesh-rpc"]
    end
    subgraph kernel["Kernel Core"]
        K[" Kernel<br/>shesh-kernel"]
        P[" Policy Engine"]
        R[" Task Router"]
        S["⏰ Scheduler"]
    end
    subgraph model["Model Layer"]
        PL[" Planner"]
        CO[" Coder"]
        VI[" Vision"]
    end
    subgraph exec["Execution Layer"]
        T[" Tool Broker"]
        B[" Block Controller<br/>shesh-blockctl"]
        RM[" Remote Shell<br/>shesh-remote"]
    end
    subgraph storage["Storage Layer"]
        WO[" WaveObj Store<br/>shesh-waveobj"]
        WP[" Pub/Sub Broker<br/>shesh-wps"]
        ES[" Event Store"]
        SN[" Snapshots"]
    end
    CLI --> K
    WAVE --> RPC
    RPC --> K
    K -->|validates via| P
    K -->|routes via| R
    K -->|schedules via| S
    R -->|plans with| PL
    PL -->|delegates to| CO
    CO -->|reviews with| VI
    K -->|dispatches to| T
    K -->|drives| B
    K -->|manages| RM
    K -->|persists to| WO
    K -->|publishes via| WP
    K -->|records to| ES
    ES -->|compacts into| SN
```
```mermaid
---
title: SheshAOS runtime data flow
---
graph LR
    A[" Submit Task"] --> B[" Dedup Check"]
    B --> C[" Policy Check"]
    C --> D[" Route Task"]
    D --> E[" Plan"]
    E --> F[" Code"]
    F --> G[" Review"]
    G --> H[" Execute Tools"]
    H --> I[" Record Events"]
    I --> J[" Update State"]
    J --> K[" Snapshot"]
```
```mermaid
---
title: SheshAOS Wave object model
---
graph TD
    A["WaveObj trait"] --> B["Block"]
    A --> C["Job"]
    A --> D["Window"]
    A --> E["Workspace"]
    A --> F["Tab"]
    A --> G["LayoutState"]
    B -->|parent of| F
    B -->|contains| B
    H["ORef"] -->|references| A
    I["MetaMap"] -->|describes| A
    J["WaveStore"] -->|persists| A
```
```mermaid
---
title: SheshAOS design principles
---
graph LR
    A[" Kernel owns truth"] --> B[" Event sourcing"]
    B --> C[" Governance first"]
    C --> D[" Local first"]
    D --> E[" Models are replaceable"]
```

## Key features
### AI engine

- Streaming responses from OpenAI-compatible and Anthropic endpoints (SSE)
- LiteLLM-compatible model routing; local-first inference, fully offline
- Multi-modal (vision) support; session history + context management

### Block & shell control

- PTY block controller (backpressure-aware reads) — the layer Wave blocks ride on
- Remote PTY shell tunneling via **russh**

### Security & governance

- Policy engine with trust tiers and capability-based security
- Approval gating for destructive operations
- Append-only event store with cryptographic integrity (hash chain + verify)
- SSH multiplexing with connection monitoring
- Secrets vault (`shesh-vault`)

### Remote management

- Native SSH client (russh), connection health monitoring, remote PTY
- Config watcher with live reload (`shesh-wconfig`)

### Interfaces

- **Wave Terminal (stock)** — mission-control surface (ADR-0016)
- **CLI** — `shesh` init / run / doctor / status / replay
- **IPC** — JSON-RPC 2.0 over Unix sockets (`shesh-rpc`)

## Hardware target

| Component | Specification |
| --- | --- |
| **CPU** | Intel Core i7-14700HX |
| **GPU** | NVIDIA RTX 4050 (6 GB VRAM) |
| **Memory** | 16 GB DDR5 |
| **OS** | Linux — CachyOS/Arch + Hyprland (primary) |
| **Storage** | NVMe SSD |

## Model stack (6 GB VRAM budget)

| Role | Model | Use case |
| --- | --- | --- |
|  **Planner** | local first (phi4-mini / Gemma class) | architecture, planning, review |
|  **Coder** | qwen2.5-coder:3b class | implementation, debugging, tests |
|  **Vision** | moondream2 class | screenshots, diagrams, documents |

## Project structure

```text
SheshAOS/
├── bin/shesh-cli/          #  CLI entrypoint
├── crates/
│   ├── shesh-kernel/       #  governance microkernel (policy, router, scheduler)
│   ├── shesh-waveobj/      #  object store & ORef graph
│   ├── shesh-wps/          #  pub/sub event broker
│   ├── shesh-blockctl/     #  PTY shell controller
│   ├── shesh-ai/           #  OpenAI/Anthropic streaming + LiteLLM routing
│   ├── shesh-remote/       #  SSH remote shell (russh)
│   ├── shesh-rpc/          #  Unix-socket JSON-RPC
│   ├── shesh-vault/        #  command snippets & inspector
│   └── shesh-wconfig/      #  config watcher & settings
├── bootstrap/              #  workspace-excluded host-provisioning crate
├── configs/                # example configuration files
├── scripts/                # dev/test helper scripts
├── docs/                   # architecture (event model → crates/shesh-kernel/src/events.rs)
├── fuzz/                   # libfuzzer targets (config parse, event JSON)
├── Cargo.toml              # workspace definition
└── Makefile                # build shortcuts
```

## Development

```bash
cargo build                  # build
cargo test --workspace       # the workspace test suite
cargo clippy --all-targets -- -D warnings   # zero-warning gate
cargo fmt --check            # formatting
cargo bench --workspace      # 6 criterion benches
```

## Documentation

| Document | Purpose |
| --- | --- |
| [ Architecture](docs/architecture.md) | System diagrams & data flows |
| [ Compiled docs](https://github.com/gaganjainse/shesh-docs) | Fleet-wide reading compilation (mdBook) |
| [ Handover](HANDOVER.md) | Developer transition guide |
| [ Contributing](CONTRIBUTING.md) | Development workflow |
| [ Security](SECURITY.md) | Vulnerability reporting |
| [ Changelog](CHANGELOG.md) | Version history |
| [ Code of Conduct](CODE_OF_CONDUCT.md) | Community standards |

## Status

Development and security guidance live in [CONTRIBUTING.md](CONTRIBUTING.md)
and [SECURITY.md](SECURITY.md). Fleet-wide documentation lives in
[shesh-docs](https://github.com/gaganjainse/shesh-docs).

## License

GPL-3.0-or-later — see [LICENSE](LICENSE).

---

Built by [gaganjainse](https://github.com/gaganjainse).
