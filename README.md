# NightWatch Atlas

NightWatch Atlas is a real-time intelligence platform for ingesting, processing, and visualizing global event streams on an operational map.  
The system is designed as a distributed data pipeline with a containerized control plane and a separate web dashboard.

## Key Capabilities

- Real-time event ingestion from multiple external intelligence sources
- Stream-based enrichment and normalization pipeline on Kafka
- Geospatial visualization with live map overlays and filterable layers
- Global Fishing Watch (GFW) vessel tracking with real-time maritime movement overlays
- WebSocket-driven alert updates for low-latency operator workflows
- Operational AI Brief generation via Groq API with deterministic fallback mode
- API-first control plane for integration with external systems

## System Overview

NightWatch Atlas consists of:

- `harvesters`: collect raw telemetry/events from external sources (OpenSky, OTX, GDELT)
- `processors`: normalize/enrich events and publish to the `events` stream
- `control-plane`: persists events, exposes REST + WebSocket APIs, and generates AI briefings
- `dashboard`: interactive operations UI with map, alerts, live feeds, and AI Brief panel
- `infra`: Docker Compose stack for runtime orchestration

Core data flow:

1. Source harvesters publish to Kafka
2. Processors transform and enrich records
3. Control plane consumes `events`, stores in PostgreSQL, broadcasts over WebSocket
4. Dashboard consumes REST snapshots + live WebSocket updates

## Repository Layout

```text
apps/
  control-plane/     FastAPI service (REST, WS, AI brief endpoint)
  dashboard/         React + Vite frontend
infra/
  docker-compose.yml Runtime stack (Kafka, Postgres, services)
services/
  harvester-*        Source-specific ingestors
  processor-ai/      Event processors
packages/
  messaging/         Shared Kafka utilities
```

## Prerequisites

- Docker Desktop (or Docker Engine + Compose plugin)
- Node.js 20+ and npm
- Python 3.11 (for optional local scripts/checks)

## Configuration

Create or update root `.env` with required provider credentials.

Typical keys used by this stack:

- `OPENSKY_USER`
- `OPENSKY_PASS`
- `OPENSKY_API_KEY`
- `OTX_KEY`
- `GROQ_API_KEY` (required for LLM-generated AI brief)
- `GROQ_MODEL` (optional, defaults to `llama-3.3-70b-versatile`)

OpenSky OAuth client credentials can be supplied through `credentials.json` at repository root.  
`infra/docker-compose.yml` mounts this file into the OpenSky harvester container.

## Runbook: Start the Platform

### 1) Start backend stack

```bash
docker compose -f infra/docker-compose.yml up -d --build
```

### 2) Start dashboard

```bash
cd apps/dashboard
npm install
npm run dev -- --host 0.0.0.0 --port 5173
```

### 3) Access

- Dashboard: `http://localhost:5173`
- API: `http://localhost:8000`
- Health: `http://localhost:8000/health`

## Runbook: Stop / Restart

Stop stack:

```bash
docker compose -f infra/docker-compose.yml down
```

Restart control plane only (for API changes):

```bash
docker compose -f infra/docker-compose.yml up -d --build control-plane
```

## API Surface (Control Plane)

Primary endpoints:

- `GET /health` - service health + websocket client count
- `GET /events` - event query with filtering and limits
- `GET /events/stats` - aggregate event statistics
- `GET /events/types` - distinct event types
- `GET /events/bbox` - geospatial event query
- `POST /ai/brief` - operational summary from recent events (Groq-backed, fallback available)
- `WS /ws` - real-time event stream

## AI Brief Behavior

`POST /ai/brief` uses Groq when `GROQ_API_KEY` is available in control-plane runtime env.  
If Groq is unavailable or the call fails, the endpoint returns a deterministic fallback brief.

Response fields include:

- `brief`
- `model`
- `used_fallback`
- `context_events`
- `generated_at`

## Operations and Diagnostics

View service status:

```bash
docker compose -f infra/docker-compose.yml ps
```

Tail control-plane logs:

```bash
docker compose -f infra/docker-compose.yml logs -f control-plane
```

Tail OpenSky pipeline logs:

```bash
docker compose -f infra/docker-compose.yml logs -f harvester-opensky processor-opensky
```

Quick API checks:

```bash
curl http://localhost:8000/health
curl "http://localhost:8000/events?limit=5"
```

## Production Considerations

- Keep credentials out of source control; use secret management in production.
- Place Kafka/Postgres behind private network boundaries.
- Configure TLS and auth in front of API/WebSocket endpoints.
- Apply retention and compaction policies for Kafka topics and database tables.
- Add centralized logging and metrics (e.g., OpenTelemetry + Prometheus/Grafana).
- Use CI/CD with image scanning, pinned dependencies, and rollout health checks.

## Security Notes

- `.env` and `credentials.json` contain sensitive material; protect file permissions.
- Rotate API keys periodically.
- Restrict CORS origins and WebSocket exposure in non-dev environments.
- Validate third-party feed licensing/terms before commercial deployment.

VIDEO DEMO LINK : https://vimeo.com/1170005346?fl=ip&fe=ec
