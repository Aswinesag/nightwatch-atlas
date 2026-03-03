import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import Map from "./components/Map"
import Alerts from "./components/Alerts"
import EventDetails from "./components/EventDetails"
import Filters from "./components/Filters"
import LiveNews from "./components/LiveNews"
import LiveWebcams from "./components/LiveWebcams"
import AIBrief from "./components/AIBrief"
import "./styles.css"
import type { Event, MapBounds, WebSocketState } from "./types"

type FilterMap = Record<string, boolean>
type LayerVisibility = {
  boundaries: boolean
  flights: boolean
  vessels: boolean
  conflicts: boolean
  cyber: boolean
  events: boolean
}

function formatTime(value?: string) {
  if (!value) return "--"
  const date = new Date(normalizeTimestamp(value))
  if (Number.isNaN(date.getTime())) return "--"
  return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })
}

function normalizeTimestamp(value: string) {
  // Backend emits naive UTC-like timestamps; treat them as UTC in browser parsing.
  if (/[zZ]$|[+\-]\d{2}:\d{2}$/.test(value)) return value
  return `${value}Z`
}

function riskBand(risk: number) {
  if (risk >= 0.82) return "critical"
  if (risk >= 0.65) return "high"
  if (risk >= 0.45) return "medium"
  return "low"
}

function App() {
  const apiBaseUrl = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000"
  const wsUrl = import.meta.env.VITE_WS_URL || "ws://localhost:8000/ws"

  const [events, setEvents] = useState<Event[]>([])
  const [selectedEvent, setSelectedEvent] = useState<Event | null>(null)
  const [loading, setLoading] = useState(true)
  const [webSocketState, setWebSocketState] = useState<WebSocketState>({
    connected: false,
    error: null,
    lastMessage: null,
  })
  const [filters, setFilters] = useState<FilterMap>({
    military: true,
    political: true,
    infrastructure: true,
    general: true,
  })
  const [layerVisibility, setLayerVisibility] = useState<LayerVisibility>({
    boundaries: true,
    flights: true,
    vessels: true,
    conflicts: true,
    cyber: true,
    events: true,
  })
  const [timeWindowMinutes, setTimeWindowMinutes] = useState(180)
  const [clockText, setClockText] = useState("")
  const [mapEvents, setMapEvents] = useState<Event[]>([])
  const [mapBounds, setMapBounds] = useState<MapBounds | null>(null)

  const wsRef = useRef<WebSocket | null>(null)
  const reconnectTimerRef = useRef<number | null>(null)

  const normalizeEvent = useCallback((event: any): Event => {
    const normalizedRisk = typeof event.risk === "number" ? event.risk : 0.3
    const normalizedTime = event.time || event.created_at || event.timestamp || new Date().toISOString()

    return {
      ...event,
      lat: event.lat ?? event.latitude ?? null,
      lon: event.lon ?? event.longitude ?? null,
      risk: normalizedRisk,
      severity: event.severity || riskBand(normalizedRisk),
      type: (event.type || "general").toLowerCase(),
      time: normalizedTime,
      alert_level: event.alert_level || event.severity || riskBand(normalizedRisk),
      source: event.source || "Unknown",
    }
  }, [])

  const fetchInitialEvents = useCallback(async () => {
    try {
      const response = await fetch(`${apiBaseUrl}/events?limit=260`)
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`)
      }
      const data = await response.json()
      if (Array.isArray(data)) {
        setEvents(data.map(normalizeEvent))
      }
      setWebSocketState((prev) => ({ ...prev, error: null }))
    } catch (error) {
      setWebSocketState((prev) => ({
        ...prev,
        error: `Failed to fetch events: ${(error as Error).message}`,
      }))
    } finally {
      setLoading(false)
    }
  }, [apiBaseUrl, normalizeEvent])

  const fetchRecentOtxEvents = useCallback(async () => {
    try {
      const response = await fetch(`${apiBaseUrl}/events?limit=2200`)
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`)
      }
      const data = await response.json()
      if (!Array.isArray(data)) {
        return
      }

      const otxEvents = data
        .map(normalizeEvent)
        .filter((event) => (event.source || "").toLowerCase() === "otx")
        .slice(0, 60)

      if (otxEvents.length === 0) {
        return
      }

      setEvents((prev) => {
        const merged = [...otxEvents, ...prev]
        const seen = new Set<string>()
        const deduped: Event[] = []
        for (const item of merged) {
          if (!seen.has(item.id)) {
            seen.add(item.id)
            deduped.push(item)
          }
          if (deduped.length >= 320) break
        }
        return deduped
      })
    } catch {
      // OTX merge is a best-effort enrichment path.
    }
  }, [apiBaseUrl, normalizeEvent])

  const fetchViewportEvents = useCallback(
    async (bounds: MapBounds) => {
      try {
        const params = new URLSearchParams({
          min_lat: bounds.min_lat.toString(),
          max_lat: bounds.max_lat.toString(),
          min_lon: bounds.min_lon.toString(),
          max_lon: bounds.max_lon.toString(),
          limit: bounds.zoom >= 5 ? "320" : bounds.zoom >= 3 ? "220" : "140",
          min_quality: "0.2",
        })
        const response = await fetch(`${apiBaseUrl}/events/bbox?${params.toString()}`)
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`)
        }
        const data = await response.json()
        if (Array.isArray(data)) {
          setMapEvents(data.map(normalizeEvent))
        }
      } catch {
        // Map viewport fetch is best effort; keep last known map events.
      }
    },
    [apiBaseUrl, normalizeEvent]
  )

  const connectWebSocket = useCallback(() => {
    if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
      return
    }

    const ws = new WebSocket(wsUrl)
    wsRef.current = ws

    ws.onopen = () => {
      setWebSocketState({ connected: true, error: null, lastMessage: "Connected to event stream" })
    }

    ws.onmessage = (event) => {
      try {
        const payload = JSON.parse(event.data)
        const normalizedEvent = normalizeEvent(payload)
        setEvents((prev) => [normalizedEvent, ...prev.filter((e) => e.id !== normalizedEvent.id)].slice(0, 220))
        setWebSocketState((prev) => ({
          ...prev,
          lastMessage: `${normalizedEvent.type.toUpperCase()} | ${formatTime(normalizedEvent.time)}`,
        }))
      } catch (error) {
        setWebSocketState((prev) => ({
          ...prev,
          error: `Failed to parse WS message: ${(error as Error).message}`,
        }))
      }
    }

    ws.onerror = () => {
      setWebSocketState((prev) => ({ ...prev, connected: false, error: "WebSocket connection error" }))
    }

    ws.onclose = () => {
      setWebSocketState((prev) => ({ ...prev, connected: false, lastMessage: "Disconnected" }))
      reconnectTimerRef.current = window.setTimeout(() => {
        connectWebSocket()
      }, 3000)
    }
  }, [normalizeEvent, wsUrl])

  useEffect(() => {
    fetchInitialEvents()
    fetchRecentOtxEvents()
    connectWebSocket()

    const otxRefresh = window.setInterval(() => {
      fetchRecentOtxEvents()
    }, 120000)

    return () => {
      window.clearInterval(otxRefresh)
      if (reconnectTimerRef.current) {
        window.clearTimeout(reconnectTimerRef.current)
      }
      if (wsRef.current) {
        wsRef.current.close()
      }
    }
  }, [connectWebSocket, fetchInitialEvents, fetchRecentOtxEvents])

  useEffect(() => {
    if (!mapBounds) return
    fetchViewportEvents(mapBounds)
  }, [fetchViewportEvents, mapBounds])

  useEffect(() => {
    const tick = () => {
      setClockText(
        new Date().toLocaleString([], {
          weekday: "short",
          day: "2-digit",
          month: "short",
          year: "numeric",
          hour: "2-digit",
          minute: "2-digit",
          second: "2-digit",
          timeZoneName: "short",
        })
      )
    }
    tick()
    const timer = window.setInterval(tick, 1000)
    return () => window.clearInterval(timer)
  }, [])

  const availableTypes = useMemo(() => {
    const typeSet = new Set<string>(["general"])
    events.forEach((event) => typeSet.add((event.type || "general").toLowerCase()))
    return Array.from(typeSet).sort()
  }, [events])

  useEffect(() => {
    setFilters((prev) => {
      const next = { ...prev }
      let changed = false
      availableTypes.forEach((type) => {
        if (!(type in next)) {
          next[type] = true
          changed = true
        }
      })
      return changed ? next : prev
    })
  }, [availableTypes])

  const filteredEvents = useMemo(
    () =>
      events.filter((event) => {
        const eventType = (event.type || "general").toLowerCase()
        return filters[eventType] ?? true
      }),
    [events, filters]
  )

  const activeEvents = useMemo(() => {
    const cutoff = Date.now() - timeWindowMinutes * 60_000
    return filteredEvents.filter((event) => {
      const raw = event.time || event.created_at || event.timestamp
      if (!raw) return true
      const ts = new Date(normalizeTimestamp(raw)).getTime()
      if (!Number.isFinite(ts)) return true
      return ts >= cutoff
    })
  }, [filteredEvents, timeWindowMinutes])

  const geolocatedCount = useMemo(
    () =>
      activeEvents.filter((event) => {
        const lat = event.lat ?? event.latitude
        const lon = event.lon ?? event.longitude
        return typeof lat === "number" && Number.isFinite(lat) && typeof lon === "number" && Number.isFinite(lon)
      }).length,
    [activeEvents]
  )

  const flightCount = useMemo(
    () => activeEvents.filter((event) => (event.source || "").toLowerCase() === "opensky").length,
    [activeEvents]
  )

  const conflictCount = useMemo(
    () =>
      activeEvents.filter((event) => {
        const type = (event.type || "").toLowerCase()
        return type === "military" || type === "political" || event.risk >= 0.7
      }).length,
    [activeEvents]
  )

  const otxCount = useMemo(
    () => activeEvents.filter((event) => (event.source || "").toLowerCase() === "otx").length,
    [activeEvents]
  )

  const latestEvents = useMemo(() => activeEvents.slice(0, 7), [activeEvents])
  const renderedMapEvents = useMemo(() => (mapEvents.length > 0 ? mapEvents : activeEvents), [activeEvents, mapEvents])

  if (loading) {
    return (
      <div className="intel-loading">
        <div className="loading-core" />
        <div className="loading-text">Bootstrapping Real-Time Intelligence Fabric</div>
      </div>
    )
  }

  return (
    <div className="intel-shell">
      <header className="intel-topbar worldmonitor-topbar">
        <div className="brand-cluster">
          <div className="brand-kicker">NIGHTWATCH NETWORK</div>
          <h1>NightWatch Atlas</h1>
        </div>
        <div className="topbar-metrics">
          <div className="metric-card">
            <span>Total Events</span>
            <strong>{activeEvents.length}</strong>
          </div>
          <div className="metric-card">
            <span>Flight Tracks</span>
            <strong>{flightCount}</strong>
          </div>
          <div className="metric-card">
            <span>Conflict</span>
            <strong>{conflictCount}</strong>
          </div>
          <div className="metric-card">
            <span>OTX Cyber</span>
            <strong>{otxCount}</strong>
          </div>
          <div className="metric-card">
            <span>Geolocated</span>
            <strong>{geolocatedCount}</strong>
          </div>
        </div>
        <div className={`status-chip ${webSocketState.connected ? "is-live" : "is-down"}`}>
          {webSocketState.connected ? "LIVE FEED" : "DISCONNECTED"}
        </div>
      </header>

      <section className="intel-commandbar worldmonitor-commandbar">
        <div className="clock-badge">{clockText}</div>
        <div className="time-window">
          <label htmlFor="window-range">Operational window: {timeWindowMinutes}m</label>
          <input
            id="window-range"
            type="range"
            min={15}
            max={720}
            step={15}
            value={timeWindowMinutes}
            onChange={(event) => setTimeWindowMinutes(Number(event.target.value))}
          />
        </div>
        <div className="layer-toggles">
          {Object.entries(layerVisibility).map(([key, enabled]) => (
            <button
              key={key}
              type="button"
              className={enabled ? "is-on" : ""}
              onClick={() =>
                setLayerVisibility((prev) => ({
                  ...prev,
                  [key]: !prev[key as keyof LayerVisibility],
                }))
              }
            >
              {key}
            </button>
          ))}
        </div>
      </section>

      <main className="wm-layout">
        <section className="intel-map-stage wm-map-stage">
          <aside className="wm-map-left">
            <Filters
              availableTypes={availableTypes}
              filters={filters}
              setFilters={setFilters}
              events={events}
            />
            <section className="intel-panel intel-briefing">
              <header>
                <span>Field Pulse</span>
                <small>{webSocketState.lastMessage || "Awaiting feed pulse"}</small>
              </header>
              <ul>
                {latestEvents.map((event) => (
                  <li key={event.id} onClick={() => setSelectedEvent(event)}>
                    <div className={`event-dot is-${riskBand(event.risk)}`} />
                    <div>
                      <strong>{event.title}</strong>
                      <span>{event.source || "Unknown"} | {formatTime(event.time)}</span>
                    </div>
                  </li>
                ))}
              </ul>
            </section>
          </aside>

          <Map
            events={renderedMapEvents}
            setSelectedEvent={setSelectedEvent}
            onViewportChange={setMapBounds}
            layerVisibility={layerVisibility}
          />
          {webSocketState.error && <div className="floating-error">{webSocketState.error}</div>}
          <div className="intel-map-overlay">Global Situation</div>
          <aside className="wm-map-right">
            <Alerts events={activeEvents} onSelectEvent={setSelectedEvent} selectedEventId={selectedEvent?.id ?? null} />
          </aside>
        </section>

        <section className="wm-bottom-deck">
          <LiveNews events={activeEvents} onSelectEvent={setSelectedEvent} />
          <LiveWebcams events={activeEvents} selectedEvent={selectedEvent} />
          <div className="wm-ai-stack">
            <AIBrief apiBaseUrl={apiBaseUrl} selectedEvent={selectedEvent} events={activeEvents} />
            <EventDetails event={selectedEvent} />
          </div>
        </section>
      </main>
    </div>
  )
}

export default App
