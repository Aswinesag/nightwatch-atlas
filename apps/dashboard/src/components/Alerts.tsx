import { useMemo } from "react"
import type { Event } from "../types"

interface AlertsProps {
  events: Event[]
  onSelectEvent: (event: Event) => void
  selectedEventId: string | null
}

function severityClass(event: Event) {
  if (event.risk >= 0.82) return "critical"
  if (event.risk >= 0.65) return "high"
  if (event.risk >= 0.45) return "medium"
  return "low"
}

function displayTime(event: Event) {
  const t = event.time || event.created_at || event.timestamp
  if (!t) return "--"
  const date = new Date(t)
  if (Number.isNaN(date.getTime())) return "--"
  return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })
}

export default function Alerts({ events, onSelectEvent, selectedEventId }: AlertsProps) {
  const highPriority = useMemo(
    () =>
      events
        .filter((event) => {
          const type = (event.type || "general").toLowerCase()
          return event.risk >= 0.58 || type === "military" || type === "political"
        })
        .sort((a, b) => b.risk - a.risk)
        .slice(0, 18),
    [events]
  )

  const criticalCount = highPriority.filter((event) => event.risk >= 0.82).length
  const activeFlights = events.filter((event) => (event.source || "").toLowerCase() === "opensky").length

  return (
    <section className="intel-panel alert-panel">
      <header>
        <span>Realtime Alerts</span>
        <small>{highPriority.length} active</small>
      </header>

      <div className="alert-stats">
        <div>
          <strong>{criticalCount}</strong>
          <span>Critical</span>
        </div>
        <div>
          <strong>{activeFlights}</strong>
          <span>Flights</span>
        </div>
        <div>
          <strong>{events.length}</strong>
          <span>Total</span>
        </div>
      </div>

      <ul className="alert-list">
        {highPriority.map((event) => {
          const severity = severityClass(event)
          return (
            <li key={event.id}>
              <button
                type="button"
                className={`alert-card ${severity} ${selectedEventId === event.id ? "is-selected" : ""}`}
                onClick={() => onSelectEvent(event)}
              >
                <div className="alert-meta">
                  <span>{event.source || "Unknown"}</span>
                  <span>{displayTime(event)}</span>
                </div>
                <strong>{event.title}</strong>
                <p>{event.description}</p>
                <div className="alert-footer">
                  <span className="alert-tag">{(event.type || "general").toUpperCase()}</span>
                  <span>Risk {event.risk.toFixed(2)}</span>
                </div>
              </button>
            </li>
          )
        })}
      </ul>
    </section>
  )
}
