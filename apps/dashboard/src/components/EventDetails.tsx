import type { Event } from "../types"

interface EventDetailsProps {
  event: Event | null
}

function formatTimestamp(event: Event) {
  const raw = event.time || event.created_at || event.timestamp
  if (!raw) return "--"
  const date = new Date(raw)
  if (Number.isNaN(date.getTime())) return "--"
  return date.toLocaleString()
}

function formatCoord(value?: number | null) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--"
  return value.toFixed(4)
}

export default function EventDetails({ event }: EventDetailsProps) {
  if (!event) {
    return (
      <section className="intel-panel details-panel empty">
        <header>
          <span>Event Inspector</span>
        </header>
        <p>Select an alert or map point to inspect context, geolocation, and metadata.</p>
      </section>
    )
  }

  return (
    <section className="intel-panel details-panel">
      <header>
        <span>Event Inspector</span>
        <small>{(event.type || "general").toUpperCase()}</small>
      </header>

      <h3>{event.title}</h3>
      <p>{event.description}</p>

      <div className="details-grid">
        <div>
          <label>Source</label>
          <span>{event.source || "Unknown"}</span>
        </div>
        <div>
          <label>Risk Score</label>
          <span>{event.risk.toFixed(2)}</span>
        </div>
        <div>
          <label>Severity</label>
          <span>{(event.alert_level || event.severity || "low").toUpperCase()}</span>
        </div>
        <div>
          <label>Timestamp</label>
          <span>{formatTimestamp(event)}</span>
        </div>
        <div>
          <label>Latitude</label>
          <span>{formatCoord(event.lat ?? event.latitude)}</span>
        </div>
        <div>
          <label>Longitude</label>
          <span>{formatCoord(event.lon ?? event.longitude)}</span>
        </div>
      </div>
    </section>
  )
}
