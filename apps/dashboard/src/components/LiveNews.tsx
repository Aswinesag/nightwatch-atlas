import { useMemo } from "react"
import type { Event } from "../types"

interface LiveNewsProps {
  events: Event[]
  onSelectEvent: (event: Event) => void
}

function toTimestamp(value?: string) {
  if (!value) return 0
  const normalized = /[zZ]$|[+\-]\d{2}:\d{2}$/.test(value) ? value : `${value}Z`
  const ts = new Date(normalized).getTime()
  return Number.isFinite(ts) ? ts : 0
}

function displaySource(source?: string) {
  if (!source) return "Unknown"
  const s = source.toLowerCase()
  if (s === "opensky") return "OpenSky"
  if (s === "otx") return "OTX"
  if (s === "gdelt") return "GDELT"
  return source
}

export default function LiveNews({ events, onSelectEvent }: LiveNewsProps) {
  const headlines = useMemo(
    () =>
      events
        .filter((event) => (event.source || "").toLowerCase() !== "opensky")
        .sort(
          (a, b) =>
            toTimestamp(b.time || b.created_at || b.timestamp) - toTimestamp(a.time || a.created_at || a.timestamp)
        )
        .slice(0, 14),
    [events]
  )

  return (
    <section className="intel-panel wm-news-panel">
      <header>
        <span>Live News</span>
        <small>{headlines.length} live</small>
      </header>
      <ul>
        {headlines.map((event) => (
          <li key={event.id}>
            <button type="button" onClick={() => onSelectEvent(event)}>
              <strong>{event.title}</strong>
              <div>
                <span>{displaySource(event.source)}</span>
                <span>Risk {event.risk.toFixed(2)}</span>
              </div>
            </button>
          </li>
        ))}
      </ul>
    </section>
  )
}
