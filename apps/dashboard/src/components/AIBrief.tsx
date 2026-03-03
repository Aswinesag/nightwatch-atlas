import { useEffect, useMemo, useState } from "react"
import type { Event } from "../types"

interface AIBriefProps {
  apiBaseUrl: string
  selectedEvent: Event | null
  events: Event[]
}

interface BriefPayload {
  brief: string
  generated_at: string
  model: string
  used_fallback: boolean
  context_events: number
}

function formatGeneratedAt(value?: string) {
  if (!value) return "--"
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return "--"
  return date.toLocaleString()
}

export default function AIBrief({ apiBaseUrl, selectedEvent, events }: AIBriefProps) {
  const [brief, setBrief] = useState<BriefPayload | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const eventSignature = useMemo(
    () => events.slice(0, 24).map((event) => event.id).join("|"),
    [events]
  )

  useEffect(() => {
    let alive = true
    const timer = window.setTimeout(async () => {
      try {
        setLoading(true)
        setError(null)
        const response = await fetch(`${apiBaseUrl}/ai/brief`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            selected_event_id: selectedEvent?.id ?? null,
            limit: 140,
          }),
        })
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`)
        }
        const data = (await response.json()) as BriefPayload
        if (alive) {
          setBrief(data)
        }
      } catch (err) {
        if (alive) {
          setError((err as Error).message)
        }
      } finally {
        if (alive) {
          setLoading(false)
        }
      }
    }, 700)

    return () => {
      alive = false
      window.clearTimeout(timer)
    }
  }, [apiBaseUrl, eventSignature, selectedEvent?.id])

  return (
    <section className="intel-panel ai-brief-panel">
      <header>
        <span>AI Brief</span>
        <small>{brief?.used_fallback ? "Fallback" : "Groq"}</small>
      </header>

      <div className="ai-brief-meta">
        <span>Model: {brief?.model || "--"}</span>
        <span>Context: {brief?.context_events ?? 0} events</span>
        <span>Generated: {formatGeneratedAt(brief?.generated_at)}</span>
      </div>

      {loading && <div className="ai-brief-state">Generating live brief...</div>}
      {error && <div className="ai-brief-state is-error">Brief unavailable: {error}</div>}
      {!loading && !error && (
        <pre className="ai-brief-content">{brief?.brief || "No AI brief available yet."}</pre>
      )}
    </section>
  )
}
