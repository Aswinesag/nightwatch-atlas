import type { Dispatch, SetStateAction } from "react"
import type { Event } from "../types"

interface FiltersProps {
  availableTypes: string[]
  filters: Record<string, boolean>
  setFilters: Dispatch<SetStateAction<Record<string, boolean>>>
  events: Event[]
}

function typeColor(type: string) {
  if (type === "military") return "#ef4444"
  if (type === "political") return "#8b5cf6"
  if (type === "infrastructure") return "#38bdf8"
  return "#14b8a6"
}

export default function Filters({ availableTypes, filters, setFilters, events }: FiltersProps) {
  const typeCounts = availableTypes.reduce<Record<string, number>>((acc, type) => {
    acc[type] = events.filter((event) => (event.type || "general").toLowerCase() === type).length
    return acc
  }, {})

  const activeCount = Object.values(filters).filter(Boolean).length

  const toggleType = (type: string) => {
    setFilters((prev) => ({
      ...prev,
      [type]: !(prev[type] ?? true),
    }))
  }

  const setAll = (enabled: boolean) => {
    setFilters((prev) => {
      const next = { ...prev }
      availableTypes.forEach((type) => {
        next[type] = enabled
      })
      return next
    })
  }

  return (
    <section className="intel-panel filter-panel">
      <header>
        <span>Threat Taxonomy</span>
        <small>{activeCount}/{availableTypes.length} active</small>
      </header>

      <div className="filter-actions">
        <button onClick={() => setAll(true)} type="button">Enable All</button>
        <button onClick={() => setAll(false)} type="button">Mute All</button>
      </div>

      <ul className="filter-list">
        {availableTypes.map((type) => (
          <li key={type}>
            <button type="button" className={filters[type] ? "is-active" : ""} onClick={() => toggleType(type)}>
              <span className="type-marker" style={{ background: typeColor(type) }} />
              <span className="type-name">{type}</span>
              <span className="type-count">{typeCounts[type] ?? 0}</span>
            </button>
          </li>
        ))}
      </ul>
    </section>
  )
}
