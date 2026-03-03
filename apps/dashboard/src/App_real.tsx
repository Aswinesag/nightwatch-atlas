import { useEffect, useState } from "react"
import Map from "./components/Map"
import Alerts from "./components/Alerts"
import EventDetails from "./components/EventDetails"
import DynamicFilters from "./components/DynamicFilters"
import type { Event } from "./types"

interface FilterConfig {
  [key: string]: boolean
}

function App() {
  const [events, setEvents] = useState<Event[]>([])
  const [selectedEvent, setSelectedEvent] = useState<Event | null>(null)
  const [availableTypes, setAvailableTypes] = useState<string[]>([])
  const [filters, setFilters] = useState<FilterConfig>({})
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  // Initialize filters dynamically based on available event types
  useEffect(() => {
    const types = [...new Set(events.map(e => e.type))]
    setAvailableTypes(types)
    
    // Initialize filters for all available types
    const initialFilters: FilterConfig = {}
    types.forEach(type => {
      initialFilters[type] = true
    })
    setFilters(initialFilters)
  }, [events])

  // Fetch initial events and setup WebSocket
  useEffect(() => {
    console.log("Setting up real-time connection...")
    setLoading(true)
    
    // Fetch initial events
    fetchInitialEvents()
    
    // Setup WebSocket for real-time updates
    setupWebSocket()
    
    return () => {
      console.log("Cleaning up connections")
    }
  }, [])

  const fetchInitialEvents = async () => {
    try {
      const response = await fetch("http://localhost:8000/events?limit=100")
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`)
      }
      const data = await response.json()
      setEvents(data)
      setError(null)
    } catch (error) {
      console.error("Error fetching initial events:", error)
      setError("Failed to fetch initial events")
    } finally {
      setLoading(false)
    }
  }

  const setupWebSocket = () => {
    const ws = new WebSocket("ws://localhost:8000/ws")

    ws.onopen = () => {
      console.log("WebSocket connected successfully")
    }

    ws.onmessage = (event) => {
      try {
        console.log("Received real-time event")
        const data = JSON.parse(event.data)
        
        // Validate event structure
        if (isValidEvent(data)) {
          setEvents(prev => {
            // Avoid duplicates
            if (prev.some(e => e.id === data.id)) {
              return prev
            }
            return [data, ...prev.slice(0, 99)] // Keep latest 100 events
          })
        } else {
          console.warn("Received invalid event structure:", data)
        }
      } catch (error) {
        console.error("Error parsing WebSocket message:", error)
      }
    }

    ws.onerror = (error) => {
      console.error("WebSocket error:", error)
      setError("WebSocket connection error")
    }

    ws.onclose = (event) => {
      console.log("WebSocket disconnected:", event.code, event.reason)
      if (event.code !== 1000) {
        setError("WebSocket connection lost")
        // Attempt to reconnect after 5 seconds
        setTimeout(() => {
          console.log("Attempting to reconnect...")
          setupWebSocket()
        }, 5000)
      }
    }
  }

  const isValidEvent = (event: any): event is Event => {
    return (
      event &&
      typeof event.id === 'string' &&
      typeof event.title === 'string' &&
      typeof event.risk === 'number' &&
      typeof event.description === 'string' &&
      (event.lat === null || typeof event.lat === 'number') &&
      (event.lon === null || typeof event.lon === 'number') &&
      typeof event.severity === 'string' &&
      typeof event.type === 'string' &&
      typeof event.time === 'string' &&
      typeof event.created_at === 'string' &&
      typeof event.source === 'string'
    )
  }

  const filteredEvents = events.filter(e => filters[e.type] !== false)

  const handleFilterChange = (type: string, enabled: boolean) => {
    setFilters(prev => ({
      ...prev,
      [type]: enabled
    }))
  }

  const handleEventSelect = (event: Event) => {
    setSelectedEvent(event)
  }

  if (loading) {
    return (
      <div style={{ 
        display: "flex", 
        justifyContent: "center", 
        alignItems: "center", 
        height: "100vh", 
        backgroundColor: "#000",
        color: "#fff"
      }}>
        <div>Loading intelligence data...</div>
      </div>
    )
  }

  if (error) {
    return (
      <div style={{ 
        display: "flex", 
        justifyContent: "center", 
        alignItems: "center", 
        height: "100vh", 
        backgroundColor: "#000",
        color: "#ff4444"
      }}>
        <div>
          <h2>Error: {error}</h2>
          <button onClick={() => window.location.reload()}>Reload</button>
        </div>
      </div>
    )
  }

  return (
    <div style={{ display: "flex", height: "100vh", backgroundColor: "#000" }}>
      <div style={{ flex: 1, height: "100%" }}>
        <Map 
          events={filteredEvents} 
          setSelectedEvent={handleEventSelect}
          layerVisibility={{
            boundaries: true,
            flights: true,
            vessels: true,
            conflicts: true,
            cyber: true,
            events: true,
          }}
        />
      </div>
      <div style={{ width: "300px", height: "100%", overflow: "auto" }}>
        <EventDetails event={selectedEvent} />
        <Alerts events={filteredEvents} onSelectEvent={handleEventSelect} selectedEventId={selectedEvent?.id ?? null} />
        <DynamicFilters 
          availableTypes={availableTypes}
          filters={filters}
          onFilterChange={handleFilterChange}
        />
      </div>
    </div>
  )
}

export default App
