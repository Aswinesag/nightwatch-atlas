export interface Event {
  id: string
  title: string
  risk: number
  description: string
  lat?: number | null
  lon?: number | null
  latitude?: number | null
  longitude?: number | null
  severity: string
  type: string
  time?: string
  created_at?: string
  alert_level?: string
  event_category?: string
  source?: string
  timestamp?: string
  url?: string
  confidence?: number
  quality_score?: number
  correlation_key?: string
  canonical_type?: string
  true_track?: number
  track?: number
  heading?: number
  bearing?: number
  course?: number
  cog?: number
  sog?: number
  speed?: number
  vessel_id?: string
  vessel_name?: string
}

export interface MapBounds {
  min_lat: number
  max_lat: number
  min_lon: number
  max_lon: number
  zoom: number
}

export interface WebSocketState {
  connected: boolean
  error: string | null
  lastMessage: string | null
}
