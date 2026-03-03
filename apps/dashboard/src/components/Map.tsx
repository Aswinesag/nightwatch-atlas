import { useMemo } from "react"
import { GeoJsonLayer, IconLayer, ScatterplotLayer } from "@deck.gl/layers"
import { MapboxOverlay } from "@deck.gl/mapbox"
import MapLibreMap, { NavigationControl, ScaleControl, useControl } from "react-map-gl/maplibre"
import "maplibre-gl/dist/maplibre-gl.css"
import type { Event } from "../types"
import type { MapBounds } from "../types"

interface MapProps {
  events: Event[]
  setSelectedEvent: (event: Event) => void
  onViewportChange?: (bounds: MapBounds) => void
  layerVisibility: {
    boundaries: boolean
    flights: boolean
    vessels: boolean
    conflicts: boolean
    cyber: boolean
    events: boolean
  }
}

interface EventPoint {
  id: string
  event: Event
  position: [number, number]
  radius: number
  color: [number, number, number]
  isConflict: boolean
  isFlight: boolean
  isCyber: boolean
  isVessel: boolean
}

interface FlightIcon {
  id: string
  event: Event
  position: [number, number]
  angle: number
  risk: number
}

interface VesselIcon {
  id: string
  event: Event
  position: [number, number]
  angle: number
  risk: number
}

type DeckOverlayProps = {
  layers: Array<ScatterplotLayer<EventPoint> | GeoJsonLayer | IconLayer<FlightIcon> | IconLayer<VesselIcon>>
}

const MAP_STYLE = "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json"
const COUNTRIES_GEOJSON_URL = "https://raw.githubusercontent.com/datasets/geo-countries/master/data/countries.geojson"
const PLANE_ICON_URL = "/plane-icon.svg"
const SHIP_ICON_URL = "/ship-icon.svg"

function colorForEvent(type: string, risk: number): [number, number, number] {
  if (type === "military") return [220, 38, 38]
  if (type === "political") return [168, 85, 247]
  if (type === "infrastructure") return [14, 165, 233]
  return risk >= 0.7 ? [249, 115, 22] : [20, 184, 166]
}

function DeckGLOverlay(props: DeckOverlayProps) {
  const overlay = useControl<MapboxOverlay>(() => new MapboxOverlay({ interleaved: true }))
  overlay.setProps(props)
  return null
}

function timestamp(event: Event) {
  const raw = event.time || event.created_at || event.timestamp
  const value = raw ? new Date(normalizeTimestamp(raw)).getTime() : 0
  return Number.isFinite(value) ? value : 0
}

function normalizeTimestamp(value: string) {
  if (/[zZ]$|[+\-]\d{2}:\d{2}$/.test(value)) return value
  return `${value}Z`
}

function flightTrack(event: Event) {
  const values = [event.true_track, event.track, event.heading, event.bearing]
  for (const value of values) {
    if (typeof value === "number" && Number.isFinite(value)) {
      const normalized = ((value % 360) + 360) % 360
      return normalized
    }
  }
  return 0
}

function vesselTrack(event: Event) {
  const values = [event.course, event.cog, event.heading, event.true_track, event.track, event.bearing]
  for (const value of values) {
    if (typeof value === "number" && Number.isFinite(value)) {
      const normalized = ((value % 360) + 360) % 360
      return normalized
    }
  }
  return 0
}

export default function Map({ events, setSelectedEvent, onViewportChange, layerVisibility }: MapProps) {
  const emitBounds = (target: any) => {
    if (!onViewportChange || !target?.getBounds || !target?.getZoom) return
    const bounds = target.getBounds()
    onViewportChange({
      min_lat: bounds.getSouth(),
      max_lat: bounds.getNorth(),
      min_lon: bounds.getWest(),
      max_lon: bounds.getEast(),
      zoom: target.getZoom(),
    })
  }

  const points = useMemo<EventPoint[]>(
    () => {
      const recentEvents = [...events].sort((a, b) => timestamp(b) - timestamp(a)).slice(0, 180)
      return recentEvents
        .map((event) => {
          const lat = event.lat ?? event.latitude
          const lon = event.lon ?? event.longitude
          if (typeof lat !== "number" || !Number.isFinite(lat)) return null
          if (typeof lon !== "number" || !Number.isFinite(lon)) return null

          const type = (event.type || "general").toLowerCase()
          const source = (event.source || "").toLowerCase()
          const isConflict = type === "military" || type === "political" || event.risk >= 0.74
          const isFlight = source === "opensky"
          const isVessel = source === "gfw" || source === "globalfishingwatch"
          const isCyber = source === "otx"

          return {
            id: event.id,
            event,
            isConflict,
            isFlight,
            isVessel,
            isCyber,
            position: [lon, lat] as [number, number],
            radius: Math.max(2400, Math.min(14000, 1800 + event.risk * 13000)),
            color: colorForEvent(type, event.risk),
          }
        })
        .filter((point): point is EventPoint => Boolean(point))
    },
    [events]
  )

  const flightIcons = useMemo<FlightIcon[]>(
    () =>
      points
        .filter((point) => point.isFlight)
        .slice(0, 120)
        .map((point) => ({
          id: point.id,
          event: point.event,
          position: point.position,
          angle: flightTrack(point.event),
          risk: point.event.risk,
        })),
    [points]
  )

  const vesselIcons = useMemo<VesselIcon[]>(
    () =>
      points
        .filter((point) => point.isVessel)
        .slice(0, 220)
        .map((point) => ({
          id: point.id,
          event: point.event,
          position: point.position,
          angle: vesselTrack(point.event),
          risk: point.event.risk,
        })),
    [points]
  )

  const layers = useMemo(
    () => [
      new GeoJsonLayer({
        id: "country-boundaries",
        data: COUNTRIES_GEOJSON_URL,
        visible: layerVisibility.boundaries,
        stroked: true,
        filled: false,
        pickable: false,
        lineWidthMinPixels: 0.6,
        getLineColor: [82, 164, 255, 80],
      }),
      new IconLayer<FlightIcon>({
        id: "flight-icons",
        data: flightIcons,
        visible: layerVisibility.flights,
        pickable: true,
        sizeUnits: "pixels",
        sizeScale: 1,
        sizeMinPixels: 18,
        sizeMaxPixels: 44,
        getPosition: (d) => d.position,
        getIcon: () => ({
          url: PLANE_ICON_URL,
          width: 120,
          height: 120,
          anchorX: 60,
          anchorY: 60,
          mask: false,
        }),
        getSize: (d) => 30 + d.risk * 20,
        getAngle: (d) => d.angle,
        getColor: [94, 234, 212, 255],
        parameters: { depthTest: false } as any,
        billboard: false,
        onClick: (info) => {
          if (info.object) {
            setSelectedEvent(info.object.event)
          }
        },
      }),
      new IconLayer<VesselIcon>({
        id: "vessel-icons",
        data: vesselIcons,
        visible: layerVisibility.vessels,
        pickable: true,
        sizeUnits: "pixels",
        sizeScale: 1,
        sizeMinPixels: 14,
        sizeMaxPixels: 34,
        getPosition: (d) => d.position,
        getIcon: () => ({
          url: SHIP_ICON_URL,
          width: 128,
          height: 128,
          anchorX: 64,
          anchorY: 64,
          mask: false,
        }),
        getSize: (d) => 22 + d.risk * 14,
        getAngle: (d) => d.angle,
        getColor: [45, 212, 191, 255],
        parameters: { depthTest: false } as any,
        billboard: false,
        onClick: (info) => {
          if (info.object) {
            setSelectedEvent(info.object.event)
          }
        },
      }),
      new ScatterplotLayer<EventPoint>({
        id: "conflict-halo",
        data: points.filter((point) => point.isConflict),
        visible: layerVisibility.conflicts,
        pickable: false,
        stroked: false,
        filled: true,
        radiusUnits: "meters",
        getPosition: (d) => d.position,
        getRadius: (d) => d.radius * 1.35,
        getFillColor: [239, 68, 68, 54],
      }),
      new ScatterplotLayer<EventPoint>({
        id: "otx-cyber-halo",
        data: points.filter((point) => point.isCyber),
        visible: layerVisibility.cyber,
        pickable: false,
        stroked: false,
        filled: true,
        radiusUnits: "meters",
        getPosition: (d) => d.position,
        getRadius: (d) => d.radius * 1.55,
        getFillColor: [56, 189, 248, 60],
      }),
      new ScatterplotLayer<EventPoint>({
        id: "otx-cyber-core",
        data: points.filter((point) => point.isCyber),
        visible: layerVisibility.cyber,
        pickable: true,
        stroked: true,
        filled: true,
        radiusUnits: "meters",
        lineWidthUnits: "pixels",
        getPosition: (d) => d.position,
        getRadius: (d) => d.radius * 0.62,
        getFillColor: [34, 211, 238, 220],
        getLineColor: [224, 242, 254, 230],
        getLineWidth: 1.5,
        onClick: (info) => {
          if (info.object) {
            setSelectedEvent(info.object.event)
          }
        },
      }),
      new ScatterplotLayer<EventPoint>({
        id: "all-events",
        data: points.filter((point) => !point.isCyber && !point.isFlight && !point.isVessel),
        visible: layerVisibility.events,
        pickable: true,
        stroked: true,
        filled: true,
        radiusUnits: "meters",
        lineWidthUnits: "pixels",
        getPosition: (d) => d.position,
        getRadius: (d) => d.radius,
        getFillColor: (d) => [...d.color, d.isFlight ? 220 : 170],
        getLineColor: [15, 23, 42, 230],
        getLineWidth: 1.1,
        onClick: (info) => {
          if (info.object) {
            setSelectedEvent(info.object.event)
          }
        },
      }),
    ],
    [flightIcons, layerVisibility.boundaries, layerVisibility.conflicts, layerVisibility.cyber, layerVisibility.events, layerVisibility.flights, layerVisibility.vessels, points, setSelectedEvent, vesselIcons]
  )

  const flightCount = points.filter((point) => point.isFlight).length
  const vesselCount = points.filter((point) => point.isVessel).length
  const conflictCount = points.filter((point) => point.isConflict).length
  const cyberCount = points.filter((point) => point.isCyber).length
  const cyberTotal = events.filter((event) => (event.source || "").toLowerCase() === "otx").length

  return (
    <div className="map-canvas-shell">
      <MapLibreMap
        mapStyle={MAP_STYLE}
        attributionControl={false}
        style={{ width: "100%", height: "100%" }}
        initialViewState={{
          longitude: 6,
          latitude: 26,
          zoom: 1.45,
        }}
        maxPitch={45}
        renderWorldCopies={false}
        minZoom={1}
        maxZoom={8}
        onLoad={(event) => emitBounds(event.target)}
        onMoveEnd={(event) => emitBounds(event.target)}
      >
        <DeckGLOverlay layers={layers} />
        <NavigationControl position="bottom-right" visualizePitch />
        <ScaleControl position="bottom-left" />
      </MapLibreMap>

      <div className="map-hud">
        <div>
          <span>Tracked Flights</span>
          <strong>{flightCount}</strong>
        </div>
        <div>
          <span>Conflict Signals</span>
          <strong>{conflictCount}</strong>
        </div>
        <div>
          <span>Vessel Tracks</span>
          <strong>{vesselCount}</strong>
        </div>
        <div>
          <span>Cyber Threats (OTX)</span>
          <strong>{cyberCount}</strong>
        </div>
        <div>
          <span>Geolocated Events</span>
          <strong>{points.length}</strong>
        </div>
        <div>
          <span>OTX Total in Feed</span>
          <strong>{cyberTotal}</strong>
        </div>
      </div>
    </div>
  )
}
