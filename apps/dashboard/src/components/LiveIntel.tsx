import { useMemo, useState } from "react"
import { CAMERA_FEEDS } from "../config/liveFeeds"
import type { Event } from "../types"

interface LiveIntelProps {
  events: Event[]
  selectedEvent: Event | null
  onSelectEvent: (event: Event) => void
}

function toTimestamp(value?: string) {
  if (!value) return 0
  const normalized = /[zZ]$|[+\-]\d{2}:\d{2}$/.test(value) ? value : `${value}Z`
  const ts = new Date(normalized).getTime()
  return Number.isFinite(ts) ? ts : 0
}

function haversineKm(aLat: number, aLon: number, bLat: number, bLon: number) {
  const toRad = (d: number) => (d * Math.PI) / 180
  const dLat = toRad(bLat - aLat)
  const dLon = toRad(bLon - aLon)
  const aa =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(aLat)) * Math.cos(toRad(bLat)) * Math.sin(dLon / 2) ** 2
  const c = 2 * Math.atan2(Math.sqrt(aa), Math.sqrt(1 - aa))
  return 6371 * c
}

function sourceLabel(source?: string) {
  if (!source) return "Unknown"
  const s = source.toLowerCase()
  if (s === "opensky") return "OpenSky"
  if (s === "otx") return "OTX"
  if (s === "gdelt") return "GDELT"
  return source
}

export default function LiveIntel({ events, selectedEvent, onSelectEvent }: LiveIntelProps) {
  const [activeCameraId, setActiveCameraId] = useState<string | null>(null)

  const focal = useMemo(() => {
    const lat = selectedEvent?.lat ?? selectedEvent?.latitude
    const lon = selectedEvent?.lon ?? selectedEvent?.longitude
    if (typeof lat === "number" && Number.isFinite(lat) && typeof lon === "number" && Number.isFinite(lon)) {
      return { lat, lon }
    }

    const fallback = events.find((event) => {
      const eLat = event.lat ?? event.latitude
      const eLon = event.lon ?? event.longitude
      return typeof eLat === "number" && Number.isFinite(eLat) && typeof eLon === "number" && Number.isFinite(eLon)
    })
    if (!fallback) return null
    return { lat: fallback.lat ?? fallback.latitude ?? 0, lon: fallback.lon ?? fallback.longitude ?? 0 }
  }, [events, selectedEvent])

  const nearestCameras = useMemo(() => {
    const cameras = CAMERA_FEEDS.map((feed) => {
      const distanceKm = focal ? haversineKm(focal.lat, focal.lon, feed.lat, feed.lon) : null
      return { ...feed, distanceKm }
    })
    if (!focal) return cameras.slice(0, 3)
    return cameras.sort((a, b) => (a.distanceKm ?? 0) - (b.distanceKm ?? 0)).slice(0, 3)
  }, [focal])

  const activeCamera = useMemo(() => {
    const preferred = nearestCameras.find((feed) => feed.id === activeCameraId)
    if (preferred) return preferred
    return nearestCameras[0] ?? null
  }, [activeCameraId, nearestCameras])

  const mutedEmbedUrl = useMemo(() => {
    if (!activeCamera?.embedUrl) return null
    try {
      const url = new URL(activeCamera.embedUrl)
      const host = url.hostname.toLowerCase()
      if (host.includes("youtube.com") || host.includes("youtu.be")) {
        url.searchParams.set("mute", "1")
        url.searchParams.set("autoplay", "1")
        url.searchParams.set("playsinline", "1")
      }
      return url.toString()
    } catch {
      return activeCamera.embedUrl
    }
  }, [activeCamera])

  const liveNews = useMemo(
    () =>
      events
        .filter((event) => (event.source || "").toLowerCase() !== "opensky")
        .sort((a, b) => toTimestamp(b.time || b.created_at || b.timestamp) - toTimestamp(a.time || a.created_at || a.timestamp))
        .slice(0, 10),
    [events]
  )

  return (
    <section className="intel-panel live-intel-panel">
      <header>
        <span>Live Intel Feeds</span>
        <small>{liveNews.length} headlines</small>
      </header>

      <div className="camera-stream">
        <div className="camera-stream-head">
          <strong>Camera Feed</strong>
          {activeCamera ? <span>{activeCamera.title}</span> : <span>No feed selected</span>}
        </div>
        {activeCamera ? (
          <>
            {activeCamera.embedUrl ? (
              <iframe
                title={`camera-${activeCamera.id}`}
                src={mutedEmbedUrl ?? activeCamera.embedUrl}
                loading="lazy"
                allow="accelerometer; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
                referrerPolicy="strict-origin-when-cross-origin"
              />
            ) : (
              <div className="camera-fallback">
                <p>Provider blocks iframe embedding. Open externally.</p>
              </div>
            )}
            <div className="camera-meta">
              <span>{activeCamera.provider}</span>
              {typeof activeCamera.distanceKm === "number" && <span>{activeCamera.distanceKm.toFixed(0)} km away</span>}
              <a href={activeCamera.externalUrl} target="_blank" rel="noreferrer">
                Open Feed
              </a>
            </div>
          </>
        ) : (
          <div className="camera-fallback">
            <p>No camera feed candidates yet.</p>
          </div>
        )}
      </div>

      <div className="camera-list">
        {nearestCameras.map((camera) => (
          <button
            key={camera.id}
            type="button"
            className={activeCamera?.id === camera.id ? "is-selected" : ""}
            onClick={() => setActiveCameraId(camera.id)}
          >
            <strong>{camera.title}</strong>
            <span>{camera.provider}</span>
          </button>
        ))}
      </div>

      <div className="news-stream">
        <div className="camera-stream-head">
          <strong>Live News</strong>
          <span>GDELT + OTX stream</span>
        </div>
        <ul>
          {liveNews.map((event) => (
            <li key={event.id}>
              <button type="button" onClick={() => onSelectEvent(event)}>
                <strong>{event.title}</strong>
                <div>
                  <span>{sourceLabel(event.source)}</span>
                  <span>Risk {event.risk.toFixed(2)}</span>
                </div>
              </button>
            </li>
          ))}
        </ul>
      </div>
    </section>
  )
}
