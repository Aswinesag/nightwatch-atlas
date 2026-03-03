import { useMemo, useState } from "react"
import { CAMERA_FEEDS } from "../config/liveFeeds"
import type { Event } from "../types"

interface LiveWebcamsProps {
  events: Event[]
  selectedEvent: Event | null
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

export default function LiveWebcams({ events, selectedEvent }: LiveWebcamsProps) {
  const [activeCameraId, setActiveCameraId] = useState<string | null>(null)
  const [blockedCameraIds, setBlockedCameraIds] = useState<Record<string, true>>({})

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
    const cameras = CAMERA_FEEDS.map((feed) => ({
      ...feed,
      distanceKm: focal ? haversineKm(focal.lat, focal.lon, feed.lat, feed.lon) : null,
    }))
    if (!focal) return cameras.slice(0, 6)
    return cameras.sort((a, b) => (a.distanceKm ?? 0) - (b.distanceKm ?? 0)).slice(0, 6)
  }, [focal])

  const embeddableCameras = useMemo(
    () => nearestCameras.filter((camera) => Boolean(camera.embedUrl) && !blockedCameraIds[camera.id]),
    [blockedCameraIds, nearestCameras]
  )

  const activeCamera = useMemo(() => {
    const preferred = nearestCameras.find((item) => item.id === activeCameraId)
    if (preferred && (preferred.embedUrl ? !blockedCameraIds[preferred.id] : true)) return preferred
    return embeddableCameras[0] ?? nearestCameras[0] ?? null
  }, [activeCameraId, blockedCameraIds, embeddableCameras, nearestCameras])

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

  const handleEmbedError = () => {
    if (!activeCamera?.embedUrl) return
    setBlockedCameraIds((previous) => ({ ...previous, [activeCamera.id]: true }))
  }

  return (
    <section className="intel-panel wm-webcams-panel">
      <header>
        <span>Live Webcams</span>
        <small>{nearestCameras.length} sources</small>
      </header>

      <div className="wm-webcam-player">
        {activeCamera?.embedUrl ? (
          <iframe
            title={`wm-cam-${activeCamera.id}`}
            key={`${activeCamera.id}-${mutedEmbedUrl ?? activeCamera.embedUrl}`}
            src={mutedEmbedUrl ?? activeCamera.embedUrl}
            loading="lazy"
            allow="accelerometer; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
            referrerPolicy="strict-origin-when-cross-origin"
            onError={handleEmbedError}
          />
        ) : (
          <div className="camera-fallback">
            <p>Selected source blocks iframe playback. Use Open Feed for external live view.</p>
            {activeCamera?.externalUrl && (
              <a href={activeCamera.externalUrl} target="_blank" rel="noreferrer">
                Open Feed
              </a>
            )}
          </div>
        )}
      </div>

      <div className="wm-webcam-grid">
        {nearestCameras.map((camera) => (
          <button
            type="button"
            key={camera.id}
            className={activeCamera?.id === camera.id ? "is-selected" : ""}
            onClick={() => {
              if (blockedCameraIds[camera.id]) {
                setBlockedCameraIds((previous) => {
                  const next = { ...previous }
                  delete next[camera.id]
                  return next
                })
              }
              setActiveCameraId(camera.id)
            }}
          >
            <strong>{camera.title}</strong>
            <span>{camera.provider}{camera.embedUrl ? " | embed" : " | external"}</span>
          </button>
        ))}
      </div>
    </section>
  )
}
