export interface CameraFeed {
  id: string
  title: string
  provider: string
  lat: number
  lon: number
  embedUrl?: string
  externalUrl: string
  tags: string[]
}

export const CAMERA_FEEDS: CameraFeed[] = [
  {
    id: "times-square-nyc",
    title: "Times Square, NYC",
    provider: "EarthCam",
    lat: 40.758,
    lon: -73.9855,
    embedUrl: "https://www.youtube.com/embed/rnXIjl_Rzy4",
    externalUrl: "https://www.youtube.com/watch?v=rnXIjl_Rzy4",
    tags: ["urban", "usa", "traffic"],
  },
  {
    id: "shibuya-tokyo",
    title: "Shibuya Crossing, Tokyo",
    provider: "YouTube",
    lat: 35.6595,
    lon: 139.7005,
    embedUrl: "https://www.youtube.com/embed/DVHoPNiNxNo",
    externalUrl: "https://www.youtube.com/watch?v=DVHoPNiNxNo",
    tags: ["urban", "asia", "traffic"],
  },
  {
    id: "london-city",
    title: "London City View",
    provider: "SkylineWebcams",
    lat: 51.5074,
    lon: -0.1278,
    embedUrl: "https://www.youtube.com/embed/7pcL-0Wo77U",
    externalUrl: "https://www.youtube.com/watch?v=7pcL-0Wo77U",
    tags: ["urban", "europe"],
  },
  {
    id: "sydney-harbour",
    title: "Sydney Harbour",
    provider: "WebcamSydney",
    lat: -33.8568,
    lon: 151.2153,
    embedUrl: "https://www.youtube.com/embed/5uZa3-RMFos",
    externalUrl: "https://www.youtube.com/watch?v=5uZa3-RMFos",
    tags: ["port", "oceania"],
  },
  {
    id: "dubai-marina",
    title: "Dubai Marina",
    provider: "Fairmont Palm",
    lat: 25.0853,
    lon: 55.146,
    embedUrl: "https://www.youtube.com/embed/7dE4IjDQJmE",
    externalUrl: "https://www.youtube.com/watch?v=7dE4IjDQJmE",
    tags: ["urban", "middle-east"],
  },
]
