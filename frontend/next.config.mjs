import { dirname } from "node:path"
import { fileURLToPath } from "node:url"

const API_BASE_URL =
  process.env.API_BASE_URL ||
  process.env.NEXT_PUBLIC_API_BASE_URL ||
  "http://text-sql:8000"
const VIS_API_BASE_URL =
  process.env.VIS_API_BASE_URL ||
  process.env.NEXT_PUBLIC_VIS_API_BASE_URL ||
  "http://plot-chart:8080"
const projectRoot = dirname(fileURLToPath(import.meta.url))

/** @type {import('next').NextConfig} */
const nextConfig = {
  turbopack: {
    root: projectRoot,
  },
  output: "standalone",
  typescript: {
    ignoreBuildErrors: true,
  },
  images: {
    unoptimized: true,
  },
  async rewrites() {
    return [
      { source: "/query/:path*", destination: `${API_BASE_URL}/query/:path*` },
      { source: "/admin/:path*", destination: `${API_BASE_URL}/admin/:path*` },
      { source: "/report/:path*", destination: `${API_BASE_URL}/report/:path*` },
      { source: "/audit/:path*", destination: `${API_BASE_URL}/audit/:path*` },
      { source: "/chat/:path*", destination: `${API_BASE_URL}/chat/:path*` },
      { source: "/dashboard/:path*", destination: `${API_BASE_URL}/dashboard/:path*` },
      { source: "/cohort/:path*", destination: `${API_BASE_URL}/cohort/:path*` },
      { source: "/pdf/:path*", destination: `${API_BASE_URL}/pdf/:path*` },
      { source: "/visualize", destination: `${VIS_API_BASE_URL}/visualize` },
    ]
  },
}

export default nextConfig
