# TerraZoning — Kokpit Inwestorski

React + TypeScript investment dashboard consuming the TerraZoning GeoJSON API.

## Stack

| Layer | Library |
|---|---|
| Bundler | Vite 6 |
| Framework | React 19 + TypeScript strict |
| Map | MapLibre GL JS 5 via react-map-gl 8 |
| Server state | TanStack Query v5 |
| Client state | Zustand 5 |
| Styling | Tailwind CSS 3 (dark mode) |
| Icons | lucide-react |
| HTTP | axios |

## Prerequisites

- Node.js 20+
- Backend running at `http://localhost:8000` (TerraZoning FastAPI)
- PostGIS populated with at least one investment lead

## Setup & run

```bash
cd frontend/

# Install dependencies
npm install

# Start dev server (proxies /api/* → localhost:8000)
npm run dev
```

Open **http://localhost:5173** in your browser.

## API proxy

Vite proxies all `/api/*` requests to `http://localhost:8000` to avoid CORS in development.
No environment variables needed for local development.

For production, configure your reverse proxy (nginx / Cloud Run / Vercel) to route:
- `/api/*` → FastAPI backend
- `/*` → built frontend (dist/)

## Map tile style

The default map style uses **MapLibre demo tiles** (`demotiles.maplibre.org`) — free, no API key.

For better basemap quality in production, set a MapTiler or Stadia Maps style URL in
`src/components/map/LeadsMap.tsx`:

```ts
const MAP_STYLE = 'https://api.maptiler.com/maps/dataviz-dark/style.json?key=YOUR_KEY';
```

## Features

- **Map panel** — parcel polygons colored by `confidence_score`:
  - Amber (70–80%) | Orange (80–90%) | Red (90%+)
  - Hover → tooltip with identyfikator + zone designation
  - Click → flies to parcel, detail panel opens in sidebar
- **Sidebar — list view** — top leads sorted by score descending, with mini coverage bar
- **Sidebar — detail view** — full breakdown: area, MPZP coverage %, land-use designation, evidence chain
- **Evidence chain** — step-by-step provenance: source → parcel → spatial delta
- **Filter bar** — min_score slider (real-time refetch), refresh button

## Build for production

```bash
npm run build
# Output in dist/ — serve as static files
```
