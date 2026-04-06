/**
 * LeadsMap — MapLibre GL JS parcel visualization.
 *
 * Sources:
 *   'leads'          — full FeatureCollection from API (fill + outline layers)
 *   'selected-lead'  — single selected feature (highlight outline)
 *
 * Layers (bottom → top):
 *   leads-fill       — color-coded by confidence_score
 *   leads-outline    — thin gray border on all parcels
 *   selected-outline — white, thick border on selected parcel
 *
 * Interactions:
 *   hover  → pointer cursor + small popup (identyfikator + confidence)
 *   click  → sets selectedLeadId in Zustand → sidebar shows detail
 *
 * Geometry is in EPSG:4326 (WGS84) — ST_Transform happened at the API boundary.
 * This component never touches coordinates.
 */

import { useCallback, useMemo, useRef, useState } from 'react';
import Map, {
  Source,
  Layer,
  Popup,
  NavigationControl,
  ScaleControl,
} from 'react-map-gl/maplibre';
import type {
  MapRef,
  MapLayerMouseEvent,
  LayerSpecification,
} from 'react-map-gl/maplibre';
import type { FeatureCollection, Geometry } from 'geojson';
import 'maplibre-gl/dist/maplibre-gl.css';

import type { LeadProperties, LeadsFeatureCollection } from '../../types/api';
import { useMapStore } from '../../store/mapStore';
import { ConfidenceBadge } from '../ui/ConfidenceBadge';

// ---------------------------------------------------------------------------
// Free MapLibre demo style — no API key required.
// For production: swap for MapTiler / Stadia Maps style with API key.
// ---------------------------------------------------------------------------
const MAP_STYLE = 'https://demotiles.maplibre.org/style.json';

// Poland center
const INITIAL_VIEW_STATE = {
  longitude: 19.48,
  latitude: 52.07,
  zoom: 5.5,
};

// ---------------------------------------------------------------------------
// Layer specifications
// ---------------------------------------------------------------------------

const LEADS_FILL_LAYER: LayerSpecification = {
  id: 'leads-fill',
  type: 'fill',
  source: 'leads',
  paint: {
    'fill-color': [
      'interpolate', ['linear'],
      ['get', 'confidence_score'],
      0.70, '#fbbf24',   // amber-400   — moderate
      0.80, '#f97316',   // orange-500  — high
      0.90, '#ef4444',   // red-500     — prime
      1.00, '#dc2626',   // red-600     — maximum
    ],
    'fill-opacity': [
      'interpolate', ['linear'],
      ['get', 'confidence_score'],
      0.70, 0.35,
      1.00, 0.60,
    ],
  },
};

const LEADS_OUTLINE_LAYER: LayerSpecification = {
  id: 'leads-outline',
  type: 'line',
  source: 'leads',
  paint: {
    'line-color': '#4b5563',   // gray-600
    'line-width': 1,
    'line-opacity': 0.8,
  },
};

const SELECTED_OUTLINE_LAYER: LayerSpecification = {
  id: 'selected-outline',
  type: 'line',
  source: 'selected-lead',
  paint: {
    'line-color': '#ffffff',
    'line-width': 2.5,
    'line-opacity': 1,
  },
};

// ---------------------------------------------------------------------------
// Popup state
// ---------------------------------------------------------------------------

interface HoverInfo {
  longitude: number;
  latitude: number;
  properties: LeadProperties;
}

// ---------------------------------------------------------------------------
// Type guard — MapLibre properties come back as Record<string, unknown>
// ---------------------------------------------------------------------------

function isLeadProperties(
  props: Record<string, unknown> | null | undefined,
): props is LeadProperties {
  return (
    props !== null &&
    props !== undefined &&
    typeof props['lead_id'] === 'string' &&
    typeof props['confidence_score'] === 'number'
  );
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

interface LeadsMapProps {
  data: LeadsFeatureCollection | undefined;
}

export function LeadsMap({ data }: LeadsMapProps) {
  const mapRef = useRef<MapRef>(null);
  const [hoverInfo, setHoverInfo] = useState<HoverInfo | null>(null);
  const [cursor, setCursor] = useState<string>('grab');

  const selectedLeadId = useMapStore((s) => s.selectedLeadId);
  const setSelectedLeadId = useMapStore((s) => s.setSelectedLeadId);

  // Selected feature as a standalone GeoJSON source for highlight layer
  const selectedFeatureCollection = useMemo<FeatureCollection<Geometry, LeadProperties> | null>(() => {
    if (!selectedLeadId || !data) return null;
    const found = data.features.find(
      (f) => f.properties.lead_id === selectedLeadId,
    );
    if (!found) return null;
    return { type: 'FeatureCollection', features: [found] };
  }, [selectedLeadId, data]);

  // ---------------------------------------------------------------------------
  // Event handlers
  // ---------------------------------------------------------------------------

  const handleClick = useCallback(
    (event: MapLayerMouseEvent) => {
      const feature = event.features?.[0];
      if (!feature) {
        setSelectedLeadId(null);
        return;
      }

      // MapLibre serialises all properties to JSON primitives on queryRenderedFeatures
      const rawProps = feature.properties as Record<string, unknown> | null;

      // evidence_chain arrives as a JSON string — parse it back
      if (rawProps && typeof rawProps['evidence_chain'] === 'string') {
        try {
          rawProps['evidence_chain'] = JSON.parse(rawProps['evidence_chain']) as unknown;
        } catch {
          rawProps['evidence_chain'] = [];
        }
      }

      if (isLeadProperties(rawProps)) {
        setSelectedLeadId(rawProps.lead_id);
      }
    },
    [setSelectedLeadId],
  );

  const handleMouseMove = useCallback((event: MapLayerMouseEvent) => {
    const feature = event.features?.[0];
    if (!feature) {
      setHoverInfo(null);
      setCursor('grab');
      return;
    }

    const rawProps = feature.properties as Record<string, unknown> | null;
    if (isLeadProperties(rawProps)) {
      setCursor('pointer');
      setHoverInfo({
        longitude: event.lngLat.lng,
        latitude: event.lngLat.lat,
        properties: rawProps,
      });
    }
  }, []);

  const handleMouseLeave = useCallback(() => {
    setHoverInfo(null);
    setCursor('grab');
  }, []);

  // ---------------------------------------------------------------------------
  // Fly to selected lead when it changes
  // ---------------------------------------------------------------------------
  const prevSelectedRef = useRef<string | null>(null);

  if (selectedLeadId !== prevSelectedRef.current) {
    prevSelectedRef.current = selectedLeadId;
    if (selectedLeadId && data && mapRef.current) {
      const found = data.features.find(
        (f) => f.properties.lead_id === selectedLeadId,
      );
      if (found?.geometry.type === 'MultiPolygon') {
        // Fly to centroid of first polygon ring
        const coords = found.geometry.coordinates[0]?.[0];
        if (coords && coords.length > 0) {
          const lngs = coords.map((c) => c[0] ?? 0);
          const lats = coords.map((c) => c[1] ?? 0);
          const centerLng = (Math.min(...lngs) + Math.max(...lngs)) / 2;
          const centerLat = (Math.min(...lats) + Math.max(...lats)) / 2;
          mapRef.current.flyTo({
            center: [centerLng, centerLat],
            zoom: 14,
            duration: 800,
          });
        }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Render
  // ---------------------------------------------------------------------------

  return (
    <Map
      ref={mapRef}
      mapStyle={MAP_STYLE}
      initialViewState={INITIAL_VIEW_STATE}
      cursor={cursor}
      interactiveLayerIds={['leads-fill']}
      onClick={handleClick}
      onMouseMove={handleMouseMove}
      onMouseLeave={handleMouseLeave}
      style={{ width: '100%', height: '100%' }}
      attributionControl={{ compact: true }}
    >
      {/* Navigation controls */}
      <NavigationControl position="top-right" showCompass={false} />
      <ScaleControl position="bottom-right" unit="metric" />

      {/* All leads — fill + thin outline */}
      {data && (
        <Source
          id="leads"
          type="geojson"
          data={data as FeatureCollection}
          generateId={false}
        >
          <Layer {...LEADS_FILL_LAYER} />
          <Layer {...LEADS_OUTLINE_LAYER} />
        </Source>
      )}

      {/* Selected lead — thick white outline */}
      {selectedFeatureCollection && (
        <Source
          id="selected-lead"
          type="geojson"
          data={selectedFeatureCollection as FeatureCollection}
        >
          <Layer {...SELECTED_OUTLINE_LAYER} />
        </Source>
      )}

      {/* Hover popup */}
      {hoverInfo && (
        <Popup
          longitude={hoverInfo.longitude}
          latitude={hoverInfo.latitude}
          offset={12}
          closeButton={false}
          closeOnClick={false}
          anchor="bottom"
          className="!p-0 !bg-transparent !border-0 !shadow-none"
          style={{ zIndex: 10 }}
        >
          <div className="rounded-lg border border-gray-700 bg-gray-900/95 px-3 py-2 backdrop-blur-sm shadow-xl min-w-[180px]">
            <p className="font-mono text-[11px] text-gray-300 truncate max-w-[200px]">
              {hoverInfo.properties.identyfikator}
            </p>
            <div className="mt-1.5 flex items-center justify-between gap-2">
              <span className="text-[10px] text-gray-500">
                {hoverInfo.properties.dominant_przeznaczenie ?? '—'}
              </span>
              <ConfidenceBadge score={hoverInfo.properties.confidence_score} variant="badge" />
            </div>
            {hoverInfo.properties.max_coverage_pct != null && (
              <p className="mt-1 text-[10px] text-amber-400 font-mono">
                pokrycie {hoverInfo.properties.max_coverage_pct.toFixed(0)}%
              </p>
            )}
            <p className="mt-1.5 text-[9px] text-gray-600 italic">Kliknij, aby zobaczyć szczegóły</p>
          </div>
        </Popup>
      )}
    </Map>
  );
}
