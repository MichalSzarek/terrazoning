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

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
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
import type { GeoJSONSource } from 'maplibre-gl';
import type { Feature, FeatureCollection, Geometry, Point, Position } from 'geojson';
import 'maplibre-gl/dist/maplibre-gl.css';

import type {
  LeadProperties,
  LeadsFeatureCollection,
  QuarantineParcelFeatureCollection,
  QuarantineParcelProperties,
} from '../../types/api';
import { useMapStore } from '../../store/mapStore';
import { ConfidenceBadge } from '../ui/ConfidenceBadge';

// ---------------------------------------------------------------------------
// Open basemap strategy:
// - default to a production-grade free style
// - allow override via Vite env for premium/self-hosted styles
// ---------------------------------------------------------------------------
const DEFAULT_MAP_STYLE = 'https://tiles.openfreemap.org/styles/liberty';
const MAP_STYLE = import.meta.env.VITE_MAP_STYLE_URL?.trim() || DEFAULT_MAP_STYLE;

const INITIAL_VIEW_STATE = {
  longitude: 19.48,
  latitude: 52.07,
  zoom: 6,
};

// ---------------------------------------------------------------------------
// Layer specifications
// ---------------------------------------------------------------------------

const POLYGON_MIN_ZOOM = 11;
const CLUSTER_MAX_ZOOM = 9;
const MARKER_MAX_ZOOM = 22;
const AUTO_FIT_MAX_ZOOM = 11.75;
const DETAIL_FLY_TO_ZOOM = 14;
const SINGLE_FEATURE_ZOOM = 13.5;
const FIT_BOUNDS_PADDING = {
  top: 48,
  right: 48,
  bottom: 72,
  left: 48,
};

const LEADS_FILL_LAYER: LayerSpecification = {
  id: 'leads-fill',
  type: 'fill',
  source: 'leads',
  minzoom: POLYGON_MIN_ZOOM,
  paint: {
    'fill-color': [
      'case',
      ['==', ['get', 'strategy_type'], 'future_buildable'],
      [
        'case',
        ['==', ['get', 'confidence_band'], 'formal'], '#38bdf8',
        ['==', ['get', 'confidence_band'], 'supported'], '#06b6d4',
        '#a78bfa',
      ],
      [
        'interpolate', ['linear'],
        ['get', 'confidence_score'],
        0.70, '#fbbf24',
        0.80, '#f97316',
        0.90, '#ef4444',
        1.00, '#dc2626',
      ],
    ],
    'fill-opacity': [
      'interpolate', ['linear'],
      ['zoom'],
      5, 0.06,
      8, 0.12,
      11, 0.22,
      14, 0.48,
    ],
  },
};

const LEADS_OUTLINE_LAYER: LayerSpecification = {
  id: 'leads-outline',
  type: 'line',
  source: 'leads',
  minzoom: POLYGON_MIN_ZOOM,
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
  minzoom: POLYGON_MIN_ZOOM,
  paint: {
    'line-color': '#ffffff',
    'line-width': 2.5,
    'line-opacity': 1,
  },
};

const HOVERED_OUTLINE_LAYER: LayerSpecification = {
  id: 'hovered-outline',
  type: 'line',
  source: 'hovered-lead',
  minzoom: POLYGON_MIN_ZOOM,
  paint: {
    'line-color': '#fde68a',
    'line-width': 2,
    'line-opacity': 0.95,
  },
};

const SELECTED_QUARANTINE_OUTLINE_LAYER: LayerSpecification = {
  ...SELECTED_OUTLINE_LAYER,
  id: 'selected-quarantine-outline',
};

const QUARANTINE_FILL_LAYER: LayerSpecification = {
  id: 'quarantine-fill',
  type: 'fill',
  source: 'quarantine',
  minzoom: POLYGON_MIN_ZOOM,
  paint: {
    'fill-color': '#facc15',
    'fill-opacity': [
      'interpolate', ['linear'],
      ['zoom'],
      5, 0.05,
      8, 0.1,
      11, 0.18,
      14, 0.32,
    ],
  },
};

const QUARANTINE_OUTLINE_LAYER: LayerSpecification = {
  id: 'quarantine-outline',
  type: 'line',
  source: 'quarantine',
  minzoom: POLYGON_MIN_ZOOM,
  paint: {
    'line-color': '#facc15',
    'line-width': 1.5,
    'line-opacity': 0.95,
  },
};

const LEAD_CLUSTER_GLOW_LAYER: LayerSpecification = {
  id: 'lead-cluster-glow',
  type: 'circle',
  source: 'lead-centroids',
  filter: ['has', 'point_count'],
  maxzoom: POLYGON_MIN_ZOOM,
  paint: {
    'circle-color': '#f97316',
    'circle-radius': [
      'interpolate', ['linear'],
      ['get', 'point_count'],
      1, 26,
      5, 34,
      10, 42,
      20, 50,
    ],
    'circle-opacity': 0.22,
    'circle-blur': 0.8,
  },
};

const LEAD_CLUSTER_LAYER: LayerSpecification = {
  id: 'lead-cluster',
  type: 'circle',
  source: 'lead-centroids',
  filter: ['has', 'point_count'],
  maxzoom: POLYGON_MIN_ZOOM,
  paint: {
    'circle-color': '#111827',
    'circle-stroke-color': '#fb923c',
    'circle-stroke-width': 2,
    'circle-radius': [
      'interpolate', ['linear'],
      ['get', 'point_count'],
      1, 16,
      5, 20,
      10, 24,
      20, 28,
    ],
    'circle-opacity': 0.92,
  },
};

const LEAD_CLUSTER_COUNT_LAYER: LayerSpecification = {
  id: 'lead-cluster-count',
  type: 'symbol',
  source: 'lead-centroids',
  filter: ['has', 'point_count'],
  maxzoom: POLYGON_MIN_ZOOM,
  layout: {
    'text-field': ['get', 'point_count_abbreviated'],
    'text-size': 12,
    'text-font': ['Open Sans Bold'],
  },
  paint: {
    'text-color': '#f8fafc',
  },
};

const LEAD_MARKER_HIT_LAYER: LayerSpecification = {
  id: 'lead-marker-hit',
  type: 'circle',
  source: 'lead-centroids',
  filter: ['!', ['has', 'point_count']],
  maxzoom: MARKER_MAX_ZOOM,
  paint: {
    'circle-radius': [
      'interpolate', ['linear'],
      ['zoom'],
      5, 14,
      8, 18,
      12, 24,
      15, 26,
      18, 24,
    ],
    'circle-color': '#ffffff',
    'circle-opacity': 0.01,
  },
};

const LEAD_MARKER_GLOW_LAYER: LayerSpecification = {
  id: 'lead-marker-glow',
  type: 'circle',
  source: 'lead-centroids',
  filter: ['!', ['has', 'point_count']],
  maxzoom: MARKER_MAX_ZOOM,
  paint: {
    'circle-color': [
      'case',
      ['==', ['get', 'strategy_type'], 'future_buildable'],
      [
        'case',
        ['==', ['get', 'confidence_band'], 'formal'], '#38bdf8',
        ['==', ['get', 'confidence_band'], 'supported'], '#06b6d4',
        '#a78bfa',
      ],
      [
        'interpolate', ['linear'],
        ['get', 'confidence_score'],
        0.70, '#fbbf24',
        0.80, '#f97316',
        0.90, '#ef4444',
        1.00, '#dc2626',
      ],
    ],
    'circle-radius': [
      'interpolate', ['linear'],
      ['zoom'],
      5, 18,
      8, 20,
      12, 18,
      15, 18,
      18, 16,
    ],
    'circle-opacity': [
      'interpolate', ['linear'],
      ['zoom'],
      5, 0.3,
      9, 0.22,
      12, 0.22,
      15, 0.2,
      18, 0.18,
    ],
    'circle-blur': 0.95,
  },
};

const LEAD_MARKER_HALO_LAYER: LayerSpecification = {
  id: 'lead-marker-halo',
  type: 'circle',
  source: 'lead-centroids',
  filter: ['!', ['has', 'point_count']],
  maxzoom: MARKER_MAX_ZOOM,
  paint: {
    'circle-radius': [
      'interpolate', ['linear'],
      ['zoom'],
      5, 10,
      8, 12,
      12, 13,
      15, 14,
      18, 13,
    ],
    'circle-color': '#0f172a',
    'circle-opacity': [
      'interpolate', ['linear'],
      ['zoom'],
      5, 0.25,
      12, 0.22,
      15, 0.2,
      18, 0.18,
    ],
    'circle-blur': 0.4,
  },
};

const FUTURE_LEAD_MARKER_RING_LAYER: LayerSpecification = {
  id: 'future-lead-marker-ring',
  type: 'circle',
  source: 'lead-centroids',
  filter: [
    'all',
    ['!', ['has', 'point_count']],
    ['==', ['get', 'strategy_type'], 'future_buildable'],
  ],
  maxzoom: MARKER_MAX_ZOOM,
  paint: {
    'circle-color': [
      'case',
      ['==', ['get', 'confidence_band'], 'formal'], '#38bdf8',
      ['==', ['get', 'confidence_band'], 'supported'], '#06b6d4',
      '#a78bfa',
    ],
    'circle-radius': [
      'interpolate', ['linear'],
      ['zoom'],
      5, 12,
      8, 14,
      12, 13,
      15, 13,
      18, 12,
    ],
    'circle-opacity': [
      'interpolate', ['linear'],
      ['zoom'],
      5, 0.25,
      8, 0.24,
      12, 0.2,
      15, 0.18,
      18, 0.16,
    ],
    'circle-stroke-color': '#e0f2fe',
    'circle-stroke-width': [
      'interpolate', ['linear'],
      ['zoom'],
      5, 1.8,
      12, 1.6,
      15, 1.4,
      18, 1.2,
    ],
    'circle-stroke-opacity': 0.55,
  },
};

const LEAD_MARKER_LAYER: LayerSpecification = {
  id: 'lead-marker',
  type: 'circle',
  source: 'lead-centroids',
  filter: ['!', ['has', 'point_count']],
  maxzoom: MARKER_MAX_ZOOM,
  paint: {
    'circle-color': [
      'case',
      ['==', ['get', 'strategy_type'], 'future_buildable'],
      [
        'case',
        ['==', ['get', 'confidence_band'], 'formal'], '#38bdf8',
        ['==', ['get', 'confidence_band'], 'supported'], '#06b6d4',
        '#a78bfa',
      ],
      [
        'interpolate', ['linear'],
        ['get', 'confidence_score'],
        0.70, '#fbbf24',
        0.80, '#f97316',
        0.90, '#ef4444',
        1.00, '#dc2626',
      ],
    ],
    'circle-radius': [
      'case',
      ['==', ['get', 'strategy_type'], 'future_buildable'],
      [
        'interpolate', ['linear'],
        ['zoom'],
        5, 10,
        8, 12,
        12, 12,
        15, 13,
        18, 12,
      ],
      [
        'interpolate', ['linear'],
        ['zoom'],
        5, 9,
        8, 10.5,
        12, 11,
        15, 12,
        18, 11,
      ],
    ],
    'circle-stroke-color': [
      'case',
      ['==', ['get', 'strategy_type'], 'future_buildable'],
      '#e0f2fe',
      '#f8fafc',
    ],
    'circle-stroke-width': [
      'interpolate', ['linear'],
      ['zoom'],
      5, 2.4,
      12, 2.1,
      15, 1.9,
      18, 1.7,
    ],
    'circle-opacity': [
      'interpolate', ['linear'],
      ['zoom'],
      5, 0.98,
      12, 0.96,
      15, 0.96,
      18, 0.96,
    ],
  },
};

const SELECTED_MARKER_LAYER: LayerSpecification = {
  id: 'selected-marker',
  type: 'circle',
  source: 'selected-lead-point',
  paint: {
    'circle-radius': [
      'interpolate', ['linear'],
      ['zoom'],
      5, 11,
      12, 13,
      15, 14,
      18, 12,
    ],
    'circle-color': '#111827',
    'circle-stroke-color': '#ffffff',
    'circle-stroke-width': 2.5,
    'circle-opacity': 0.95,
  },
};

const SELECTED_MARKER_HALO_LAYER: LayerSpecification = {
  id: 'selected-marker-halo',
  type: 'circle',
  source: 'selected-lead-point',
  paint: {
    'circle-radius': [
      'interpolate', ['linear'],
      ['zoom'],
      5, 18,
      12, 20,
      15, 22,
      18, 20,
    ],
    'circle-color': '#f59e0b',
    'circle-opacity': [
      'interpolate', ['linear'],
      ['zoom'],
      5, 0.24,
      12, 0.22,
      15, 0.22,
      18, 0.20,
    ],
    'circle-blur': 0.65,
  },
};

const HOVERED_MARKER_LAYER: LayerSpecification = {
  id: 'hovered-marker',
  type: 'circle',
  source: 'hovered-lead-point',
  paint: {
    'circle-radius': [
      'interpolate', ['linear'],
      ['zoom'],
      5, 12.5,
      12, 14.5,
      15, 15,
      18, 13,
    ],
    'circle-color': '#111827',
    'circle-stroke-color': '#fde68a',
    'circle-stroke-width': 2,
    'circle-opacity': 0.95,
  },
};

const HOVERED_MARKER_HALO_LAYER: LayerSpecification = {
  id: 'hovered-marker-halo',
  type: 'circle',
  source: 'hovered-lead-point',
  paint: {
    'circle-radius': [
      'interpolate', ['linear'],
      ['zoom'],
      5, 15,
      12, 17,
      15, 19,
      18, 17,
    ],
    'circle-color': '#fde68a',
    'circle-opacity': [
      'interpolate', ['linear'],
      ['zoom'],
      5, 0.22,
      12, 0.18,
      15, 0.18,
      18, 0.16,
    ],
    'circle-blur': 0.7,
  },
};

const SELECTED_QUARANTINE_MARKER_LAYER: LayerSpecification = {
  ...SELECTED_MARKER_LAYER,
  id: 'selected-quarantine-marker',
};

const QUARANTINE_MARKER_HIT_LAYER: LayerSpecification = {
  id: 'quarantine-marker-hit',
  type: 'circle',
  source: 'quarantine-centroids',
  maxzoom: MARKER_MAX_ZOOM,
  paint: {
    'circle-radius': [
      'interpolate', ['linear'],
      ['zoom'],
      5, 14,
      8, 18,
      12, 22,
      15, 24,
      18, 22,
    ],
    'circle-color': '#ffffff',
    'circle-opacity': 0.01,
  },
};

const QUARANTINE_MARKER_LAYER: LayerSpecification = {
  id: 'quarantine-marker',
  type: 'circle',
  source: 'quarantine-centroids',
  maxzoom: MARKER_MAX_ZOOM,
  paint: {
    'circle-color': '#facc15',
    'circle-radius': [
      'interpolate', ['linear'],
      ['zoom'],
      5, 6.5,
      8, 8,
      12, 8.5,
      15, 7.5,
      18, 7,
    ],
    'circle-stroke-color': '#111827',
    'circle-stroke-width': 1.5,
    'circle-opacity': 0.95,
  },
};

// ---------------------------------------------------------------------------
// Popup state
// ---------------------------------------------------------------------------

interface HoverInfo {
  kind: 'lead' | 'quarantine';
  longitude: number;
  latitude: number;
  properties: LeadProperties | QuarantineParcelProperties;
}

function isClusterProperties(
  props: unknown,
): props is { cluster: boolean; cluster_id: number; point_count: number } {
  if (props === null || props === undefined || typeof props !== 'object') {
    return false;
  }
  const candidate = props as Record<string, unknown>;
  return (
    (candidate['cluster'] === true || candidate['cluster'] === 'true') &&
    typeof candidate['cluster_id'] === 'number' &&
    typeof candidate['point_count'] === 'number'
  );
}

// ---------------------------------------------------------------------------
// Type guard — MapLibre properties come back as Record<string, unknown>
// ---------------------------------------------------------------------------

function isLeadProperties(
  props: unknown,
): props is LeadProperties {
  if (props === null || props === undefined || typeof props !== 'object') {
    return false;
  }
  const candidate = props as Record<string, unknown>;
  return (
    typeof candidate['lead_id'] === 'string' &&
    typeof candidate['confidence_score'] === 'number'
  );
}

function isQuarantineProperties(
  props: unknown,
): props is QuarantineParcelProperties {
  if (props === null || props === undefined || typeof props !== 'object') {
    return false;
  }
  const candidate = props as Record<string, unknown>;
  return (
    typeof candidate['dzialka_id'] === 'string' &&
    typeof candidate['identyfikator'] === 'string'
  );
}

function getRingSignedArea(ring: Position[]): number {
  let area = 0;
  for (let index = 0; index < ring.length - 1; index += 1) {
    const [x1, y1] = ring[index] ?? [];
    const [x2, y2] = ring[index + 1] ?? [];
    if (x1 == null || y1 == null || x2 == null || y2 == null) continue;
    area += (x1 * y2) - (x2 * y1);
  }
  return area / 2;
}

function getPolygonRingCentroid(ring: Position[]): Position | null {
  let areaFactor = 0;
  let centroidX = 0;
  let centroidY = 0;

  for (let index = 0; index < ring.length - 1; index += 1) {
    const [x1, y1] = ring[index] ?? [];
    const [x2, y2] = ring[index + 1] ?? [];
    if (x1 == null || y1 == null || x2 == null || y2 == null) continue;
    const cross = (x1 * y2) - (x2 * y1);
    areaFactor += cross;
    centroidX += (x1 + x2) * cross;
    centroidY += (y1 + y2) * cross;
  }

  if (areaFactor === 0) {
    return null;
  }

  return [
    centroidX / (3 * areaFactor),
    centroidY / (3 * areaFactor),
  ];
}

function isPointInRing(point: Position, ring: Position[]): boolean {
  const [lng, lat] = point;
  if (lng == null || lat == null) return false;

  let inside = false;
  for (let current = 0, previous = ring.length - 1; current < ring.length; previous = current, current += 1) {
    const [lng1, lat1] = ring[current] ?? [];
    const [lng2, lat2] = ring[previous] ?? [];
    if (lng1 == null || lat1 == null || lng2 == null || lat2 == null) continue;

    const intersects = ((lat1 > lat) !== (lat2 > lat))
      && (lng < ((lng2 - lng1) * (lat - lat1)) / ((lat2 - lat1) || Number.EPSILON) + lng1);

    if (intersects) inside = !inside;
  }
  return inside;
}

function isPointInsidePolygon(point: Position, polygon: Position[][]): boolean {
  const outerRing = polygon[0];
  if (!outerRing || !isPointInRing(point, outerRing)) {
    return false;
  }

  for (let holeIndex = 1; holeIndex < polygon.length; holeIndex += 1) {
    const hole = polygon[holeIndex];
    if (hole && isPointInRing(point, hole)) {
      return false;
    }
  }

  return true;
}

function getBoundsFromPolygon(polygon: Position[][]): [[number, number], [number, number]] | null {
  let minLng = Number.POSITIVE_INFINITY;
  let minLat = Number.POSITIVE_INFINITY;
  let maxLng = Number.NEGATIVE_INFINITY;
  let maxLat = Number.NEGATIVE_INFINITY;

  for (const ring of polygon) {
    for (const [lng, lat] of ring) {
      if (lng == null || lat == null) continue;
      if (lng < minLng) minLng = lng;
      if (lat < minLat) minLat = lat;
      if (lng > maxLng) maxLng = lng;
      if (lat > maxLat) maxLat = lat;
    }
  }

  if (![minLng, minLat, maxLng, maxLat].every(Number.isFinite)) {
    return null;
  }

  return [[minLng, minLat], [maxLng, maxLat]];
}

function getPrimaryPolygon(geometry: Feature<Geometry>['geometry']): Position[][] | null {
  if (geometry.type === 'Polygon') {
    return geometry.coordinates;
  }

  if (geometry.type !== 'MultiPolygon') {
    return null;
  }

  let selectedPolygon: Position[][] | null = null;
  let selectedArea = Number.NEGATIVE_INFINITY;

  for (const polygon of geometry.coordinates) {
    const outerRing = polygon[0];
    if (!outerRing) continue;
    const area = Math.abs(getRingSignedArea(outerRing));
    if (area > selectedArea) {
      selectedPolygon = polygon;
      selectedArea = area;
    }
  }

  return selectedPolygon;
}

function findBestInteriorPoint(polygon: Position[][]): Position | null {
  const bounds = getBoundsFromPolygon(polygon);
  if (!bounds) return null;

  const [[minLng, minLat], [maxLng, maxLat]] = bounds;
  const outerRing = polygon[0];
  if (!outerRing) return null;

  const centroid = getPolygonRingCentroid(outerRing);
  if (centroid && isPointInsidePolygon(centroid, polygon)) {
    return centroid;
  }

  const boundsCenter: Position = [
    (minLng + maxLng) / 2,
    (minLat + maxLat) / 2,
  ];
  if (isPointInsidePolygon(boundsCenter, polygon)) {
    return boundsCenter;
  }

  let bestPoint: Position | null = null;
  let bestDistance = Number.POSITIVE_INFINITY;
  const steps = 6;

  for (let xIndex = 1; xIndex < steps; xIndex += 1) {
    for (let yIndex = 1; yIndex < steps; yIndex += 1) {
      const candidate: Position = [
        minLng + ((maxLng - minLng) * xIndex) / steps,
        minLat + ((maxLat - minLat) * yIndex) / steps,
      ];

      if (!isPointInsidePolygon(candidate, polygon)) {
        continue;
      }

      const [candidateLng, candidateLat] = candidate;
      const [centerLng, centerLat] = boundsCenter;
      if (
        candidateLng == null
        || candidateLat == null
        || centerLng == null
        || centerLat == null
      ) {
        continue;
      }

      const deltaLng = candidateLng - centerLng;
      const deltaLat = candidateLat - centerLat;
      const distance = (deltaLng * deltaLng) + (deltaLat * deltaLat);

      if (distance < bestDistance) {
        bestPoint = candidate;
        bestDistance = distance;
      }
    }
  }

  return bestPoint ?? outerRing[0] ?? null;
}

function createDisplayPointFeature<T extends LeadProperties | QuarantineParcelProperties>(
  feature: Feature<Geometry, T>,
): Feature<Point, T> | null {
  if ('display_point' in feature.properties && feature.properties.display_point?.type === 'Point') {
    return {
      type: 'Feature',
      geometry: feature.properties.display_point,
      properties: feature.properties,
    };
  }

  const polygon = getPrimaryPolygon(feature.geometry);
  if (!polygon) return null;

  const displayPoint = findBestInteriorPoint(polygon);
  if (!displayPoint) return null;

  return {
    type: 'Feature',
    geometry: {
      type: 'Point',
      coordinates: displayPoint,
    },
    properties: feature.properties,
  };
}

function extendBoundsWithGeometry(
  geometry: Geometry,
  bounds: { minLng: number; minLat: number; maxLng: number; maxLat: number },
): void {
  const visitPosition = ([lng, lat]: Position) => {
    if (lng == null || lat == null) return;
    if (lng < bounds.minLng) bounds.minLng = lng;
    if (lat < bounds.minLat) bounds.minLat = lat;
    if (lng > bounds.maxLng) bounds.maxLng = lng;
    if (lat > bounds.maxLat) bounds.maxLat = lat;
  };

  if (geometry.type === 'Polygon') {
    for (const ring of geometry.coordinates) {
      for (const position of ring) visitPosition(position);
    }
    return;
  }

  if (geometry.type === 'MultiPolygon') {
    for (const polygon of geometry.coordinates) {
      for (const ring of polygon) {
        for (const position of ring) visitPosition(position);
      }
    }
  }
}

function getFeatureCollectionBounds(
  data: LeadsFeatureCollection | undefined,
  quarantineData: QuarantineParcelFeatureCollection | undefined,
): [[number, number], [number, number]] | null {
  const bounds = {
    minLng: Number.POSITIVE_INFINITY,
    minLat: Number.POSITIVE_INFINITY,
    maxLng: Number.NEGATIVE_INFINITY,
    maxLat: Number.NEGATIVE_INFINITY,
  };

  for (const feature of data?.features ?? []) {
    extendBoundsWithGeometry(feature.geometry, bounds);
  }

  for (const feature of quarantineData?.features ?? []) {
    extendBoundsWithGeometry(feature.geometry, bounds);
  }

  if (![bounds.minLng, bounds.minLat, bounds.maxLng, bounds.maxLat].every(Number.isFinite)) {
    return null;
  }

  return [
    [bounds.minLng, bounds.minLat],
    [bounds.maxLng, bounds.maxLat],
  ];
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

interface LeadsMapProps {
  data: LeadsFeatureCollection | undefined;
  quarantineData?: QuarantineParcelFeatureCollection | undefined;
  focusResultsNonce?: number;
}

export function LeadsMap({ data, quarantineData, focusResultsNonce = 0 }: LeadsMapProps) {
  const mapRef = useRef<MapRef>(null);
  const autoFitSignatureRef = useRef<string | null>(null);
  const [hoverInfo, setHoverInfo] = useState<HoverInfo | null>(null);
  const [cursor, setCursor] = useState<string>('grab');
  const [mapLoaded, setMapLoaded] = useState(false);

  const selectedLeadId = useMapStore((s) => s.selectedLeadId);
  const setSelectedLeadId = useMapStore((s) => s.setSelectedLeadId);
  const hoveredLeadId = useMapStore((s) => s.hoveredLeadId);
  const setHoveredLeadId = useMapStore((s) => s.setHoveredLeadId);
  const selectedQuarantineId = useMapStore((s) => s.selectedQuarantineId);
  const setSelectedQuarantineId = useMapStore((s) => s.setSelectedQuarantineId);

  const leadCentroids = useMemo<FeatureCollection<Point, LeadProperties> | null>(() => {
    if (!data) return null;

    const features = data.features
      .map((feature) => createDisplayPointFeature(feature))
      .filter((feature): feature is Feature<Point, LeadProperties> => feature !== null);

    return {
      type: 'FeatureCollection',
      features,
    };
  }, [data]);

  const quarantineCentroids = useMemo<FeatureCollection<Point, QuarantineParcelProperties> | null>(() => {
    if (!quarantineData) return null;

    const features = quarantineData.features
      .map((feature) => createDisplayPointFeature(feature))
      .filter((feature): feature is Feature<Point, QuarantineParcelProperties> => feature !== null);

    return {
      type: 'FeatureCollection',
      features,
    };
  }, [quarantineData]);

  // Selected feature as a standalone GeoJSON source for highlight layer
  const selectedFeatureCollection = useMemo<FeatureCollection<Geometry, LeadProperties> | null>(() => {
    if (!selectedLeadId || !data) return null;
    const found = data.features.find(
      (f) => f.properties.lead_id === selectedLeadId,
    );
    if (!found) return null;
    return { type: 'FeatureCollection', features: [found] };
  }, [selectedLeadId, data]);

  const selectedQuarantineFeatureCollection = useMemo<FeatureCollection<Geometry, QuarantineParcelProperties> | null>(() => {
    if (!selectedQuarantineId || !quarantineData) return null;
    const found = quarantineData.features.find(
      (f) => f.properties.dzialka_id === selectedQuarantineId,
    );
    if (!found) return null;
    return { type: 'FeatureCollection', features: [found] };
  }, [selectedQuarantineId, quarantineData]);

  const selectedLeadPointCollection = useMemo<FeatureCollection<Point, LeadProperties> | null>(() => {
    if (!selectedLeadId || !leadCentroids) return null;
    const found = leadCentroids.features.find(
      (feature) => feature.properties.lead_id === selectedLeadId,
    );
    if (!found) return null;
    return { type: 'FeatureCollection', features: [found] };
  }, [selectedLeadId, leadCentroids]);

  const hoveredFeatureCollection = useMemo<FeatureCollection<Geometry, LeadProperties> | null>(() => {
    if (!hoveredLeadId || !data || hoveredLeadId === selectedLeadId) return null;
    const found = data.features.find(
      (feature) => feature.properties.lead_id === hoveredLeadId,
    );
    if (!found) return null;
    return { type: 'FeatureCollection', features: [found] };
  }, [data, hoveredLeadId, selectedLeadId]);

  const hoveredLeadPointCollection = useMemo<FeatureCollection<Point, LeadProperties> | null>(() => {
    if (!hoveredLeadId || !leadCentroids || hoveredLeadId === selectedLeadId) return null;
    const found = leadCentroids.features.find(
      (feature) => feature.properties.lead_id === hoveredLeadId,
    );
    if (!found) return null;
    return { type: 'FeatureCollection', features: [found] };
  }, [hoveredLeadId, leadCentroids, selectedLeadId]);

  const selectedQuarantinePointCollection = useMemo<FeatureCollection<Point, QuarantineParcelProperties> | null>(() => {
    if (!selectedQuarantineId || !quarantineCentroids) return null;
    const found = quarantineCentroids.features.find(
      (feature) => feature.properties.dzialka_id === selectedQuarantineId,
    );
    if (!found) return null;
    return { type: 'FeatureCollection', features: [found] };
  }, [selectedQuarantineId, quarantineCentroids]);

  const allFeatureBounds = useMemo(
    () => getFeatureCollectionBounds(data, quarantineData),
    [data, quarantineData],
  );

  const dataSignature = useMemo(() => {
    const leadSignature = data?.features
      .map((feature) => feature.properties.lead_id)
      .join('|') ?? '';
    const quarantineSignature = quarantineData?.features
      .map((feature) => feature.properties.dzialka_id)
      .join('|') ?? '';
    return `${leadSignature}::${quarantineSignature}`;
  }, [data, quarantineData]);

  // ---------------------------------------------------------------------------
  // Event handlers
  // ---------------------------------------------------------------------------

  const handleClick = useCallback(
    (event: MapLayerMouseEvent) => {
      const feature = event.features?.[0];
      if (!feature) {
        setSelectedLeadId(null);
        setSelectedQuarantineId(null);
        return;
      }

      const rawProps = feature.properties as Record<string, unknown> | null;

      if (isClusterProperties(rawProps) && feature.geometry.type === 'Point') {
        const map = mapRef.current?.getMap();
        const source = map?.getSource('lead-centroids');
        if (!map || !source || !('getClusterExpansionZoom' in source)) {
          return;
        }

        const [clusterLng, clusterLat] = feature.geometry.coordinates;
        if (clusterLng == null || clusterLat == null) {
          return;
        }
        const clusterCenter: [number, number] = [
          clusterLng,
          clusterLat,
        ];
        void (source as GeoJSONSource)
          .getClusterExpansionZoom(rawProps.cluster_id)
          .then((zoom) => {
            map.easeTo({
              center: clusterCenter,
              zoom,
              duration: 650,
            });
          })
          .catch(() => {
            // Ignore expansion failures; the map remains in its current state.
          });
        return;
      }

      if (isLeadProperties(rawProps)) {
        setSelectedQuarantineId(null);
        setSelectedLeadId(rawProps.lead_id);
        setHoveredLeadId(rawProps.lead_id);
        return;
      }

      if (isQuarantineProperties(rawProps)) {
        setSelectedLeadId(null);
        setSelectedQuarantineId(rawProps.dzialka_id);
      }
    },
    [setHoveredLeadId, setSelectedLeadId, setSelectedQuarantineId],
  );

  const handleMouseMove = useCallback((event: MapLayerMouseEvent) => {
    const feature = event.features?.[0];
    if (!feature) {
      setHoverInfo(null);
      setCursor('grab');
      return;
    }

    const rawProps = feature.properties as Record<string, unknown> | null;
    if (isClusterProperties(rawProps)) {
      setHoverInfo(null);
      setCursor('pointer');
      return;
    }

    if (isLeadProperties(rawProps)) {
      setCursor('pointer');
      setHoveredLeadId(rawProps.lead_id);
      setHoverInfo({
        kind: 'lead',
        longitude: event.lngLat.lng,
        latitude: event.lngLat.lat,
        properties: rawProps,
      });
      return;
    }

    if (isQuarantineProperties(rawProps)) {
      setHoveredLeadId(null);
      setCursor('pointer');
      setHoverInfo({
        kind: 'quarantine',
        longitude: event.lngLat.lng,
        latitude: event.lngLat.lat,
        properties: rawProps,
      });
      return;
    }

    setHoveredLeadId(null);
  }, [setHoveredLeadId]);

  const handleMouseLeave = useCallback(() => {
    setHoverInfo(null);
    setCursor('grab');
    setHoveredLeadId(null);
  }, [setHoveredLeadId]);

  // ---------------------------------------------------------------------------
  // Fly to selected lead when it changes
  // ---------------------------------------------------------------------------
  const prevSelectedLeadRef = useRef<string | null>(null);
  const prevSelectedQuarantineRef = useRef<string | null>(null);

  useEffect(() => {
    if (selectedLeadId === prevSelectedLeadRef.current) return;
    prevSelectedLeadRef.current = selectedLeadId;

    if (!selectedLeadId || !selectedLeadPointCollection || !mapRef.current) {
      return;
    }

    const selectedPoint = selectedLeadPointCollection.features[0];
    if (!selectedPoint) return;
    const [lng, lat] = selectedPoint.geometry.coordinates;
    if (lng == null || lat == null) return;

    mapRef.current.flyTo({
      center: [lng, lat],
      zoom: DETAIL_FLY_TO_ZOOM,
      duration: 800,
    });
  }, [selectedLeadId, selectedLeadPointCollection]);

  useEffect(() => {
    if (selectedQuarantineId === prevSelectedQuarantineRef.current) return;
    prevSelectedQuarantineRef.current = selectedQuarantineId;

    if (!selectedQuarantineId || !selectedQuarantinePointCollection || !mapRef.current) {
      return;
    }

    const selectedPoint = selectedQuarantinePointCollection.features[0];
    if (!selectedPoint) return;
    const [lng, lat] = selectedPoint.geometry.coordinates;
    if (lng == null || lat == null) return;

    mapRef.current.flyTo({
      center: [lng, lat],
      zoom: DETAIL_FLY_TO_ZOOM,
      duration: 800,
    });
  }, [selectedQuarantineId, selectedQuarantinePointCollection]);

  useEffect(() => {
    if (!mapLoaded || !mapRef.current || !allFeatureBounds) {
      return;
    }

    if (selectedLeadId || selectedQuarantineId) {
      return;
    }

    if (autoFitSignatureRef.current === dataSignature) {
      return;
    }

    autoFitSignatureRef.current = dataSignature;

    const [[minLng, minLat], [maxLng, maxLat]] = allFeatureBounds;
    const map = mapRef.current.getMap();
    const isCompactExtent = Math.abs(maxLng - minLng) < 0.12 && Math.abs(maxLat - minLat) < 0.12;

    if (isCompactExtent) {
      map.flyTo({
        center: [
          (minLng + maxLng) / 2,
          (minLat + maxLat) / 2,
        ],
        zoom: SINGLE_FEATURE_ZOOM,
        duration: 900,
      });
      return;
    }

    map.fitBounds(allFeatureBounds, {
      padding: FIT_BOUNDS_PADDING,
      maxZoom: AUTO_FIT_MAX_ZOOM,
      duration: 950,
    });
  }, [
    allFeatureBounds,
    dataSignature,
    mapLoaded,
    selectedLeadId,
    selectedQuarantineId,
  ]);

  useEffect(() => {
    if (!mapLoaded || !mapRef.current || !allFeatureBounds) {
      return;
    }

    const map = mapRef.current.getMap();
    map.fitBounds(allFeatureBounds, {
      padding: FIT_BOUNDS_PADDING,
      maxZoom: AUTO_FIT_MAX_ZOOM,
      duration: 850,
    });
  }, [allFeatureBounds, focusResultsNonce, mapLoaded]);

  // ---------------------------------------------------------------------------
  // Render
  // ---------------------------------------------------------------------------

  return (
    <Map
      ref={mapRef}
      mapStyle={MAP_STYLE}
      initialViewState={INITIAL_VIEW_STATE}
      cursor={cursor}
      dragRotate={false}
      touchZoomRotate={false}
      interactiveLayerIds={[
        'lead-cluster',
        'lead-marker-hit',
        'lead-marker',
        'leads-fill',
        'quarantine-marker-hit',
        'quarantine-marker',
        'quarantine-fill',
      ]}
      onClick={handleClick}
      onMouseMove={handleMouseMove}
      onMouseLeave={handleMouseLeave}
      onLoad={() => setMapLoaded(true)}
      style={{ width: '100%', height: '100%' }}
      attributionControl={{ compact: true }}
    >
      {/* Navigation controls */}
      <NavigationControl position="top-right" showCompass={false} />
      <ScaleControl position="bottom-right" unit="metric" />

      {/* Centroid markers keep leads visible before the operator zooms into parcel scale. */}
      {leadCentroids && (
        <Source
          id="lead-centroids"
          type="geojson"
          data={leadCentroids}
          cluster
          clusterMaxZoom={CLUSTER_MAX_ZOOM}
          clusterRadius={56}
          generateId={false}
        >
          <Layer {...LEAD_CLUSTER_GLOW_LAYER} />
          <Layer {...LEAD_CLUSTER_LAYER} />
          <Layer {...LEAD_CLUSTER_COUNT_LAYER} />
          <Layer {...LEAD_MARKER_HIT_LAYER} />
          <Layer {...LEAD_MARKER_GLOW_LAYER} />
          <Layer {...LEAD_MARKER_HALO_LAYER} />
          <Layer {...FUTURE_LEAD_MARKER_RING_LAYER} />
          <Layer {...LEAD_MARKER_LAYER} />
        </Source>
      )}

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

      {hoveredLeadPointCollection && (
        <Source
          id="hovered-lead-point"
          type="geojson"
          data={hoveredLeadPointCollection}
        >
          <Layer {...HOVERED_MARKER_HALO_LAYER} />
          <Layer {...HOVERED_MARKER_LAYER} />
        </Source>
      )}

      {hoveredFeatureCollection && (
        <Source
          id="hovered-lead"
          type="geojson"
          data={hoveredFeatureCollection as FeatureCollection}
        >
          <Layer {...HOVERED_OUTLINE_LAYER} />
        </Source>
      )}

      {quarantineCentroids && (
        <Source
          id="quarantine-centroids"
          type="geojson"
          data={quarantineCentroids}
          generateId={false}
        >
          <Layer {...QUARANTINE_MARKER_HIT_LAYER} />
          <Layer {...QUARANTINE_MARKER_LAYER} />
        </Source>
      )}

      {quarantineData && (
        <Source
          id="quarantine"
          type="geojson"
          data={quarantineData as FeatureCollection}
          generateId={false}
        >
          <Layer {...QUARANTINE_FILL_LAYER} />
          <Layer {...QUARANTINE_OUTLINE_LAYER} />
        </Source>
      )}

      {/* Selected lead centroid — visible even before polygon details are readable */}
      {selectedLeadPointCollection && (
        <Source
          id="selected-lead-point"
          type="geojson"
          data={selectedLeadPointCollection}
        >
          <Layer {...SELECTED_MARKER_HALO_LAYER} />
          <Layer {...SELECTED_MARKER_LAYER} />
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

      {selectedQuarantinePointCollection && (
        <Source
          id="selected-quarantine-point"
          type="geojson"
          data={selectedQuarantinePointCollection}
        >
          <Layer {...SELECTED_QUARANTINE_MARKER_LAYER} />
        </Source>
      )}

      {selectedQuarantineFeatureCollection && (
        <Source
          id="selected-quarantine"
          type="geojson"
          data={selectedQuarantineFeatureCollection as FeatureCollection}
        >
          <Layer {...SELECTED_QUARANTINE_OUTLINE_LAYER} />
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
          <div
            className={[
              'rounded-lg px-3 py-2 backdrop-blur-sm shadow-xl min-w-[180px]',
              hoverInfo.kind === 'quarantine'
                ? 'border border-yellow-500/40 bg-gray-950/95'
                : 'border border-gray-700 bg-gray-900/95',
            ].join(' ')}
          >
            <p className="font-mono text-[11px] text-gray-300 truncate max-w-[200px]">
              {hoverInfo.properties.identyfikator}
            </p>
            <div className="mt-1.5 flex items-center justify-between gap-2">
              <span className="text-[10px] text-gray-500">
                {'confidence_score' in hoverInfo.properties
                  ? (hoverInfo.properties.strategy_type === 'future_buildable'
                    ? (hoverInfo.properties.dominant_future_signal ?? hoverInfo.properties.dominant_przeznaczenie ?? '—')
                    : (hoverInfo.properties.dominant_przeznaczenie ?? '—'))
                  : (hoverInfo.properties.manual_przeznaczenie ?? hoverInfo.properties.reason ?? 'kwarantanna')}
              </span>
              {'confidence_score' in hoverInfo.properties ? (
                <ConfidenceBadge score={hoverInfo.properties.confidence_score} variant="badge" />
              ) : (
                <span className="inline-flex items-center rounded border border-yellow-500/30 bg-yellow-500/10 px-2 py-0.5 text-[10px] font-medium uppercase tracking-wider text-yellow-300">
                  review
                </span>
              )}
            </div>
            {'max_coverage_pct' in hoverInfo.properties && hoverInfo.properties.max_coverage_pct != null && (
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
