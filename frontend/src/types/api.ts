/**
 * TypeScript interfaces for TerraZoning API responses.
 * Mirror backend app/schemas/leads.py — coordinate any contract changes with the Backend Lead.
 *
 * Geometry arrives as GeoJSON in EPSG:4326 (WGS84).
 * PostGIS handles ST_Transform(geom, 4326) — the browser never sees EPSG:2180.
 */

import type { Feature, FeatureCollection, Geometry } from 'geojson';

// ---------------------------------------------------------------------------
// Evidence chain
// ---------------------------------------------------------------------------

export type EvidenceStepName = 'source' | 'parcel' | 'delta' | 'document';

export interface EvidenceStepSource {
  step: 'source';
  ref: string;
  url?: string;
}

export interface EvidenceStepParcel {
  step: 'parcel';
  ref: string;
  teryt?: string;
  listing_ref?: string;
}

export interface EvidenceStepDelta {
  step: 'delta';
  ref: string;
  coverage: number;      // percentage 0–100
  przeznaczenie: string; // e.g. "MN"
  plan: string;          // e.g. "MPZP Wola 2022"
  plan_type: string;     // "mpzp" | "pog" | "studium"
  computed_at?: string;
}

export interface EvidenceStepDocument {
  step: 'document';
  ref: string;
  uri?: string;          // gs:// URI
}

export type EvidenceStep =
  | EvidenceStepSource
  | EvidenceStepParcel
  | EvidenceStepDelta
  | EvidenceStepDocument;

// ---------------------------------------------------------------------------
// Investment Lead
// ---------------------------------------------------------------------------

export type LeadPriority = 'high' | 'medium' | 'low';
export type LeadStatus = 'new' | 'reviewed' | 'shortlisted' | 'rejected' | 'invested';

export interface LeadProperties {
  lead_id: string;
  confidence_score: number;      // 0.0–1.0
  priority: LeadPriority;
  status: LeadStatus;
  area_m2: number | null;
  max_coverage_pct: number | null;
  dominant_przeznaczenie: string | null;
  identyfikator: string;         // canonical TERYT key: "{obreb}.{numer}"
  teryt_gmina: string;           // 7-char TERYT code
  listing_id: string | null;
  evidence_chain: EvidenceStep[];
  created_at: string;            // ISO 8601 UTC
}

// GeoJSON types — geometry in EPSG:4326 from ST_AsGeoJSON(ST_Transform(geom, 4326))
export type LeadFeature = Feature<Geometry, LeadProperties>;

// Backend extends FeatureCollection with `count` — Mapbox ignores unknown top-level fields
export interface LeadsFeatureCollection extends FeatureCollection<Geometry, LeadProperties> {
  count: number;
}

// ---------------------------------------------------------------------------
// Query parameters
// ---------------------------------------------------------------------------

export interface LeadsQueryParams {
  min_score?: number;
  limit?: number;
  include_count?: boolean;
}
