/**
 * TypeScript interfaces for TerraZoning API responses.
 * Mirror backend app/schemas/leads.py — coordinate any contract changes with the Backend Lead.
 *
 * Geometry arrives as GeoJSON in EPSG:4326 (WGS84).
 * PostGIS handles ST_Transform(geom, 4326) — the browser never sees EPSG:2180.
 */

import type { Feature, FeatureCollection, Geometry, Point } from 'geojson';

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
export type LeadStrategyType = 'current_buildable' | 'future_buildable';
export type FutureConfidenceBand = 'formal' | 'supported' | 'speculative';

export interface SignalBreakdownItem {
  kind: string;
  status: string;
  designation_raw: string | null;
  designation_normalized: string | null;
  weight: number;
  source_url: string | null;
  evidence_label: string | null;
}

export interface LeadProperties {
  lead_id: string;
  confidence_score: number;      // 0.0–1.0
  priority: LeadPriority;
  strategy_type: LeadStrategyType;
  confidence_band: FutureConfidenceBand | null;
  status: LeadStatus;
  reviewed_at: string | null;
  notes: string | null;
  display_point: Point | null;
  area_m2: number | null;
  max_coverage_pct: number | null;
  max_buildable_area_m2: number | null;
  dominant_przeznaczenie: string | null;
  price_zl: number | null;
  price_per_m2_zl: number | null;
  price_signal: 'reliable' | 'suspicious' | 'missing';
  quality_signal: 'complete' | 'partial' | 'missing_financials' | 'review_required';
  investment_score: number;
  future_signal_score: number | null;
  cheapness_score: number | null;
  overall_score: number | null;
  signal_quality_tier: 'formal' | 'supported' | 'below_threshold' | 'blocked' | null;
  next_best_action: string | null;
  dominant_future_signal: string | null;
  future_signal_count: number | null;
  distance_to_nearest_buildable_m: number | null;
  adjacent_buildable_pct: number | null;
  missing_metrics: string[];
  identyfikator: string;         // canonical TERYT key: "{obreb}.{numer}"
  teryt_gmina: string;           // 7-char TERYT code
  listing_id: string | null;
  source_url: string | null;
  kw_number: string | null;
  ekw_search_url: string | null;
  evidence_chain: EvidenceStep[];
  signal_breakdown: SignalBreakdownItem[];
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
  status_filter?: LeadStatus;
  strategy_filter?: LeadStrategyType;
  confidence_band_filter?: FutureConfidenceBand;
  cheap_only?: boolean;
  min_price_zl?: number;
  max_price_zl?: number;
  min_price_per_m2_zl?: number;
  max_price_per_m2_zl?: number;
  min_area_m2?: number;
  max_area_m2?: number;
  min_coverage_pct?: number;
  min_buildable_area_m2?: number;
  teryt_prefix?: string;
  teryt_gmina?: string;
  designation?: string;
  search?: string;
}

export interface LeadStatusUpdatePayload {
  status: Exclude<LeadStatus, 'new'>;
  notes?: string;
}

export interface LeadStatusUpdateResponse {
  lead_id: string;
  status: LeadStatus;
  reviewed_at: string | null;
  notes: string | null;
}

// ---------------------------------------------------------------------------
// Quarantine parcels
// ---------------------------------------------------------------------------

export interface QuarantineParcelProperties {
  dzialka_id: string;
  identyfikator: string;
  teryt_gmina: string | null;
  area_m2: number | null;
  source_url: string | null;
  reason: string | null;
  status: string | null;
  current_use: string | null;
  dominant_przeznaczenie: string | null;
  manual_przeznaczenie: string | null;
  created_at: string | null;
}

export type QuarantineParcelFeature = Feature<Geometry, QuarantineParcelProperties>;

export interface QuarantineParcelFeatureCollection extends FeatureCollection<Geometry, QuarantineParcelProperties> {
  count: number;
}

export interface ManualOverridePayload {
  manual_przeznaczenie: string;
}

// ---------------------------------------------------------------------------
// Investor watchlist
// ---------------------------------------------------------------------------

export interface InvestorWatchlist {
  max_price_per_m2: number | null;
  min_coverage_pct: number;
  min_confidence_pct: number;
  required_designation: string;
  only_reliable_price: boolean;
  acknowledged_at: string | null;
  updated_at: string;
}

export interface InvestorWatchlistUpdatePayload {
  max_price_per_m2: number | null;
  min_coverage_pct: number;
  min_confidence_pct: number;
  required_designation: string;
  only_reliable_price: boolean;
  acknowledged_at: string | null;
}

export interface MarketBenchmarkResponse {
  teryt_gmina: string;
  scope: 'gmina' | 'powiat' | 'wojewodztwo';
  sample_size: number;
  p25_price_per_m2_zl: number | null;
  p40_price_per_m2_zl: number | null;
  median_price_per_m2_zl: number | null;
}
