/**
 * Zustand store for map interaction state.
 * Only client state that genuinely needs to be shared across components lives here.
 * Server state (leads data) lives in TanStack Query — not here.
 */

import { create } from 'zustand';
import type { FutureConfidenceBand, LeadStatus, LeadStrategyType } from '../types/api';

export type LeadSortKey =
  | 'investment_score_desc'
  | 'confidence_desc'
  | 'price_per_m2_asc'
  | 'entry_price_asc'
  | 'buildable_area_desc';

export type PriceFilterKey =
  | 'all'
  | 'reliable'
  | 'suspicious'
  | 'missing';

export type LeadStatusFilterKey =
  | 'active'
  | LeadStatus;

export type StrategyFilterKey =
  | 'all'
  | LeadStrategyType;

export type ConfidenceBandFilterKey =
  | 'all'
  | FutureConfidenceBand;

interface MapStore {
  /** lead_id of the currently selected parcel (click) */
  selectedLeadId: string | null;
  setSelectedLeadId: (id: string | null) => void;

  /** dzialka_id of the currently selected quarantine parcel */
  selectedQuarantineId: string | null;
  setSelectedQuarantineId: (id: string | null) => void;

  /** lead_id of the currently hovered parcel (for cursor + tooltip) */
  hoveredLeadId: string | null;
  setHoveredLeadId: (id: string | null) => void;

  /** Filter controls (synced between sidebar and map) */
  minScore: number;
  setMinScore: (score: number) => void;

  limit: number;
  setLimit: (limit: number) => void;

  leadSort: LeadSortKey;
  setLeadSort: (sort: LeadSortKey) => void;

  priceFilter: PriceFilterKey;
  setPriceFilter: (filter: PriceFilterKey) => void;

  statusFilter: LeadStatusFilterKey;
  setStatusFilter: (filter: LeadStatusFilterKey) => void;

  strategyFilter: StrategyFilterKey;
  setStrategyFilter: (filter: StrategyFilterKey) => void;

  confidenceBandFilter: ConfidenceBandFilterKey;
  setConfidenceBandFilter: (filter: ConfidenceBandFilterKey) => void;

  cheapOnly: boolean;
  setCheapOnly: (value: boolean) => void;
}

export const useMapStore = create<MapStore>((set) => ({
  selectedLeadId: null,
  setSelectedLeadId: (id) => set({ selectedLeadId: id }),

  selectedQuarantineId: null,
  setSelectedQuarantineId: (id) => set({ selectedQuarantineId: id }),

  hoveredLeadId: null,
  setHoveredLeadId: (id) => set({ hoveredLeadId: id }),

  minScore: 0.7,
  setMinScore: (score) => set({ minScore: score }),

  limit: 100,
  setLimit: (limit) => set({ limit }),

  leadSort: 'investment_score_desc',
  setLeadSort: (sort) => set({ leadSort: sort }),

  priceFilter: 'reliable',
  setPriceFilter: (filter) => set({ priceFilter: filter }),

  statusFilter: 'active',
  setStatusFilter: (filter) => set({ statusFilter: filter }),

  strategyFilter: 'all',
  setStrategyFilter: (filter) => set({ strategyFilter: filter }),

  confidenceBandFilter: 'all',
  setConfidenceBandFilter: (filter) => set({ confidenceBandFilter: filter }),

  cheapOnly: false,
  setCheapOnly: (value) => set({ cheapOnly: value }),
}));
