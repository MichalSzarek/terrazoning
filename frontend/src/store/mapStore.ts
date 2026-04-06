/**
 * Zustand store for map interaction state.
 * Only client state that genuinely needs to be shared across components lives here.
 * Server state (leads data) lives in TanStack Query — not here.
 */

import { create } from 'zustand';

interface MapStore {
  /** lead_id of the currently selected parcel (click) */
  selectedLeadId: string | null;
  setSelectedLeadId: (id: string | null) => void;

  /** lead_id of the currently hovered parcel (for cursor + tooltip) */
  hoveredLeadId: string | null;
  setHoveredLeadId: (id: string | null) => void;

  /** Filter controls (synced between sidebar and map) */
  minScore: number;
  setMinScore: (score: number) => void;

  limit: number;
  setLimit: (limit: number) => void;
}

export const useMapStore = create<MapStore>((set) => ({
  selectedLeadId: null,
  setSelectedLeadId: (id) => set({ selectedLeadId: id }),

  hoveredLeadId: null,
  setHoveredLeadId: (id) => set({ hoveredLeadId: id }),

  minScore: 0.7,
  setMinScore: (score) => set({ minScore: score }),

  limit: 100,
  setLimit: (limit) => set({ limit }),
}));
