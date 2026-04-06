/**
 * TerraZoning — Kokpit Inwestorski
 *
 * Layout: fixed sidebar (320px) + full-height map.
 * No layout shift — both panels fill the viewport height immediately.
 *
 * Data flow:
 *   useLeads() [TanStack Query] → LeadsFeatureCollection
 *     → LeadsMap (renders GeoJSON polygons)
 *     → LeadList (renders sortable rows)
 *     → LeadDetail (renders on selection)
 *
 * State:
 *   selectedLeadId, minScore, limit  — Zustand (mapStore)
 *   leads data, loading, error       — TanStack Query
 */

import { useMemo } from 'react';
import { Activity, SlidersHorizontal, RefreshCw } from 'lucide-react';

import { useLeads } from './hooks/useLeads';
import { useMapStore } from './store/mapStore';
import { LeadsMap } from './components/map/LeadsMap';
import { LeadList } from './components/sidebar/LeadList';
import { LeadDetail } from './components/sidebar/LeadDetail';
import type { LeadFeature } from './types/api';

// ---------------------------------------------------------------------------
// Filter bar
// ---------------------------------------------------------------------------

interface FilterBarProps {
  minScore: number;
  onMinScoreChange: (v: number) => void;
  count: number | undefined;
  isFetching: boolean;
  onRefresh: () => void;
}

function FilterBar({ minScore, onMinScoreChange, count, isFetching, onRefresh }: FilterBarProps) {
  return (
    <div className="flex items-center gap-3 border-b border-gray-800 bg-gray-900 px-4 py-2 flex-shrink-0">
      <SlidersHorizontal size={12} className="text-gray-500 flex-shrink-0" aria-hidden />
      <label htmlFor="min-score-range" className="text-[10px] text-gray-500 whitespace-nowrap">
        min score
      </label>
      <input
        id="min-score-range"
        type="range"
        min={0}
        max={1}
        step={0.05}
        value={minScore}
        onChange={(e) => onMinScoreChange(parseFloat(e.target.value))}
        className="h-1 flex-1 cursor-pointer accent-amber-500"
        aria-valuetext={`${Math.round(minScore * 100)}%`}
      />
      <span className="font-mono text-[11px] text-amber-400 w-8 text-right flex-shrink-0">
        {Math.round(minScore * 100)}%
      </span>
      {count !== undefined && (
        <span className="text-[10px] text-gray-600 whitespace-nowrap">{count} leadów</span>
      )}
      <button
        type="button"
        onClick={onRefresh}
        disabled={isFetching}
        className="ml-auto rounded p-1 text-gray-500 hover:text-gray-300 hover:bg-gray-800 transition-colors disabled:opacity-40 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-amber-500"
        aria-label="Odśwież dane"
      >
        <RefreshCw
          size={12}
          aria-hidden
          className={isFetching ? 'animate-spin' : ''}
        />
      </button>
    </div>
  );
}

// ---------------------------------------------------------------------------
// App
// ---------------------------------------------------------------------------

export default function App() {
  const minScore = useMapStore((s) => s.minScore);
  const setMinScore = useMapStore((s) => s.setMinScore);
  const selectedLeadId = useMapStore((s) => s.selectedLeadId);
  const setSelectedLeadId = useMapStore((s) => s.setSelectedLeadId);

  const { data, isLoading, isFetching, error, refetch } = useLeads({
    min_score: minScore,
    limit: 100,
  });

  // Find the selected lead feature from the collection
  const selectedFeature = useMemo<LeadFeature | null>(() => {
    if (!selectedLeadId || !data) return null;
    return data.features.find((f) => f.properties.lead_id === selectedLeadId) ?? null;
  }, [selectedLeadId, data]);

  return (
    <div className="flex h-screen flex-col overflow-hidden bg-[#0d1117]">
      {/* ── Top bar ─────────────────────────────────────────────────────── */}
      <header className="flex h-10 flex-shrink-0 items-center justify-between border-b border-gray-800 bg-gray-900 px-4">
        <div className="flex items-center gap-2">
          <Activity size={14} className="text-amber-500" aria-hidden />
          <span className="text-sm font-semibold tracking-tight text-gray-100">
            TerraZoning
          </span>
          <span className="ml-1 text-xs text-gray-600">— Kokpit Inwestorski</span>
        </div>
        <div className="flex items-center gap-2 text-[10px] text-gray-600">
          <span className="hidden sm:inline">
            {data ? `${data.features.length} parcel loaded` : 'loading…'}
          </span>
          {isFetching && !isLoading && (
            <span className="flex items-center gap-1 text-amber-500">
              <RefreshCw size={9} className="animate-spin" aria-hidden />
              sync
            </span>
          )}
        </div>
      </header>

      {/* ── Main content ────────────────────────────────────────────────── */}
      <div className="flex flex-1 overflow-hidden">
        {/* ── Sidebar (320px fixed) ──────────────────────────────────── */}
        <aside
          className="flex w-80 flex-shrink-0 flex-col border-r border-gray-800 bg-gray-900"
          aria-label="Panel leadów inwestycyjnych"
        >
          {/* Filter controls */}
          <FilterBar
            minScore={minScore}
            onMinScoreChange={setMinScore}
            count={data?.features.length}
            isFetching={isFetching}
            onRefresh={() => void refetch()}
          />

          {/* List / Detail panel */}
          <div className="flex-1 overflow-hidden">
            {selectedFeature ? (
              <LeadDetail
                feature={selectedFeature}
                onBack={() => setSelectedLeadId(null)}
              />
            ) : (
              <div className="h-full overflow-y-auto scrollbar-thin scrollbar-track-transparent scrollbar-thumb-gray-700">
                <LeadList
                  features={data?.features ?? []}
                  isLoading={isLoading}
                  error={error}
                />
              </div>
            )}
          </div>
        </aside>

        {/* ── Map ──────────────────────────────────────────────────────── */}
        <main className="relative flex-1" aria-label="Mapa okazji inwestycyjnych">
          <LeadsMap data={data} />

          {/* Map legend */}
          <div
            className="absolute bottom-8 left-4 rounded-lg border border-gray-700 bg-gray-900/90 px-3 py-2 backdrop-blur-sm text-[10px] text-gray-400"
            aria-label="Legenda mapy"
          >
            <p className="mb-1.5 font-medium text-gray-500 uppercase tracking-wider text-[9px]">
              Confidence Score
            </p>
            {[
              { color: '#fbbf24', label: '70–80%  Umiarkowany' },
              { color: '#f97316', label: '80–90%  Wysoki' },
              { color: '#ef4444', label: '90%+    Prime' },
            ].map(({ color, label }) => (
              <div key={label} className="flex items-center gap-2 mb-0.5">
                <span
                  className="h-2.5 w-2.5 flex-shrink-0 rounded-sm"
                  style={{ backgroundColor: color }}
                  aria-hidden
                />
                <span className="font-mono">{label}</span>
              </div>
            ))}
          </div>
        </main>
      </div>
    </div>
  );
}
