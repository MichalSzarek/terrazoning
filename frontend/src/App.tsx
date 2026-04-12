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

import { lazy, Suspense, useEffect, useMemo, useRef } from 'react';
import { Activity, SlidersHorizontal, RefreshCw } from 'lucide-react';

import { useLeads } from './hooks/useLeads';
import { useQuarantineParcels } from './hooks/useQuarantineParcels';
import { useMapStore } from './store/mapStore';
import type {
  ConfidenceBandFilterKey,
  LeadSortKey,
  LeadStatusFilterKey,
  PriceFilterKey,
  StrategyFilterKey,
} from './store/mapStore';
import { LeadList } from './components/sidebar/LeadList';
import { LeadDetail } from './components/sidebar/LeadDetail';
import { QuarantineList } from './components/sidebar/QuarantineList';
import { QuarantineDetail } from './components/sidebar/QuarantineDetail';
import { InvestorSnapshot } from './components/sidebar/InvestorSnapshot';
import { ShortlistSection } from './components/sidebar/ShortlistSection';
import { WatchlistSection } from './components/sidebar/WatchlistSection';
import {
  filterLeadFeaturesByPrice,
  filterLeadFeaturesForView,
  getConfidenceBandDescription,
  getConfidenceBandLabel,
  getStrategyDescription,
  getStrategyLabel,
  sortLeadFeatures,
} from './lib/investorMetrics';
import { futureBuildabilityEnabled } from './lib/featureFlags';
import type { LeadFeature, QuarantineParcelFeature } from './types/api';

const LeadsMap = lazy(async () => {
  const module = await import('./components/map/LeadsMap');
  return { default: module.LeadsMap };
});

// ---------------------------------------------------------------------------
// Filter bar
// ---------------------------------------------------------------------------

interface FilterBarProps {
  minScore: number;
  onMinScoreChange: (v: number) => void;
  sortKey: LeadSortKey;
  onSortChange: (sort: LeadSortKey) => void;
  priceFilter: PriceFilterKey;
  onPriceFilterChange: (filter: PriceFilterKey) => void;
  statusFilter: LeadStatusFilterKey;
  onStatusFilterChange: (filter: LeadStatusFilterKey) => void;
  strategyFilter: StrategyFilterKey;
  onStrategyFilterChange: (filter: StrategyFilterKey) => void;
  confidenceBandFilter: ConfidenceBandFilterKey;
  onConfidenceBandFilterChange: (filter: ConfidenceBandFilterKey) => void;
  cheapOnly: boolean;
  onCheapOnlyChange: (value: boolean) => void;
  futureBuildabilityEnabled: boolean;
  onApplyPreset: (preset: 'default' | 'future_safe') => void;
  onResetFilters: () => void;
  activeFilterCount: number;
  count: number | undefined;
  isFetching: boolean;
  onRefresh: () => void;
}

function FilterBar({
  minScore,
  onMinScoreChange,
  sortKey,
  onSortChange,
  priceFilter,
  onPriceFilterChange,
  statusFilter,
  onStatusFilterChange,
  strategyFilter,
  onStrategyFilterChange,
  confidenceBandFilter,
  onConfidenceBandFilterChange,
  cheapOnly,
  onCheapOnlyChange,
  futureBuildabilityEnabled,
  onApplyPreset,
  onResetFilters,
  activeFilterCount,
  count,
  isFetching,
  onRefresh,
}: FilterBarProps) {
  const futureSafeMode = futureBuildabilityEnabled && strategyFilter === 'future_buildable' && confidenceBandFilter === 'all';

  return (
    <div className="flex flex-col gap-3 border-b border-gray-800 bg-gray-900/95 px-4 py-3 backdrop-blur-sm flex-shrink-0">
      <div className="flex items-center gap-2">
        <div className="flex min-w-0 items-center gap-2">
          <SlidersHorizontal size={12} className="text-gray-500 flex-shrink-0" aria-hidden />
          <div className="min-w-0">
            <p className="text-[10px] font-semibold uppercase tracking-[0.2em] text-gray-500">
              Filtry leadów
            </p>
            <p className="text-[10px] text-gray-600">
              {count !== undefined ? `${count} wyników po filtrach` : 'Ładowanie wyników'}
            </p>
          </div>
        </div>
        {activeFilterCount > 0 && (
          <span className="ml-auto rounded-full border border-amber-500/20 bg-amber-500/10 px-2 py-0.5 text-[10px] font-medium text-amber-300">
            {activeFilterCount} aktywne
          </span>
        )}
        <button
          type="button"
          onClick={onResetFilters}
          className="rounded-md border border-gray-700 bg-gray-950 px-2 py-1 text-[10px] text-gray-300 transition-colors hover:border-gray-600 hover:bg-gray-900 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-amber-500"
        >
          reset
        </button>
        <button
          type="button"
          onClick={onRefresh}
          disabled={isFetching}
          className="rounded p-1 text-gray-500 hover:text-gray-300 hover:bg-gray-800 transition-colors disabled:opacity-40 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-amber-500"
          aria-label="Odśwież dane"
        >
          <RefreshCw
            size={12}
            aria-hidden
            className={isFetching ? 'animate-spin' : ''}
          />
        </button>
      </div>

      <div className="rounded-xl border border-gray-800 bg-gray-950/80 px-3 py-2">
        <div className="flex items-center justify-between gap-3">
          <label htmlFor="min-score-range" className="text-[10px] uppercase tracking-[0.18em] text-gray-500 whitespace-nowrap">
            min score
          </label>
          <span className="font-mono text-[11px] text-amber-400">
            {Math.round(minScore * 100)}%
          </span>
        </div>
        <input
          id="min-score-range"
          type="range"
          min={0}
          max={1}
          step={0.05}
          value={minScore}
          onChange={(e) => onMinScoreChange(parseFloat(e.target.value))}
          className="mt-2 h-1.5 w-full cursor-pointer accent-amber-500"
          aria-valuetext={`${Math.round(minScore * 100)}%`}
        />
      </div>

      <div className="grid grid-cols-2 gap-2">
        <div className="flex flex-col gap-1">
          <label htmlFor="lead-sort" className="text-[10px] uppercase tracking-[0.18em] text-gray-500">
            sortuj
          </label>
          <select
            id="lead-sort"
            value={sortKey}
            onChange={(e) => onSortChange(e.target.value as LeadSortKey)}
            className="w-full rounded-lg border border-gray-700 bg-gray-950 px-2.5 py-2 text-[11px] text-gray-300 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-amber-500"
          >
            <option value="investment_score_desc">okazje inwestorskie</option>
            <option value="confidence_desc">confidence</option>
            <option value="price_per_m2_asc">zł/m² ↑</option>
            <option value="entry_price_asc">cena wejścia ↑</option>
            <option value="buildable_area_desc">pow. budowlana ↓</option>
          </select>
        </div>
        <div className="flex flex-col gap-1">
          <label htmlFor="price-filter" className="text-[10px] uppercase tracking-[0.18em] text-gray-500">
            cena
          </label>
          <select
            id="price-filter"
            value={priceFilter}
            onChange={(e) => onPriceFilterChange(e.target.value as PriceFilterKey)}
            className="w-full rounded-lg border border-gray-700 bg-gray-950 px-2.5 py-2 text-[11px] text-gray-300 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-amber-500"
          >
            <option value="all">wszystkie</option>
            <option value="reliable">wiarygodne</option>
            <option value="suspicious">do weryfikacji</option>
            <option value="missing">brak ceny</option>
          </select>
        </div>
        <div className="flex flex-col gap-1">
          <label htmlFor="status-filter" className="text-[10px] uppercase tracking-[0.18em] text-gray-500">
            status
          </label>
          <select
            id="status-filter"
            value={statusFilter}
            onChange={(e) => onStatusFilterChange(e.target.value as LeadStatusFilterKey)}
            className="w-full rounded-lg border border-gray-700 bg-gray-950 px-2.5 py-2 text-[11px] text-gray-300 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-amber-500"
          >
            <option value="active">aktywne</option>
            <option value="new">new</option>
            <option value="reviewed">reviewed</option>
            <option value="shortlisted">shortlisted</option>
            <option value="rejected">rejected</option>
            <option value="invested">invested</option>
          </select>
        </div>
        <div className="flex flex-col gap-1">
          <label htmlFor="strategy-filter" className="text-[10px] uppercase tracking-[0.18em] text-gray-500">
            strategia
          </label>
          <select
            id="strategy-filter"
            value={strategyFilter}
            onChange={(e) => onStrategyFilterChange(e.target.value as StrategyFilterKey)}
            className="w-full rounded-lg border border-gray-700 bg-gray-950 px-2.5 py-2 text-[11px] text-gray-300 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-amber-500"
          >
            <option value="all">wszystkie</option>
            <option value="current_buildable">aktualnie budowlane</option>
            {futureBuildabilityEnabled && <option value="future_buildable">przyszłe budowlane</option>}
          </select>
        </div>
        {futureBuildabilityEnabled && (
          <div className="flex flex-col gap-1">
            <label htmlFor="confidence-band-filter" className="text-[10px] uppercase tracking-[0.18em] text-gray-500">
              pewność
            </label>
            <select
              id="confidence-band-filter"
              value={confidenceBandFilter}
              onChange={(e) => onConfidenceBandFilterChange(e.target.value as ConfidenceBandFilterKey)}
              className="w-full rounded-lg border border-gray-700 bg-gray-950 px-2.5 py-2 text-[11px] text-gray-300 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-amber-500"
            >
              <option value="all">
                {futureSafeMode ? 'wszystkie (bez speculative)' : 'all'}
              </option>
              <option value="formal">formalne</option>
              <option value="supported">wspierane</option>
              <option value="speculative">spekulacyjne</option>
            </select>
          </div>
        )}
        {futureBuildabilityEnabled && futureSafeMode && (
          <div className="flex items-center">
            <span className="rounded border border-sky-500/20 bg-sky-500/10 px-2 py-1 text-[10px] font-medium text-sky-300">
              safe mode
            </span>
          </div>
        )}
      </div>

      <div className="flex flex-wrap items-center gap-2">
        <div className="flex items-center gap-1 rounded-full border border-gray-800 bg-gray-950/80 p-1">
          <button
            type="button"
            onClick={() => onStrategyFilterChange('all')}
            className={[
              'rounded-full px-2.5 py-1 text-[10px] font-medium transition-colors',
              strategyFilter === 'all'
                ? 'bg-gray-800 text-gray-100'
                : 'text-gray-500 hover:bg-gray-800 hover:text-gray-200',
            ].join(' ')}
          >
            wszystkie
          </button>
          <button
            type="button"
            onClick={() => onStrategyFilterChange('current_buildable')}
            className={[
              'rounded-full px-2.5 py-1 text-[10px] font-medium transition-colors',
              strategyFilter === 'current_buildable'
                ? 'bg-emerald-500/15 text-emerald-200'
                : 'text-gray-500 hover:bg-gray-800 hover:text-gray-200',
            ].join(' ')}
          >
            current
          </button>
          {futureBuildabilityEnabled && (
            <button
              type="button"
              onClick={() => onStrategyFilterChange('future_buildable')}
              className={[
                'rounded-full px-2.5 py-1 text-[10px] font-medium transition-colors',
                strategyFilter === 'future_buildable'
                  ? 'bg-sky-500/15 text-sky-200'
                  : 'text-gray-500 hover:bg-gray-800 hover:text-gray-200',
              ].join(' ')}
            >
              future
            </button>
          )}
        </div>
        {futureBuildabilityEnabled && (
          <div className="flex items-center gap-1 rounded-full border border-gray-800 bg-gray-950/80 px-1 py-0.5">
            <button
              type="button"
              onClick={() => onApplyPreset('default')}
              className="rounded-full px-2 py-1 text-[10px] font-medium text-gray-400 hover:bg-gray-800 hover:text-gray-200"
              title="Przywróć standardowy preset"
            >
              current
            </button>
            <button
              type="button"
              onClick={() => onApplyPreset('future_safe')}
              className="rounded-full px-2 py-1 text-[10px] font-medium text-sky-300 hover:bg-sky-500/10 hover:text-sky-200"
              title="Preset future_buildable: formal + supported + cheap_only"
            >
              future safe
            </button>
          </div>
        )}
        <label className="flex items-center gap-1 text-[10px] text-gray-500 whitespace-nowrap">
          <input
            type="checkbox"
            checked={cheapOnly}
            onChange={(e) => onCheapOnlyChange(e.target.checked)}
            className="accent-amber-500"
          />
          cheap only
        </label>
      </div>
    </div>
  );
}

function MapLoadingFallback() {
  return (
    <div className="absolute inset-0 flex items-center justify-center bg-[radial-gradient(circle_at_top,_rgba(251,191,36,0.12),_transparent_35%),linear-gradient(180deg,_#0f172a_0%,_#111827_100%)]">
      <div className="rounded-2xl border border-gray-800 bg-gray-950/80 px-5 py-4 text-center backdrop-blur-sm">
        <p className="text-xs font-semibold uppercase tracking-[0.24em] text-amber-300">
          Ładowanie mapy
        </p>
        <p className="mt-2 text-sm text-gray-400">
          Przygotowuję warstwę parceli i klastrów inwestycyjnych.
        </p>
      </div>
    </div>
  );
}

function MapEmptyState() {
  return (
    <div className="pointer-events-none absolute inset-x-0 top-6 z-10 flex justify-center px-6">
      <div className="max-w-md rounded-2xl border border-gray-800 bg-gray-950/88 px-4 py-3 text-center shadow-2xl shadow-black/20 backdrop-blur-sm">
        <p className="text-[11px] font-semibold uppercase tracking-[0.22em] text-amber-300">
          Brak wynikow na mapie
        </p>
        <p className="mt-1 text-sm text-gray-300">
          Biezace filtry nie zwracaja zadnych leadow.
        </p>
        <p className="mt-1 text-xs text-gray-500">
          Zmien strategia, obniz min score albo wylacz `cheap only`.
        </p>
      </div>
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
  const selectedQuarantineId = useMapStore((s) => s.selectedQuarantineId);
  const setSelectedQuarantineId = useMapStore((s) => s.setSelectedQuarantineId);
  const leadSort = useMapStore((s) => s.leadSort);
  const setLeadSort = useMapStore((s) => s.setLeadSort);
  const priceFilter = useMapStore((s) => s.priceFilter);
  const setPriceFilter = useMapStore((s) => s.setPriceFilter);
  const statusFilter = useMapStore((s) => s.statusFilter);
  const setStatusFilter = useMapStore((s) => s.setStatusFilter);
  const strategyFilter = useMapStore((s) => s.strategyFilter);
  const setStrategyFilter = useMapStore((s) => s.setStrategyFilter);
  const confidenceBandFilter = useMapStore((s) => s.confidenceBandFilter);
  const setConfidenceBandFilter = useMapStore((s) => s.setConfidenceBandFilter);
  const cheapOnly = useMapStore((s) => s.cheapOnly);
  const setCheapOnly = useMapStore((s) => s.setCheapOnly);
  const previousStrategyFilterRef = useRef<StrategyFilterKey | null>(null);
  const activeFilterCount = [
    minScore !== 0.7,
    leadSort !== 'investment_score_desc',
    priceFilter !== 'reliable',
    statusFilter !== 'active',
    strategyFilter !== 'all',
    confidenceBandFilter !== 'all',
    cheapOnly,
  ].filter(Boolean).length;

  const { data, isLoading, isFetching, error, refetch } = useLeads({
    min_score: minScore,
    limit: 100,
    ...(statusFilter !== 'active' ? { status_filter: statusFilter } : {}),
    ...(strategyFilter !== 'all' ? { strategy_filter: strategyFilter } : {}),
    ...(confidenceBandFilter !== 'all' ? { confidence_band_filter: confidenceBandFilter } : {}),
    ...(cheapOnly ? { cheap_only: true } : {}),
  });
  const {
    data: quarantineData,
    isLoading: isQuarantineLoading,
    error: quarantineError,
    refetch: refetchQuarantine,
  } = useQuarantineParcels();
  const {
    data: shortlistData,
    isLoading: isShortlistLoading,
    error: shortlistError,
  } = useLeads({
    min_score: 0,
    limit: 100,
    status_filter: 'shortlisted',
  });
  const {
    data: watchlistData,
    isLoading: isWatchlistLoading,
    error: watchlistError,
  } = useLeads({
    min_score: 0,
    limit: 100,
  });

  const selectedQuarantineFeature = useMemo<QuarantineParcelFeature | null>(() => {
    if (!selectedQuarantineId || !quarantineData) return null;
    return quarantineData.features.find((f) => f.properties.dzialka_id === selectedQuarantineId) ?? null;
  }, [selectedQuarantineId, quarantineData]);

  const sortedLeads = useMemo<LeadFeature[]>(() => {
    const sorted = sortLeadFeatures(data?.features ?? [], leadSort);
    return filterLeadFeaturesByPrice(sorted, priceFilter);
  }, [data?.features, leadSort, priceFilter]);

  const visibleLeads = useMemo<LeadFeature[]>(() => (
    filterLeadFeaturesForView(sortedLeads, strategyFilter, confidenceBandFilter, futureBuildabilityEnabled)
  ), [confidenceBandFilter, futureBuildabilityEnabled, sortedLeads, strategyFilter]);

  const visibleData = useMemo(() => {
    if (!data) return data;
    return {
      ...data,
      features: visibleLeads,
      count: visibleLeads.length,
    };
  }, [data, visibleLeads]);

  const mapData = visibleData;
  const mapQuarantineData = selectedQuarantineFeature ? quarantineData : undefined;

  const selectedFeature = useMemo<LeadFeature | null>(() => {
    if (!selectedLeadId || !mapData) return null;
    return mapData.features.find((f) => f.properties.lead_id === selectedLeadId) ?? null;
  }, [selectedLeadId, mapData]);

  useEffect(() => {
    if (futureBuildabilityEnabled && strategyFilter === 'future_buildable' && minScore > 0.5) {
      setMinScore(0.5);
    }
  }, [futureBuildabilityEnabled, strategyFilter, minScore, setMinScore]);

  useEffect(() => {
    const previousStrategyFilter = previousStrategyFilterRef.current;
    if (
      futureBuildabilityEnabled &&
      strategyFilter === 'future_buildable' &&
      previousStrategyFilter !== 'future_buildable' &&
      !cheapOnly
    ) {
      setCheapOnly(true);
    }
    previousStrategyFilterRef.current = strategyFilter;
  }, [cheapOnly, futureBuildabilityEnabled, setCheapOnly, strategyFilter]);

  useEffect(() => {
    if (!futureBuildabilityEnabled && strategyFilter === 'future_buildable') {
      setStrategyFilter('all');
      setConfidenceBandFilter('all');
    }
  }, [futureBuildabilityEnabled, strategyFilter, setConfidenceBandFilter, setStrategyFilter]);

  useEffect(() => {
    if (!futureBuildabilityEnabled && confidenceBandFilter !== 'all') {
      setConfidenceBandFilter('all');
    }
  }, [futureBuildabilityEnabled, confidenceBandFilter, setConfidenceBandFilter]);

  function applyPreset(preset: 'default' | 'future_safe'): void {
    if (preset === 'default') {
      setStrategyFilter('all');
      setConfidenceBandFilter('all');
      setCheapOnly(false);
      setMinScore(0.7);
      setPriceFilter('reliable');
      setLeadSort('investment_score_desc');
      setStatusFilter('active');
      return;
    }

    setStrategyFilter('future_buildable');
    setConfidenceBandFilter('all');
    setCheapOnly(true);
    setMinScore(0.5);
    setPriceFilter('reliable');
    setLeadSort('price_per_m2_asc');
    setStatusFilter('active');
  }

  useEffect(() => {
    if (selectedLeadId && mapData && !selectedFeature) {
      setSelectedLeadId(null);
    }
  }, [selectedLeadId, selectedFeature, setSelectedLeadId, mapData]);

  useEffect(() => {
    if (selectedQuarantineId && quarantineData && !selectedQuarantineFeature) {
      setSelectedQuarantineId(null);
    }
  }, [selectedQuarantineId, quarantineData, selectedQuarantineFeature, setSelectedQuarantineId]);

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
            {visibleData ? `${visibleData.features.length} leadów` : 'loading…'}
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
          className="flex w-[22.5rem] xl:w-[24rem] flex-shrink-0 flex-col border-r border-gray-800 bg-gray-900"
          aria-label="Panel leadów inwestycyjnych"
        >
          {/* Filter controls */}
          <FilterBar
            minScore={minScore}
            onMinScoreChange={setMinScore}
            sortKey={leadSort}
            onSortChange={setLeadSort}
            priceFilter={priceFilter}
            onPriceFilterChange={setPriceFilter}
            statusFilter={statusFilter}
            onStatusFilterChange={setStatusFilter}
            strategyFilter={strategyFilter}
            onStrategyFilterChange={setStrategyFilter}
            confidenceBandFilter={confidenceBandFilter}
            onConfidenceBandFilterChange={setConfidenceBandFilter}
            cheapOnly={cheapOnly}
            onCheapOnlyChange={setCheapOnly}
            futureBuildabilityEnabled={futureBuildabilityEnabled}
            onApplyPreset={applyPreset}
            onResetFilters={() => applyPreset('default')}
            activeFilterCount={activeFilterCount}
            count={visibleLeads.length}
            isFetching={isFetching}
            onRefresh={() => {
              void refetch();
              void refetchQuarantine();
            }}
          />

          {/* List / Detail panel */}
          <div className="flex-1 overflow-hidden">
            {selectedFeature ? (
              <LeadDetail
                feature={selectedFeature}
                onBack={() => setSelectedLeadId(null)}
              />
            ) : selectedQuarantineFeature ? (
              <QuarantineDetail
                feature={selectedQuarantineFeature}
                onBack={() => setSelectedQuarantineId(null)}
              />
            ) : (
              <div className="h-full overflow-y-auto scrollbar-thin scrollbar-track-transparent scrollbar-thumb-gray-700">
                <InvestorSnapshot features={visibleLeads} />
                {futureBuildabilityEnabled && strategyFilter === 'future_buildable' && confidenceBandFilter === 'all' && (
                  <div className="border-b border-gray-800 px-4 py-2">
                    <div className="rounded-xl border border-sky-500/20 bg-sky-500/10 px-3 py-2 text-[11px] text-sky-200">
                      Safe mode aktywny: pokazuję `formal` + `supported`, a `speculative` jest ukryte do czasu świadomego wyboru.
                    </div>
                  </div>
                )}
                <WatchlistSection
                  features={watchlistData?.features ?? []}
                  isLoading={isWatchlistLoading}
                  error={watchlistError}
                />
                <ShortlistSection
                  features={shortlistData?.features ?? []}
                  isLoading={isShortlistLoading}
                  error={shortlistError}
                />
                <QuarantineList
                  features={quarantineData?.features ?? []}
                  isLoading={isQuarantineLoading}
                  error={quarantineError}
                />
                <LeadList
                  features={visibleLeads}
                  isLoading={isLoading}
                  error={error}
                />
              </div>
            )}
          </div>
        </aside>

        {/* ── Map ──────────────────────────────────────────────────────── */}
        <main className="relative flex-1" aria-label="Mapa okazji inwestycyjnych">
          {(!mapData || mapData.features.length === 0) && <MapEmptyState />}
          <Suspense fallback={<MapLoadingFallback />}>
            <LeadsMap data={mapData} quarantineData={mapQuarantineData} />
          </Suspense>

          {/* Map legend */}
          <div
            className="absolute bottom-8 left-4 w-[290px] rounded-xl border border-gray-700 bg-gray-900/90 px-3 py-3 backdrop-blur-sm text-[10px] text-gray-400 shadow-2xl shadow-black/20"
            aria-label="Legenda mapy"
          >
            <div className="flex items-center justify-between gap-2">
              <p className="font-medium uppercase tracking-wider text-[9px] text-gray-500">
                Legenda mapy
              </p>
              <p className="text-[9px] text-gray-600">
                current_buildable vs future_buildable
              </p>
            </div>
            <p className="mt-1 max-w-[230px] text-[9px] leading-relaxed text-gray-500">
              Przy małym zoomie leady grupują się w markery i klastry. Granice działek pojawiają się po zbliżeniu.
            </p>
            <div className="mt-2 grid grid-cols-1 gap-2">
              <div>
                <p className="mb-1 font-medium uppercase tracking-wider text-[9px] text-emerald-300">
                  {getStrategyLabel('current_buildable')}
                </p>
                <div className="flex items-center gap-2 text-gray-400">
                  <span className="h-2.5 w-2.5 rounded-sm bg-emerald-400" aria-hidden />
                  <span>{getStrategyDescription('current_buildable')}</span>
                </div>
              </div>
              <div>
                <p className="mb-1 font-medium uppercase tracking-wider text-[9px] text-sky-300">
                  {getStrategyLabel('future_buildable')}
                </p>
                <div className="flex items-center gap-2 text-gray-400">
                  <span className="h-2.5 w-2.5 rounded-sm bg-sky-400" aria-hidden />
                  <span>
                    {futureBuildabilityEnabled
                      ? getStrategyDescription('future_buildable')
                      : 'wyłączone feature flagą w tym środowisku'}
                  </span>
                </div>
              </div>
            </div>
            {futureBuildabilityEnabled && (
              <div className="mt-3 border-t border-gray-800 pt-2">
                <p className="mb-1 font-medium uppercase tracking-wider text-[9px] text-gray-500">
                  Confidence bands
                </p>
                {[
                  { color: '#fbbf24', band: 'formal' as const },
                  { color: '#f97316', band: 'supported' as const },
                  { color: '#a78bfa', band: 'speculative' as const },
                ].map(({ color, band }) => (
                  <div key={band} className="mb-0.5 flex items-center gap-2">
                    <span
                      className="h-2.5 w-2.5 flex-shrink-0 rounded-sm"
                      style={{ backgroundColor: color }}
                      aria-hidden
                    />
                    <span className="font-mono">
                      {getConfidenceBandLabel(band)} · {getConfidenceBandDescription(band)}
                    </span>
                  </div>
                ))}
              </div>
            )}
          </div>
        </main>
      </div>
    </div>
  );
}
