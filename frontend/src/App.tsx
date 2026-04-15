/**
 * TerraZoning — investor-first control room.
 *
 * Layout:
 *   - sticky investor control bar in the left sidebar
 *   - results workspace as the default mode
 *   - desktop detail drawer on the right, preserving the list
 *   - full-screen map as the decision surface
 */

import { lazy, Suspense, useEffect, useMemo, useRef, useState } from 'react';
import {
  Activity,
  BellRing,
  ChevronDown,
  ChevronUp,
  RefreshCw,
  Search,
  SlidersHorizontal,
  Star,
  Layers3,
  ClipboardList,
  X,
  ArrowRight,
} from 'lucide-react';

import { useLeads } from './hooks/useLeads';
import { useQuarantineParcels } from './hooks/useQuarantineParcels';
import { useMapStore } from './store/mapStore';
import type {
  ConfidenceBandFilterKey,
  LeadSortKey,
  LeadStatusFilterKey,
  NumericRangeFilter,
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
  formatAreaCompact,
  formatCurrencyPln,
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

type InvestorPreset = 'default' | 'future_safe' | 'budget_300k' | 'small_entry' | 'large_plot' | 'quick_flip';

interface FilterChip {
  key: string;
  label: string;
  onClear: () => void;
}

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
  totalPriceRange: NumericRangeFilter;
  onTotalPriceRangeChange: (range: NumericRangeFilter) => void;
  pricePerM2Range: NumericRangeFilter;
  onPricePerM2RangeChange: (range: NumericRangeFilter) => void;
  areaRange: NumericRangeFilter;
  onAreaRangeChange: (range: NumericRangeFilter) => void;
  minCoveragePct: number | null;
  onMinCoveragePctChange: (value: number | null) => void;
  minBuildableAreaM2: number | null;
  onMinBuildableAreaM2Change: (value: number | null) => void;
  designationFilter: string;
  onDesignationFilterChange: (value: string) => void;
  searchQuery: string;
  onSearchQueryChange: (value: string) => void;
  terytPrefix: string;
  onTerytPrefixChange: (value: string) => void;
  terytGmina: string;
  onTerytGminaChange: (value: string) => void;
  showAdvancedFilters: boolean;
  onShowAdvancedFiltersChange: (value: boolean) => void;
  futureBuildabilityEnabled: boolean;
  onApplyPreset: (preset: InvestorPreset) => void;
  onResetFilters: () => void;
  activeFilterCount: number;
  count: number;
  isFetching: boolean;
  onRefresh: () => void;
}

function parseNullableNumber(value: string): number | null {
  const trimmed = value.trim().replace(',', '.');
  if (trimmed === '') return null;
  const parsed = Number(trimmed);
  return Number.isFinite(parsed) ? parsed : null;
}

function RangeField({
  label,
  minValue,
  maxValue,
  onChange,
  minPlaceholder,
  maxPlaceholder,
  suffix,
}: {
  label: string;
  minValue: number | null;
  maxValue: number | null;
  onChange: (range: NumericRangeFilter) => void;
  minPlaceholder: string;
  maxPlaceholder: string;
  suffix?: string;
}) {
  return (
    <div className="rounded-2xl border border-gray-800 bg-gray-950/80 px-3 py-3">
      <label className="text-[10px] font-semibold uppercase tracking-[0.18em] text-gray-500">
        {label}
      </label>
      <div className="mt-2 grid grid-cols-2 gap-2">
        <div className="space-y-1">
          <span className="block text-[10px] uppercase tracking-[0.16em] text-gray-600">od</span>
          <input
            type="number"
            inputMode="decimal"
            value={minValue ?? ''}
            onChange={(event) => onChange({ min: parseNullableNumber(event.target.value), max: maxValue })}
            placeholder={minPlaceholder}
            className="w-full rounded-xl border border-gray-800 bg-gray-900 px-3 py-2 text-[12px] text-gray-200 placeholder:text-gray-600 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-amber-500"
            aria-label={`${label} od${suffix ? ` ${suffix}` : ''}`}
          />
        </div>
        <div className="space-y-1">
          <span className="block text-[10px] uppercase tracking-[0.16em] text-gray-600">do</span>
          <input
            type="number"
            inputMode="decimal"
            value={maxValue ?? ''}
            onChange={(event) => onChange({ min: minValue, max: parseNullableNumber(event.target.value) })}
            placeholder={maxPlaceholder}
            className="w-full rounded-xl border border-gray-800 bg-gray-900 px-3 py-2 text-[12px] text-gray-200 placeholder:text-gray-600 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-amber-500"
            aria-label={`${label} do${suffix ? ` ${suffix}` : ''}`}
          />
        </div>
      </div>
      {suffix && (
        <div className="mt-2 text-[10px] text-gray-600">{suffix}</div>
      )}
    </div>
  );
}

function SelectField<T extends string>({
  label,
  value,
  onChange,
  options,
}: {
  label: string;
  value: T;
  onChange: (value: T) => void;
  options: Array<{ value: T; label: string }>;
}) {
  return (
    <div className="flex flex-col gap-1">
      <label className="text-[10px] font-semibold uppercase tracking-[0.18em] text-gray-500">
        {label}
      </label>
      <select
        value={value}
        onChange={(event) => onChange(event.target.value as T)}
        className="w-full rounded-xl border border-gray-800 bg-gray-950 px-3 py-2 text-[12px] text-gray-200 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-amber-500"
      >
        {options.map((option) => (
          <option key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
      </select>
    </div>
  );
}

function ActiveFilterChip({ chip }: { chip: FilterChip }) {
  return (
    <button
      type="button"
      onClick={chip.onClear}
      className="inline-flex items-center gap-1 rounded-full border border-gray-700 bg-gray-950 px-2.5 py-1 text-[10px] text-gray-300 transition-colors hover:border-gray-600 hover:bg-gray-900 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-amber-500"
      title="Usuń filtr"
    >
      <span>{chip.label}</span>
      <X size={10} aria-hidden />
    </button>
  );
}

function WorkspaceTabButton({
  icon,
  label,
  count,
  isActive,
  onClick,
}: {
  icon: React.ReactNode;
  label: string;
  count: number;
  isActive: boolean;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={[
        'flex min-w-0 items-center justify-between gap-2 rounded-xl border px-3 py-2 text-left transition-colors',
        isActive
          ? 'border-amber-500/30 bg-amber-500/10 text-gray-100'
          : 'border-gray-800 bg-gray-950/70 text-gray-400 hover:border-gray-700 hover:bg-gray-900',
      ].join(' ')}
    >
      <span className="flex items-center gap-2 truncate text-[11px] font-medium">
        <span className={isActive ? 'text-amber-300' : 'text-gray-500'}>{icon}</span>
        {label}
      </span>
      <span className="rounded-full border border-gray-700 bg-gray-900 px-2 py-0.5 text-[10px] font-medium text-gray-300">
        {count}
      </span>
    </button>
  );
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
  totalPriceRange,
  onTotalPriceRangeChange,
  pricePerM2Range,
  onPricePerM2RangeChange,
  areaRange,
  onAreaRangeChange,
  minCoveragePct,
  onMinCoveragePctChange,
  minBuildableAreaM2,
  onMinBuildableAreaM2Change,
  designationFilter,
  onDesignationFilterChange,
  searchQuery,
  onSearchQueryChange,
  terytPrefix,
  onTerytPrefixChange,
  terytGmina,
  onTerytGminaChange,
  showAdvancedFilters,
  onShowAdvancedFiltersChange,
  futureBuildabilityEnabled,
  onApplyPreset,
  onResetFilters,
  activeFilterCount,
  count,
  isFetching,
  onRefresh,
}: FilterBarProps) {
  const [isCollapsed, setIsCollapsed] = useState(false);
  const [showMetricFilters, setShowMetricFilters] = useState(false);
  const futureSafeMode = futureBuildabilityEnabled && strategyFilter === 'future_buildable' && confidenceBandFilter === 'all';

  return (
    <div className="sticky top-0 z-10 border-b border-gray-800 bg-gray-900/95 px-4 py-4 backdrop-blur-md">
      <div className="flex items-start gap-3">
        <div className="flex min-w-0 flex-1 items-start gap-2">
          <SlidersHorizontal size={12} className="mt-0.5 text-gray-500" aria-hidden />
          <div className="min-w-0">
            <p className="text-[10px] font-semibold uppercase tracking-[0.22em] text-gray-500">
              Investor control bar
            </p>
            <p className="mt-1 text-[11px] text-gray-500">
              Ustaw budżet, strategię i pewność, a potem zawęź decyzję w jednym miejscu.
            </p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <span className="rounded-full border border-amber-500/20 bg-amber-500/10 px-2 py-0.5 text-[10px] font-medium text-amber-300">
            {activeFilterCount} aktywne
          </span>
          <button
            type="button"
            onClick={() => setIsCollapsed((value) => !value)}
            className="rounded-lg border border-gray-700 bg-gray-950 px-2.5 py-1.5 text-[11px] text-gray-300 transition-colors hover:border-gray-600 hover:bg-gray-900 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-amber-500"
          >
            {isCollapsed ? 'rozwiń' : 'zwiń'}
          </button>
          <button
            type="button"
            onClick={onResetFilters}
            className="rounded-lg border border-gray-700 bg-gray-950 px-2.5 py-1.5 text-[11px] text-gray-300 transition-colors hover:border-gray-600 hover:bg-gray-900 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-amber-500"
          >
            clear all
          </button>
          <button
            type="button"
            onClick={onRefresh}
            disabled={isFetching}
            className="rounded-lg border border-gray-700 bg-gray-950 p-2 text-gray-400 transition-colors hover:border-gray-600 hover:bg-gray-900 hover:text-gray-100 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-amber-500 disabled:opacity-40"
            aria-label="Odśwież dane"
          >
            <RefreshCw size={12} className={isFetching ? 'animate-spin' : ''} aria-hidden />
          </button>
        </div>
      </div>

      {isCollapsed ? (
        <div className="mt-3 flex flex-wrap items-center gap-2">
          <span className="rounded-full border border-gray-800 bg-gray-950/80 px-3 py-1 text-[10px] text-gray-500">
            {count} wyników po filtrach
          </span>
          <span className="rounded-full border border-gray-800 bg-gray-950/80 px-3 py-1 text-[10px] text-gray-500">
            {strategyFilter === 'all' ? 'wszystkie strategie' : strategyFilter === 'future_buildable' ? 'przyszłe budowlane' : 'aktualnie budowlane'}
          </span>
          <span className="rounded-full border border-gray-800 bg-gray-950/80 px-3 py-1 text-[10px] text-gray-500">
            {sortKey === 'investment_score_desc'
              ? 'okazje inwestorskie'
              : sortKey === 'total_price_asc'
                ? 'cena całkowita ↑'
                : sortKey === 'total_price_desc'
                  ? 'cena całkowita ↓'
                  : sortKey === 'price_per_m2_asc'
                    ? 'zł/m² ↑'
                    : sortKey === 'entry_price_asc'
                      ? 'najtańsze wejście ↑'
                      : sortKey === 'area_desc'
                        ? 'powierzchnia ↓'
                        : sortKey === 'buildable_area_desc'
                          ? 'pow. budowlana ↓'
                          : 'pewność ↓'}
          </span>
          {cheapOnly && (
            <span className="rounded-full border border-emerald-500/20 bg-emerald-500/10 px-3 py-1 text-[10px] text-emerald-300">
              cheap only
            </span>
          )}
        </div>
      ) : (
        <>
      <div className="mt-4 flex flex-wrap items-center gap-2">
        <button
          type="button"
          onClick={() => onApplyPreset('future_safe')}
          className="rounded-full border border-sky-500/20 bg-sky-500/10 px-3 py-1.5 text-[11px] font-medium text-sky-200 transition-colors hover:border-sky-400/40 hover:bg-sky-500/15"
        >
          Future safe
        </button>
        <button
          type="button"
          onClick={() => onApplyPreset('budget_300k')}
          className="rounded-full border border-gray-700 bg-gray-950 px-3 py-1.5 text-[11px] text-gray-300 transition-colors hover:border-gray-600 hover:bg-gray-900"
        >
          Budżet do 300k
        </button>
        <button
          type="button"
          onClick={() => onApplyPreset('small_entry')}
          className="rounded-full border border-gray-700 bg-gray-950 px-3 py-1.5 text-[11px] text-gray-300 transition-colors hover:border-gray-600 hover:bg-gray-900"
        >
          Małe wejście
        </button>
        <button
          type="button"
          onClick={() => onApplyPreset('large_plot')}
          className="rounded-full border border-gray-700 bg-gray-950 px-3 py-1.5 text-[11px] text-gray-300 transition-colors hover:border-gray-600 hover:bg-gray-900"
        >
          Duża działka
        </button>
        <button
          type="button"
          onClick={() => onApplyPreset('quick_flip')}
          className="rounded-full border border-gray-700 bg-gray-950 px-3 py-1.5 text-[11px] text-gray-300 transition-colors hover:border-gray-600 hover:bg-gray-900"
        >
          Szybki flip
        </button>
        <div className="ml-auto rounded-full border border-gray-800 bg-gray-950/80 px-3 py-1 text-[10px] text-gray-500">
          {count} wyników po filtrach
        </div>
      </div>

      <div className="mt-4 grid grid-cols-1 gap-3 sm:grid-cols-2">
        <SelectField
          label="Strategia"
          value={strategyFilter}
          onChange={onStrategyFilterChange}
          options={[
            { value: 'all', label: 'wszystkie' },
            { value: 'current_buildable', label: 'aktualnie budowlane' },
            ...(futureBuildabilityEnabled
              ? [{ value: 'future_buildable' as const, label: 'przyszłe budowlane' }]
              : []),
          ]}
        />
        <SelectField
          label="Pewność"
          value={confidenceBandFilter}
          onChange={onConfidenceBandFilterChange}
          options={[
            { value: 'all', label: futureSafeMode ? 'wszystkie (bez speculative)' : 'wszystkie' },
            { value: 'formal', label: 'formalne' },
            { value: 'supported', label: 'wspierane' },
            { value: 'speculative', label: 'spekulacyjne' },
          ]}
        />
        <SelectField
          label="Status"
          value={statusFilter}
          onChange={onStatusFilterChange}
          options={[
            { value: 'active', label: 'aktywne' },
            { value: 'new', label: 'new' },
            { value: 'reviewed', label: 'reviewed' },
            { value: 'shortlisted', label: 'shortlisted' },
            { value: 'rejected', label: 'rejected' },
            { value: 'invested', label: 'invested' },
          ]}
        />
        <SelectField
          label="Sortowanie"
          value={sortKey}
          onChange={onSortChange}
          options={[
            { value: 'investment_score_desc', label: 'okazje inwestorskie' },
            { value: 'total_price_asc', label: 'cena całkowita ↑' },
            { value: 'total_price_desc', label: 'cena całkowita ↓' },
            { value: 'price_per_m2_asc', label: 'zł/m² ↑' },
            { value: 'entry_price_asc', label: 'najtańsze wejście ↑' },
            { value: 'area_desc', label: 'powierzchnia ↓' },
            { value: 'buildable_area_desc', label: 'pow. budowlana ↓' },
            { value: 'confidence_desc', label: 'pewność ↓' },
          ]}
        />
        <div className="rounded-2xl border border-gray-800 bg-gray-950/80 px-3 py-3 sm:col-span-2">
          <p className="text-[10px] font-semibold uppercase tracking-[0.18em] text-gray-500">
            Tryb inwestora
          </p>
          <div className="mt-3 flex flex-wrap items-center gap-2">
            <label className="inline-flex items-center gap-2 rounded-full border border-gray-800 bg-gray-900 px-3 py-1.5 text-[11px] text-gray-300">
              <input
                type="checkbox"
                checked={cheapOnly}
                onChange={(event) => onCheapOnlyChange(event.target.checked)}
                className="accent-amber-500"
              />
              cheap only
            </label>
            <span className="rounded-full border border-emerald-500/20 bg-emerald-500/10 px-3 py-1.5 text-[11px] text-emerald-200">
              {getStrategyLabel(strategyFilter === 'all' ? 'current_buildable' : strategyFilter === 'future_buildable' ? 'future_buildable' : 'current_buildable')}
            </span>
            {futureSafeMode && (
              <span className="rounded-full border border-sky-500/20 bg-sky-500/10 px-3 py-1.5 text-[11px] text-sky-200">
                safe mode
              </span>
            )}
          </div>
          <p className="mt-2 text-[11px] text-gray-500">
            {strategyFilter === 'future_buildable'
              ? getStrategyDescription('future_buildable')
              : strategyFilter === 'current_buildable'
                ? getStrategyDescription('current_buildable')
                : 'porównuj oba strumienie bez mieszania ich logiki'}
          </p>
        </div>
      </div>

      <button
        type="button"
        onClick={() => setShowMetricFilters((value) => !value)}
        className="mt-3 flex w-full items-center justify-between rounded-2xl border border-gray-800 bg-gray-950/80 px-3 py-2 text-left text-[11px] text-gray-300 transition-colors hover:border-gray-700 hover:bg-gray-900"
      >
        <div>
          <div className="font-medium text-gray-100">Budżet i metryki</div>
          <div className="mt-1 text-gray-500">
            Cena całkowita, zł/m² i powierzchnia w wygodnym, zwijanym bloku.
          </div>
        </div>
        {showMetricFilters ? <ChevronUp size={14} aria-hidden /> : <ChevronDown size={14} aria-hidden />}
      </button>

      {showMetricFilters && (
        <div className="mt-3 grid grid-cols-1 gap-3">
          <RangeField
            label="Cena całkowita"
            minValue={totalPriceRange.min}
            maxValue={totalPriceRange.max}
            onChange={onTotalPriceRangeChange}
            minPlaceholder="np. 50000"
            maxPlaceholder="np. 300000"
            suffix="PLN"
          />
          <RangeField
            label="zł / m²"
            minValue={pricePerM2Range.min}
            maxValue={pricePerM2Range.max}
            onChange={onPricePerM2RangeChange}
            minPlaceholder="np. 40"
            maxPlaceholder="np. 300"
            suffix="PLN / m²"
          />
          <RangeField
            label="Powierzchnia"
            minValue={areaRange.min}
            maxValue={areaRange.max}
            onChange={onAreaRangeChange}
            minPlaceholder="np. 800"
            maxPlaceholder="np. 3000"
            suffix="m²"
          />
        </div>
      )}

      <button
        type="button"
        onClick={() => onShowAdvancedFiltersChange(!showAdvancedFilters)}
        className="mt-3 flex w-full items-center justify-between rounded-2xl border border-gray-800 bg-gray-950/80 px-3 py-2 text-left text-[11px] text-gray-300 transition-colors hover:border-gray-700 hover:bg-gray-900"
      >
        <div>
          <div className="font-medium text-gray-100">Advanced filters</div>
          <div className="mt-1 text-gray-500">
            Pokrycie, buildable area, województwo, TERYT, search i jakość ceny.
          </div>
        </div>
        {showAdvancedFilters ? <ChevronUp size={14} aria-hidden /> : <ChevronDown size={14} aria-hidden />}
      </button>

      {showAdvancedFilters && (
        <div className="mt-3 grid grid-cols-1 gap-3">
          <div className="rounded-2xl border border-gray-800 bg-gray-950/80 px-3 py-3">
            <div className="flex items-center justify-between gap-3">
              <label htmlFor="min-score-range" className="text-[10px] font-semibold uppercase tracking-[0.18em] text-gray-500">
                min score
              </label>
              <span className="font-mono text-[11px] text-amber-300">{Math.round(minScore * 100)}%</span>
            </div>
            <input
              id="min-score-range"
              type="range"
              min={0}
              max={1}
              step={0.05}
              value={minScore}
              onChange={(event) => onMinScoreChange(parseFloat(event.target.value))}
              className="mt-3 h-1.5 w-full cursor-pointer accent-amber-500"
            />
          </div>
          <div className="grid grid-cols-2 gap-3">
            <div className="rounded-2xl border border-gray-800 bg-gray-950/80 px-3 py-3">
              <label className="text-[10px] font-semibold uppercase tracking-[0.18em] text-gray-500">
                min pokrycie %
              </label>
              <input
                type="number"
                inputMode="decimal"
                value={minCoveragePct ?? ''}
                onChange={(event) => onMinCoveragePctChange(parseNullableNumber(event.target.value))}
                placeholder="np. 30"
                className="mt-2 w-full rounded-xl border border-gray-800 bg-gray-900 px-3 py-2 text-[12px] text-gray-200 placeholder:text-gray-600 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-amber-500"
              />
            </div>
            <div className="rounded-2xl border border-gray-800 bg-gray-950/80 px-3 py-3">
              <label className="text-[10px] font-semibold uppercase tracking-[0.18em] text-gray-500">
                min pow. budowlana
              </label>
              <input
                type="number"
                inputMode="decimal"
                value={minBuildableAreaM2 ?? ''}
                onChange={(event) => onMinBuildableAreaM2Change(parseNullableNumber(event.target.value))}
                placeholder="np. 500"
                className="mt-2 w-full rounded-xl border border-gray-800 bg-gray-900 px-3 py-2 text-[12px] text-gray-200 placeholder:text-gray-600 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-amber-500"
              />
            </div>
          </div>
          <SelectField
            label="Jakość ceny"
            value={priceFilter}
            onChange={onPriceFilterChange}
            options={[
              { value: 'all', label: 'wszystkie' },
              { value: 'reliable', label: 'wiarygodne' },
              { value: 'suspicious', label: 'do weryfikacji' },
              { value: 'missing', label: 'brak ceny' },
            ]}
          />
          <div className="rounded-2xl border border-gray-800 bg-gray-950/80 px-3 py-3">
            <label className="text-[10px] font-semibold uppercase tracking-[0.18em] text-gray-500">
              Województwo
            </label>
            <select
              value={terytPrefix}
              onChange={(event) => onTerytPrefixChange(event.target.value)}
              className="mt-2 w-full rounded-xl border border-gray-800 bg-gray-900 px-3 py-2 text-[12px] text-gray-200 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-amber-500"
            >
              <option value="">wszystkie</option>
              <option value="12">Małopolskie</option>
              <option value="24">Śląskie</option>
            </select>
          </div>
          <div className="rounded-2xl border border-gray-800 bg-gray-950/80 px-3 py-3">
            <label className="text-[10px] font-semibold uppercase tracking-[0.18em] text-gray-500">
              TERYT gminy
            </label>
            <input
              type="text"
              value={terytGmina}
              onChange={(event) => onTerytGminaChange(event.target.value)}
              placeholder="np. 2404042"
              className="mt-2 w-full rounded-xl border border-gray-800 bg-gray-900 px-3 py-2 text-[12px] text-gray-200 placeholder:text-gray-600 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-amber-500"
            />
          </div>
          <div className="rounded-2xl border border-gray-800 bg-gray-950/80 px-3 py-3">
            <label className="text-[10px] font-semibold uppercase tracking-[0.18em] text-gray-500">
              Przeznaczenie / sygnał
            </label>
            <input
              type="text"
              value={designationFilter}
              onChange={(event) => onDesignationFilterChange(event.target.value)}
              placeholder="np. mixed_residential, MN, pog_zone"
              className="mt-2 w-full rounded-xl border border-gray-800 bg-gray-900 px-3 py-2 text-[12px] text-gray-200 placeholder:text-gray-600 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-amber-500"
            />
          </div>
          <div className="rounded-2xl border border-gray-800 bg-gray-950/80 px-3 py-3">
            <label className="text-[10px] font-semibold uppercase tracking-[0.18em] text-gray-500">
              Search
            </label>
            <div className="mt-2 flex items-center gap-2 rounded-xl border border-gray-800 bg-gray-900 px-3 py-2">
              <Search size={13} className="text-gray-500" aria-hidden />
              <input
                type="text"
                value={searchQuery}
                onChange={(event) => onSearchQueryChange(event.target.value)}
                placeholder="identyfikator działki, source URL, notes, dominant future signal…"
                className="w-full bg-transparent text-[12px] text-gray-200 placeholder:text-gray-600 focus-visible:outline-none"
              />
            </div>
          </div>
        </div>
      )}
        </>
      )}
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
          Brak wyników na mapie
        </p>
        <p className="mt-1 text-sm text-gray-300">
          Bieżące filtry nie zwracają żadnych leadów.
        </p>
        <p className="mt-1 text-xs text-gray-500">
          Zmień strategię, obniż min score, podnieś budżet albo wyłącz `cheap only`.
        </p>
      </div>
    </div>
  );
}

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
  const totalPriceRange = useMapStore((s) => s.totalPriceRange);
  const setTotalPriceRange = useMapStore((s) => s.setTotalPriceRange);
  const pricePerM2Range = useMapStore((s) => s.pricePerM2Range);
  const setPricePerM2Range = useMapStore((s) => s.setPricePerM2Range);
  const areaRange = useMapStore((s) => s.areaRange);
  const setAreaRange = useMapStore((s) => s.setAreaRange);
  const minCoveragePct = useMapStore((s) => s.minCoveragePct);
  const setMinCoveragePct = useMapStore((s) => s.setMinCoveragePct);
  const minBuildableAreaM2 = useMapStore((s) => s.minBuildableAreaM2);
  const setMinBuildableAreaM2 = useMapStore((s) => s.setMinBuildableAreaM2);
  const designationFilter = useMapStore((s) => s.designationFilter);
  const setDesignationFilter = useMapStore((s) => s.setDesignationFilter);
  const searchQuery = useMapStore((s) => s.searchQuery);
  const setSearchQuery = useMapStore((s) => s.setSearchQuery);
  const terytPrefix = useMapStore((s) => s.terytPrefix);
  const setTerytPrefix = useMapStore((s) => s.setTerytPrefix);
  const terytGmina = useMapStore((s) => s.terytGmina);
  const setTerytGmina = useMapStore((s) => s.setTerytGmina);
  const showAdvancedFilters = useMapStore((s) => s.showAdvancedFilters);
  const setShowAdvancedFilters = useMapStore((s) => s.setShowAdvancedFilters);
  const workspaceView = useMapStore((s) => s.workspaceView);
  const setWorkspaceView = useMapStore((s) => s.setWorkspaceView);
  const previousStrategyFilterRef = useRef<StrategyFilterKey | null>(null);
  const hydratedFromUrlRef = useRef(false);
  const [mapFocusNonce, setMapFocusNonce] = useState(0);

  const queryParams = useMemo(
    () => ({
      min_score: minScore,
      limit: 100,
      ...(statusFilter !== 'active' ? { status_filter: statusFilter } : {}),
      ...(strategyFilter !== 'all' ? { strategy_filter: strategyFilter } : {}),
      ...(confidenceBandFilter !== 'all' ? { confidence_band_filter: confidenceBandFilter } : {}),
      ...(cheapOnly ? { cheap_only: true } : {}),
      ...(totalPriceRange.min != null ? { min_price_zl: totalPriceRange.min } : {}),
      ...(totalPriceRange.max != null ? { max_price_zl: totalPriceRange.max } : {}),
      ...(pricePerM2Range.min != null ? { min_price_per_m2_zl: pricePerM2Range.min } : {}),
      ...(pricePerM2Range.max != null ? { max_price_per_m2_zl: pricePerM2Range.max } : {}),
      ...(areaRange.min != null ? { min_area_m2: areaRange.min } : {}),
      ...(areaRange.max != null ? { max_area_m2: areaRange.max } : {}),
      ...(minCoveragePct != null ? { min_coverage_pct: minCoveragePct } : {}),
      ...(minBuildableAreaM2 != null ? { min_buildable_area_m2: minBuildableAreaM2 } : {}),
      ...(terytPrefix ? { teryt_prefix: terytPrefix } : {}),
      ...(terytGmina ? { teryt_gmina: terytGmina } : {}),
      ...(designationFilter.trim() ? { designation: designationFilter.trim() } : {}),
      ...(searchQuery.trim() ? { search: searchQuery.trim() } : {}),
    }),
    [
      areaRange.max,
      areaRange.min,
      cheapOnly,
      confidenceBandFilter,
      designationFilter,
      minBuildableAreaM2,
      minCoveragePct,
      minScore,
      pricePerM2Range.max,
      pricePerM2Range.min,
      searchQuery,
      statusFilter,
      strategyFilter,
      terytGmina,
      terytPrefix,
      totalPriceRange.max,
      totalPriceRange.min,
    ],
  );

  const { data, isLoading, isFetching, error, refetch } = useLeads(queryParams);
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

  const selectedFeature = useMemo<LeadFeature | null>(() => {
    if (!selectedLeadId || !visibleData) return null;
    return visibleData.features.find((f) => f.properties.lead_id === selectedLeadId) ?? null;
  }, [selectedLeadId, visibleData]);

  const selectedQuarantineFeature = useMemo<QuarantineParcelFeature | null>(() => {
    if (!selectedQuarantineId || !quarantineData) return null;
    return quarantineData.features.find((f) => f.properties.dzialka_id === selectedQuarantineId) ?? null;
  }, [selectedQuarantineId, quarantineData]);

  const activeFilterCount = [
    minScore !== 0.7,
    leadSort !== 'investment_score_desc',
    priceFilter !== 'reliable',
    statusFilter !== 'active',
    strategyFilter !== 'all',
    confidenceBandFilter !== 'all',
    cheapOnly,
    totalPriceRange.min != null,
    totalPriceRange.max != null,
    pricePerM2Range.min != null,
    pricePerM2Range.max != null,
    areaRange.min != null,
    areaRange.max != null,
    minCoveragePct != null,
    minBuildableAreaM2 != null,
    designationFilter.trim() !== '',
    searchQuery.trim() !== '',
    terytPrefix !== '',
    terytGmina.trim() !== '',
  ].filter(Boolean).length;

  const activeFilterChips = useMemo<FilterChip[]>(() => {
    const chips: FilterChip[] = [];
    if (strategyFilter !== 'all') {
      chips.push({
        key: 'strategy',
        label: getStrategyLabel(strategyFilter),
        onClear: () => setStrategyFilter('all'),
      });
    }
    if (confidenceBandFilter !== 'all') {
      chips.push({
        key: 'confidence',
        label: getConfidenceBandLabel(confidenceBandFilter),
        onClear: () => setConfidenceBandFilter('all'),
      });
    }
    if (cheapOnly) {
      chips.push({
        key: 'cheapOnly',
        label: 'cheap only',
        onClear: () => setCheapOnly(false),
      });
    }
    if (totalPriceRange.min != null || totalPriceRange.max != null) {
      chips.push({
        key: 'totalPrice',
        label: `cena ${formatCurrencyPln(totalPriceRange.min, { compact: true })} → ${formatCurrencyPln(totalPriceRange.max, { compact: true })}`,
        onClear: () => setTotalPriceRange({ min: null, max: null }),
      });
    }
    if (pricePerM2Range.min != null || pricePerM2Range.max != null) {
      chips.push({
        key: 'pricePerM2',
        label: `zł/m² ${pricePerM2Range.min ?? '—'} → ${pricePerM2Range.max ?? '—'}`,
        onClear: () => setPricePerM2Range({ min: null, max: null }),
      });
    }
    if (areaRange.min != null || areaRange.max != null) {
      chips.push({
        key: 'area',
        label: `pow. ${formatAreaCompact(areaRange.min)} → ${formatAreaCompact(areaRange.max)}`,
        onClear: () => setAreaRange({ min: null, max: null }),
      });
    }
    if (minCoveragePct != null) {
      chips.push({
        key: 'coverage',
        label: `pokrycie ≥ ${minCoveragePct}%`,
        onClear: () => setMinCoveragePct(null),
      });
    }
    if (minBuildableAreaM2 != null) {
      chips.push({
        key: 'buildableArea',
        label: `pow. bud. ≥ ${formatAreaCompact(minBuildableAreaM2)}`,
        onClear: () => setMinBuildableAreaM2(null),
      });
    }
    if (terytPrefix) {
      chips.push({
        key: 'province',
        label: terytPrefix === '12' ? 'Małopolskie' : terytPrefix === '24' ? 'Śląskie' : `TERYT ${terytPrefix}`,
        onClear: () => setTerytPrefix(''),
      });
    }
    if (terytGmina.trim()) {
      chips.push({
        key: 'terytGmina',
        label: `gmina ${terytGmina}`,
        onClear: () => setTerytGmina(''),
      });
    }
    if (designationFilter.trim()) {
      chips.push({
        key: 'designation',
        label: `sygnał ${designationFilter.trim()}`,
        onClear: () => setDesignationFilter(''),
      });
    }
    if (searchQuery.trim()) {
      chips.push({
        key: 'search',
        label: `search ${searchQuery.trim()}`,
        onClear: () => setSearchQuery(''),
      });
    }
    if (leadSort !== 'investment_score_desc') {
      const sortLabelMap: Record<LeadSortKey, string> = {
        investment_score_desc: 'okazje inwestorskie',
        confidence_desc: 'pewność ↓',
        total_price_asc: 'cena całkowita ↑',
        total_price_desc: 'cena całkowita ↓',
        price_per_m2_asc: 'zł/m² ↑',
        entry_price_asc: 'najtańsze wejście ↑',
        area_desc: 'powierzchnia ↓',
        buildable_area_desc: 'pow. budowlana ↓',
      };
      chips.push({
        key: 'sort',
        label: `sort ${sortLabelMap[leadSort]}`,
        onClear: () => setLeadSort('investment_score_desc'),
      });
    }
    if (priceFilter !== 'reliable') {
      chips.push({
        key: 'priceFilter',
        label: `cena ${priceFilter}`,
        onClear: () => setPriceFilter('reliable'),
      });
    }
    if (statusFilter !== 'active') {
      chips.push({
        key: 'status',
        label: `status ${statusFilter}`,
        onClear: () => setStatusFilter('active'),
      });
    }
    return chips;
  }, [
    areaRange.max,
    areaRange.min,
    cheapOnly,
    confidenceBandFilter,
    designationFilter,
    minBuildableAreaM2,
    minCoveragePct,
    leadSort,
    priceFilter,
    pricePerM2Range.max,
    pricePerM2Range.min,
    searchQuery,
    setAreaRange,
    setCheapOnly,
    setConfidenceBandFilter,
    setDesignationFilter,
    setLeadSort,
    setMinBuildableAreaM2,
    setMinCoveragePct,
    setPriceFilter,
    setPricePerM2Range,
    setSearchQuery,
    setStatusFilter,
    setStrategyFilter,
    setTerytGmina,
    setTerytPrefix,
    setTotalPriceRange,
    statusFilter,
    strategyFilter,
    terytGmina,
    terytPrefix,
    totalPriceRange.max,
    totalPriceRange.min,
  ]);

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
    if (
      futureBuildabilityEnabled &&
      strategyFilter === 'future_buildable' &&
      previousStrategyFilter !== 'future_buildable' &&
      priceFilter === 'reliable'
    ) {
      setPriceFilter('all');
    }
    previousStrategyFilterRef.current = strategyFilter;
  }, [cheapOnly, futureBuildabilityEnabled, priceFilter, setCheapOnly, setPriceFilter, strategyFilter]);

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

  useEffect(() => {
    if (selectedLeadId && visibleData && !selectedFeature) {
      setSelectedLeadId(null);
    }
  }, [selectedLeadId, selectedFeature, setSelectedLeadId, visibleData]);

  useEffect(() => {
    if (selectedQuarantineId && quarantineData && !selectedQuarantineFeature) {
      setSelectedQuarantineId(null);
    }
  }, [quarantineData, selectedQuarantineFeature, selectedQuarantineId, setSelectedQuarantineId]);

  useEffect(() => {
    if (hydratedFromUrlRef.current || typeof window === 'undefined') return;
    const params = new URLSearchParams(window.location.search);
    const minScoreParam = params.get('min_score');
    const strategyParam = params.get('strategy');
    const confidenceParam = params.get('confidence');
    const statusParam = params.get('status');
    const priceFilterParam = params.get('price_quality');
    const cheapOnlyParam = params.get('cheap_only');
    const minPriceParam = params.get('min_price_zl');
    const maxPriceParam = params.get('max_price_zl');
    const minPricePerM2Param = params.get('min_price_per_m2_zl');
    const maxPricePerM2Param = params.get('max_price_per_m2_zl');
    const minAreaParam = params.get('min_area_m2');
    const maxAreaParam = params.get('max_area_m2');
    const minCoverageParam = params.get('min_coverage_pct');
    const minBuildableAreaParam = params.get('min_buildable_area_m2');
    const designationParam = params.get('designation');
    const searchParam = params.get('search');
    const terytPrefixParam = params.get('teryt_prefix');
    const terytGminaParam = params.get('teryt_gmina');
    const sortParam = params.get('sort');
    const workspaceParam = params.get('workspace');

    if (minScoreParam) setMinScore(Number(minScoreParam));
    if (strategyParam === 'all' || strategyParam === 'current_buildable' || strategyParam === 'future_buildable') {
      setStrategyFilter(strategyParam);
    }
    if (confidenceParam === 'all' || confidenceParam === 'formal' || confidenceParam === 'supported' || confidenceParam === 'speculative') {
      setConfidenceBandFilter(confidenceParam);
    }
    if (statusParam === 'active' || statusParam === 'new' || statusParam === 'reviewed' || statusParam === 'shortlisted' || statusParam === 'rejected' || statusParam === 'invested') {
      setStatusFilter(statusParam);
    }
    if (priceFilterParam === 'all' || priceFilterParam === 'reliable' || priceFilterParam === 'suspicious' || priceFilterParam === 'missing') {
      setPriceFilter(priceFilterParam);
    }
    if (cheapOnlyParam === '1') setCheapOnly(true);
    if (minPriceParam || maxPriceParam) {
      setTotalPriceRange({ min: parseNullableNumber(minPriceParam ?? ''), max: parseNullableNumber(maxPriceParam ?? '') });
    }
    if (minPricePerM2Param || maxPricePerM2Param) {
      setPricePerM2Range({ min: parseNullableNumber(minPricePerM2Param ?? ''), max: parseNullableNumber(maxPricePerM2Param ?? '') });
    }
    if (minAreaParam || maxAreaParam) {
      setAreaRange({ min: parseNullableNumber(minAreaParam ?? ''), max: parseNullableNumber(maxAreaParam ?? '') });
    }
    if (minCoverageParam) setMinCoveragePct(parseNullableNumber(minCoverageParam));
    if (minBuildableAreaParam) setMinBuildableAreaM2(parseNullableNumber(minBuildableAreaParam));
    if (designationParam) setDesignationFilter(designationParam);
    if (searchParam) setSearchQuery(searchParam);
    if (terytPrefixParam) setTerytPrefix(terytPrefixParam);
    if (terytGminaParam) setTerytGmina(terytGminaParam);
    if (
      sortParam === 'investment_score_desc'
      || sortParam === 'confidence_desc'
      || sortParam === 'total_price_asc'
      || sortParam === 'total_price_desc'
      || sortParam === 'price_per_m2_asc'
      || sortParam === 'entry_price_asc'
      || sortParam === 'area_desc'
      || sortParam === 'buildable_area_desc'
    ) {
      setLeadSort(sortParam);
    }
    if (workspaceParam === 'results' || workspaceParam === 'shortlist' || workspaceParam === 'watchlist' || workspaceParam === 'quarantine') {
      setWorkspaceView(workspaceParam);
    }

    hydratedFromUrlRef.current = true;
  }, [
    setAreaRange,
    setCheapOnly,
    setConfidenceBandFilter,
    setDesignationFilter,
    setLeadSort,
    setMinBuildableAreaM2,
    setMinCoveragePct,
    setMinScore,
    setPriceFilter,
    setPricePerM2Range,
    setSearchQuery,
    setStatusFilter,
    setStrategyFilter,
    setTerytGmina,
    setTerytPrefix,
    setTotalPriceRange,
    setWorkspaceView,
  ]);

  useEffect(() => {
    if (!hydratedFromUrlRef.current || typeof window === 'undefined') return;
    const params = new URLSearchParams();
    if (minScore !== 0.7) params.set('min_score', String(minScore));
    if (strategyFilter !== 'all') params.set('strategy', strategyFilter);
    if (confidenceBandFilter !== 'all') params.set('confidence', confidenceBandFilter);
    if (statusFilter !== 'active') params.set('status', statusFilter);
    if (priceFilter !== 'reliable') params.set('price_quality', priceFilter);
    if (cheapOnly) params.set('cheap_only', '1');
    if (totalPriceRange.min != null) params.set('min_price_zl', String(totalPriceRange.min));
    if (totalPriceRange.max != null) params.set('max_price_zl', String(totalPriceRange.max));
    if (pricePerM2Range.min != null) params.set('min_price_per_m2_zl', String(pricePerM2Range.min));
    if (pricePerM2Range.max != null) params.set('max_price_per_m2_zl', String(pricePerM2Range.max));
    if (areaRange.min != null) params.set('min_area_m2', String(areaRange.min));
    if (areaRange.max != null) params.set('max_area_m2', String(areaRange.max));
    if (minCoveragePct != null) params.set('min_coverage_pct', String(minCoveragePct));
    if (minBuildableAreaM2 != null) params.set('min_buildable_area_m2', String(minBuildableAreaM2));
    if (designationFilter.trim()) params.set('designation', designationFilter.trim());
    if (searchQuery.trim()) params.set('search', searchQuery.trim());
    if (terytPrefix) params.set('teryt_prefix', terytPrefix);
    if (terytGmina.trim()) params.set('teryt_gmina', terytGmina.trim());
    if (leadSort !== 'investment_score_desc') params.set('sort', leadSort);
    if (workspaceView !== 'results') params.set('workspace', workspaceView);
    const nextQuery = params.toString();
    const nextUrl = nextQuery ? `${window.location.pathname}?${nextQuery}` : window.location.pathname;
    window.history.replaceState({}, '', nextUrl);
  }, [
    areaRange.max,
    areaRange.min,
    cheapOnly,
    confidenceBandFilter,
    designationFilter,
    leadSort,
    minBuildableAreaM2,
    minCoveragePct,
    minScore,
    priceFilter,
    pricePerM2Range.max,
    pricePerM2Range.min,
    searchQuery,
    statusFilter,
    strategyFilter,
    terytGmina,
    terytPrefix,
    totalPriceRange.max,
    totalPriceRange.min,
    workspaceView,
  ]);

  function resetFilters(): void {
    setStrategyFilter('all');
    setConfidenceBandFilter('all');
    setCheapOnly(false);
    setMinScore(0.7);
    setPriceFilter('reliable');
    setLeadSort('investment_score_desc');
    setStatusFilter('active');
    setTotalPriceRange({ min: null, max: null });
    setPricePerM2Range({ min: null, max: null });
    setAreaRange({ min: null, max: null });
    setMinCoveragePct(null);
    setMinBuildableAreaM2(null);
    setDesignationFilter('');
    setSearchQuery('');
    setTerytPrefix('');
    setTerytGmina('');
    setShowAdvancedFilters(false);
    setWorkspaceView('results');
  }

  function applyPreset(preset: InvestorPreset): void {
    if (preset === 'default') {
      resetFilters();
      return;
    }

    if (preset === 'future_safe') {
      setStrategyFilter('future_buildable');
      setConfidenceBandFilter('all');
      setCheapOnly(true);
      setMinScore(0.5);
      setPriceFilter('all');
      setLeadSort('price_per_m2_asc');
      setStatusFilter('active');
      return;
    }

    if (preset === 'budget_300k') {
      setTotalPriceRange({ min: totalPriceRange.min, max: 300_000 });
      return;
    }

    if (preset === 'small_entry') {
      setTotalPriceRange({ min: null, max: 150_000 });
      setLeadSort('entry_price_asc');
      return;
    }

    if (preset === 'large_plot') {
      setAreaRange({ min: 1_500, max: areaRange.max });
      return;
    }

    if (preset === 'quick_flip') {
      setStrategyFilter('current_buildable');
      setConfidenceBandFilter('all');
      setStatusFilter('active');
      setCheapOnly(false);
      setPriceFilter('reliable');
      setLeadSort('entry_price_asc');
    }
  }

  const resultsCount = visibleLeads.length;
  const shortlistCount = shortlistData?.features.length ?? 0;
  const watchlistCount = watchlistData?.features.length ?? 0;
  const quarantineCount = quarantineData?.features.length ?? 0;

  return (
    <div className="flex h-screen flex-col overflow-hidden bg-[#0d1117] text-gray-100">
      <header className="flex h-11 flex-shrink-0 items-center justify-between border-b border-gray-800 bg-gray-900 px-4">
        <div className="flex items-center gap-2">
          <Activity size={15} className="text-amber-500" aria-hidden />
          <span className="text-sm font-semibold tracking-tight text-gray-100">TerraZoning</span>
          <span className="ml-1 text-xs text-gray-600">— Kokpit Inwestorski</span>
        </div>
        <div className="flex items-center gap-2 text-[10px] text-gray-500">
          <span className="hidden sm:inline">{resultsCount} wyników</span>
          {isFetching && !isLoading && (
            <span className="flex items-center gap-1 text-amber-400">
              <RefreshCw size={9} className="animate-spin" aria-hidden />
              sync
            </span>
          )}
        </div>
      </header>

      <div className="flex flex-1 overflow-hidden">
        <aside className="flex w-[26rem] xl:w-[28rem] 2xl:w-[30rem] flex-shrink-0 flex-col border-r border-gray-800 bg-gray-900" aria-label="Panel inwestora">
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
            totalPriceRange={totalPriceRange}
            onTotalPriceRangeChange={setTotalPriceRange}
            pricePerM2Range={pricePerM2Range}
            onPricePerM2RangeChange={setPricePerM2Range}
            areaRange={areaRange}
            onAreaRangeChange={setAreaRange}
            minCoveragePct={minCoveragePct}
            onMinCoveragePctChange={setMinCoveragePct}
            minBuildableAreaM2={minBuildableAreaM2}
            onMinBuildableAreaM2Change={setMinBuildableAreaM2}
            designationFilter={designationFilter}
            onDesignationFilterChange={setDesignationFilter}
            searchQuery={searchQuery}
            onSearchQueryChange={setSearchQuery}
            terytPrefix={terytPrefix}
            onTerytPrefixChange={setTerytPrefix}
            terytGmina={terytGmina}
            onTerytGminaChange={setTerytGmina}
            showAdvancedFilters={showAdvancedFilters}
            onShowAdvancedFiltersChange={setShowAdvancedFilters}
            futureBuildabilityEnabled={futureBuildabilityEnabled}
            onApplyPreset={applyPreset}
            onResetFilters={resetFilters}
            activeFilterCount={activeFilterCount}
            count={resultsCount}
            isFetching={isFetching}
            onRefresh={() => {
              void refetch();
              void refetchQuarantine();
            }}
          />

          <div className="min-h-0 flex-1 overflow-y-auto overscroll-y-contain pb-8">
            <InvestorSnapshot features={visibleLeads} />

            {futureBuildabilityEnabled && strategyFilter === 'future_buildable' && confidenceBandFilter === 'all' && (
              <div className="border-b border-gray-800 px-4 py-3">
                <div className="rounded-2xl border border-sky-500/20 bg-sky-500/10 px-3 py-3 text-[11px] text-sky-200">
                  Safe mode aktywny: pokazuję `formal` + `supported`, a `speculative` ukrywam do czasu świadomego wyboru.
                </div>
              </div>
            )}

            <div className="border-b border-gray-800 px-4 py-4">
              <div className="grid grid-cols-2 gap-2">
                <WorkspaceTabButton
                  icon={<Layers3 size={13} aria-hidden />}
                  label="Wyniki"
                  count={resultsCount}
                  isActive={workspaceView === 'results'}
                  onClick={() => setWorkspaceView('results')}
                />
                <WorkspaceTabButton
                  icon={<Star size={13} aria-hidden />}
                  label="Shortlista"
                  count={shortlistCount}
                  isActive={workspaceView === 'shortlist'}
                  onClick={() => setWorkspaceView('shortlist')}
                />
                <WorkspaceTabButton
                  icon={<BellRing size={13} aria-hidden />}
                  label="Watchlista"
                  count={watchlistCount}
                  isActive={workspaceView === 'watchlist'}
                  onClick={() => setWorkspaceView('watchlist')}
                />
                <WorkspaceTabButton
                  icon={<ClipboardList size={13} aria-hidden />}
                  label="Kwarantanna"
                  count={quarantineCount}
                  isActive={workspaceView === 'quarantine'}
                  onClick={() => setWorkspaceView('quarantine')}
                />
              </div>
            </div>

            {workspaceView === 'results' && (
              <>
                <div className="sticky top-0 z-[1] border-b border-gray-800 bg-gray-900/95 px-4 py-3 backdrop-blur-sm">
                  <div className="flex items-start justify-between gap-3">
                    <div className="min-w-0">
                      <p className="text-[10px] font-semibold uppercase tracking-[0.18em] text-gray-500">
                        Results workspace
                      </p>
                      <p className="mt-1 text-[11px] text-gray-500">
                        Najpierw budżet i strategia, potem szybkie porównanie okazji bez wychodzenia z listy.
                      </p>
                    </div>
                    <button
                      type="button"
                      onClick={() => {
                        setSelectedLeadId(null);
                        setSelectedQuarantineId(null);
                        setMapFocusNonce((current) => current + 1);
                      }}
                      className="inline-flex items-center gap-1 rounded-lg border border-gray-700 bg-gray-950 px-2.5 py-1.5 text-[11px] text-gray-300 transition-colors hover:border-gray-600 hover:bg-gray-900"
                    >
                      focus map
                      <ArrowRight size={11} aria-hidden />
                    </button>
                  </div>
                  {activeFilterChips.length > 0 && (
                    <div className="mt-3 flex flex-wrap gap-2">
                      {activeFilterChips.map((chip) => (
                        <ActiveFilterChip key={chip.key} chip={chip} />
                      ))}
                    </div>
                  )}
                </div>
                <LeadList
                  features={visibleLeads}
                  isLoading={isLoading}
                  error={error}
                />
              </>
            )}

            {workspaceView === 'shortlist' && (
              <ShortlistSection
                features={shortlistData?.features ?? []}
                isLoading={isShortlistLoading}
                error={shortlistError}
              />
            )}

            {workspaceView === 'watchlist' && (
              <WatchlistSection
                features={watchlistData?.features ?? []}
                isLoading={isWatchlistLoading}
                error={watchlistError}
              />
            )}

            {workspaceView === 'quarantine' && (
              <QuarantineList
                features={quarantineData?.features ?? []}
                isLoading={isQuarantineLoading}
                error={quarantineError}
              />
            )}
          </div>
        </aside>

        <main className="relative flex-1" aria-label="Mapa okazji inwestycyjnych">
          {(!visibleData || visibleData.features.length === 0) && <MapEmptyState />}
          <Suspense fallback={<MapLoadingFallback />}>
            <LeadsMap
              data={visibleData}
              quarantineData={selectedQuarantineFeature ? quarantineData : undefined}
              focusResultsNonce={mapFocusNonce}
            />
          </Suspense>

          <div className="absolute bottom-8 left-4 z-10 w-[312px] rounded-2xl border border-gray-700 bg-gray-900/92 px-4 py-3 text-[10px] text-gray-400 shadow-2xl shadow-black/20 backdrop-blur-sm">
            <div className="flex items-center justify-between gap-2">
              <p className="font-medium uppercase tracking-wider text-[9px] text-gray-500">
                Legenda mapy
              </p>
              <p className="text-[9px] text-gray-600">current_buildable vs future_buildable</p>
            </div>
            <p className="mt-1 text-[9px] leading-relaxed text-gray-500">
              W małym zoomie punkty grupują się w klastry. Po zbliżeniu mapa pokazuje aktywne okazje zgodnie z bieżącymi filtrami.
            </p>
            <div className="mt-3 grid gap-3">
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
              <div className="mt-3 border-t border-gray-800 pt-3">
                <p className="mb-1 font-medium uppercase tracking-wider text-[9px] text-gray-500">
                  Confidence bands
                </p>
                {[
                  { color: '#fbbf24', band: 'formal' as const },
                  { color: '#f97316', band: 'supported' as const },
                  { color: '#a78bfa', band: 'speculative' as const },
                ].map(({ color, band }) => (
                  <div key={band} className="mb-0.5 flex items-center gap-2">
                    <span className="h-2.5 w-2.5 flex-shrink-0 rounded-sm" style={{ backgroundColor: color }} aria-hidden />
                    <span className="font-mono">
                      {getConfidenceBandLabel(band)} · {getConfidenceBandDescription(band)}
                    </span>
                  </div>
                ))}
              </div>
            )}
          </div>

          {(selectedFeature || selectedQuarantineFeature) && (
            <div className="pointer-events-none absolute inset-y-0 right-0 z-20 flex w-full justify-end">
              <aside className="pointer-events-auto h-full w-full border-l border-gray-800 bg-gray-900 shadow-2xl shadow-black/40 sm:w-[26rem] xl:w-[31rem]">
                {selectedFeature ? (
                  <LeadDetail feature={selectedFeature} onBack={() => setSelectedLeadId(null)} />
                ) : selectedQuarantineFeature ? (
                  <QuarantineDetail feature={selectedQuarantineFeature} onBack={() => setSelectedQuarantineId(null)} />
                ) : null}
              </aside>
            </div>
          )}
        </main>
      </div>
    </div>
  );
}
