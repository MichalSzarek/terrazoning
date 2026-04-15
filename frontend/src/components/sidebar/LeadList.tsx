/**
 * Sidebar lead list — sorted by confidence_score DESC (matches API order).
 * Clicking a row: selects the lead, map highlights the parcel, detail view opens.
 */

import { MapPin, TrendingUp, AlertCircle, ArrowRight } from 'lucide-react';
import type { LeadFeature } from '../../types/api';
import { ConfidenceBadge } from '../ui/ConfidenceBadge';
import { useMapStore } from '../../store/mapStore';
import {
  classifyPriceSignal,
  formatAreaCompact,
  formatCurrencyPln,
  formatInvestmentScore,
  getFutureLeadInsight,
  getLeadHeadlineMetric,
  getConfidenceBandLabel,
  priceSignalLabel,
} from '../../lib/investorMetrics';

interface LeadListProps {
  features: LeadFeature[];
  isLoading: boolean;
  error: Error | null;
}

function SkeletonRow() {
  return (
    <div className="animate-pulse border-b border-gray-800 px-4 py-3 space-y-2">
      <div className="flex items-center justify-between">
        <div className="h-3 w-32 rounded bg-gray-700" />
        <div className="h-5 w-16 rounded bg-gray-700" />
      </div>
      <div className="h-2 w-24 rounded bg-gray-800" />
    </div>
  );
}

function EmptyState() {
  return (
    <div className="flex flex-col items-center justify-center py-16 px-4 text-center">
      <TrendingUp size={32} className="mb-3 text-gray-600" aria-hidden />
      <p className="text-sm font-medium text-gray-400">Brak leadów</p>
      <p className="mt-1 text-xs text-gray-600">
        Spróbuj obniżyć próg min_score lub uruchom pipeline.
      </p>
    </div>
  );
}

function ErrorState({ message }: { message: string }) {
  return (
    <div className="flex flex-col items-center justify-center py-16 px-4 text-center">
      <AlertCircle size={32} className="mb-3 text-red-500" aria-hidden />
      <p className="text-sm font-medium text-red-400">Błąd ładowania</p>
      <p className="mt-1 text-xs text-gray-500 font-mono">{message}</p>
    </div>
  );
}

function countByConfidenceBand(features: LeadFeature[], band: 'formal' | 'supported' | 'speculative'): number {
  return features.filter((feature) => feature.properties.confidence_band === band).length;
}

interface LeadRowProps {
  feature: LeadFeature;
  isSelected: boolean;
  onSelect: (id: string) => void;
  onMouseEnter: () => void;
  onMouseLeave: () => void;
}

function LeadRow({ feature, isSelected, onSelect, onMouseEnter, onMouseLeave }: LeadRowProps) {
  const p = feature.properties;
  const formattedArea = formatAreaCompact(p.area_m2);
  const buildableArea = p.max_buildable_area_m2;
  const headlineMetric = getLeadHeadlineMetric(p);
  const priceSignal = classifyPriceSignal(p);
  const futureInsight = p.strategy_type === 'future_buildable' ? getFutureLeadInsight(p) : null;
  const dominantInsight = p.strategy_type === 'future_buildable'
    ? (p.dominant_future_signal ?? p.next_best_action ?? 'czeka na kolejny sygnał planistyczny')
    : (p.dominant_przeznaczenie ?? 'obowiązująca funkcja budowlana');

  return (
    <button
      type="button"
      onClick={() => onSelect(p.lead_id)}
      onMouseEnter={onMouseEnter}
      onMouseLeave={onMouseLeave}
      className={[
        'w-full border-b border-gray-800 px-4 py-3 text-left transition-colors',
        'hover:bg-gray-800/60 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-amber-500',
        isSelected ? 'bg-gray-800/90 border-l-2 border-l-amber-500 shadow-[inset_0_0_0_1px_rgba(251,191,36,0.08)]' : 'border-l-2 border-l-transparent',
      ].join(' ')}
      aria-pressed={isSelected}
      aria-label={`Działka ${p.identyfikator}, confidence ${Math.round(p.confidence_score * 100)}%`}
    >
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-start gap-2">
            <p className="truncate font-mono text-[12px] font-semibold text-gray-100 leading-tight">
              {p.identyfikator}
            </p>
            <span className={`rounded-full border px-2 py-0.5 text-[10px] font-medium uppercase tracking-wider ${
              p.strategy_type === 'future_buildable'
                ? 'border-sky-500/30 bg-sky-500/10 text-sky-200'
                : 'border-emerald-500/30 bg-emerald-500/10 text-emerald-200'
            }`}>
              {p.strategy_type === 'future_buildable' ? 'future' : 'current'}
            </span>
            {p.confidence_band && (
              <span
                className={[
                  'rounded-full border px-2 py-0.5 text-[10px] uppercase tracking-wider',
                  p.confidence_band === 'formal'
                    ? 'border-sky-400/30 bg-sky-400/10 text-sky-200'
                    : p.confidence_band === 'supported'
                      ? 'border-cyan-500/30 bg-cyan-500/10 text-cyan-200'
                      : 'border-violet-400/30 bg-violet-400/10 text-violet-200',
                ].join(' ')}
              >
                {getConfidenceBandLabel(p.confidence_band)}
              </span>
            )}
            <span className="rounded-full border border-gray-700 bg-gray-900 px-2 py-0.5 text-[10px] uppercase tracking-wider text-gray-400">
              {p.status}
            </span>
          </div>

          <div className="mt-2 flex flex-wrap items-end gap-x-4 gap-y-2">
            <div>
              <div className="text-[10px] uppercase tracking-wider text-gray-500">cena całkowita</div>
              <div className="text-lg font-semibold leading-none text-gray-100">
                {formatCurrencyPln(p.price_zl, { compact: true })}
              </div>
            </div>
            <div className="text-[11px] text-gray-400">
              <div>{headlineMetric}</div>
              <div className="mt-1 flex items-center gap-1 text-gray-500">
                <MapPin size={10} aria-hidden />
                {p.teryt_gmina}
              </div>
            </div>
            <div className="text-[11px] text-gray-400">
              <div>{formattedArea}</div>
              <div className="mt-1 text-gray-500">
                {buildableArea != null ? `${formatAreaCompact(buildableArea)} build.` : 'brak buildable area'}
              </div>
            </div>
            <div className="text-[11px] text-gray-400">
              <div>{p.max_coverage_pct != null ? `${p.max_coverage_pct.toFixed(0)}% pokrycia` : 'brak coverage'}</div>
              <div className="mt-1 text-gray-500">{p.dominant_przeznaczenie ?? 'bez designation'}</div>
            </div>
          </div>

          <div className="mt-3 flex flex-wrap items-center gap-2 text-[11px] text-gray-500">
            <span className="rounded-full border border-cyan-500/20 bg-cyan-500/10 px-2 py-0.5 text-[10px] font-medium text-cyan-200">
              score {formatInvestmentScore(p.investment_score)}
            </span>
            {p.strategy_type === 'future_buildable' && p.cheapness_score != null && (
              <span className="rounded-full border border-emerald-500/20 bg-emerald-500/10 px-2 py-0.5 text-[10px] font-medium text-emerald-200">
                cheapness {p.cheapness_score.toFixed(0)}/20
              </span>
            )}
            {futureInsight && (
              <>
                <span
                  className={[
                    'rounded-full border px-2 py-0.5 text-[10px] font-medium uppercase tracking-wider',
                    futureInsight.evidenceTierTone === 'formal'
                      ? 'border-sky-400/30 bg-sky-400/10 text-sky-200'
                      : futureInsight.evidenceTierTone === 'supported'
                        ? 'border-cyan-500/30 bg-cyan-500/10 text-cyan-200'
                        : futureInsight.evidenceTierTone === 'speculative'
                          ? 'border-violet-400/30 bg-violet-400/10 text-violet-200'
                          : 'border-gray-700 bg-gray-900 text-gray-400',
                  ].join(' ')}
                  title={futureInsight.evidenceTierHint}
                >
                  {futureInsight.evidenceTierLabel}
                </span>
                <span className="text-gray-500" title={futureInsight.spatialContextHint}>
                  {futureInsight.spatialContextLabel}
                </span>
              </>
            )}
            <span
              className={[
                'rounded-full px-2 py-0.5 text-[10px] font-medium',
                priceSignal === 'reliable'
                  ? 'bg-emerald-500/10 text-emerald-300'
                  : priceSignal === 'suspicious'
                    ? 'bg-yellow-500/10 text-yellow-200'
                    : 'bg-gray-800 text-gray-500',
              ].join(' ')}
            >
              {priceSignalLabel(priceSignal)}
            </span>
          </div>

          <div className="mt-3 flex items-start justify-between gap-3">
            <div className="min-w-0">
              <p className="truncate text-[11px] text-gray-300">
                {dominantInsight}
              </p>
              <p className="mt-1 truncate text-[11px] text-gray-500">
                {p.next_best_action ?? (p.strategy_type === 'future_buildable'
                  ? 'sprawdź źródło planistyczne i porównaj z aukcją'
                  : 'otwórz szczegóły i zweryfikuj operat')}
              </p>
              {p.notes && (
                <p className="mt-1 truncate text-[11px] text-gray-600">{p.notes}</p>
              )}
            </div>
            <span className="inline-flex items-center gap-1 text-[11px] text-amber-300">
              szczegóły
              <ArrowRight size={12} aria-hidden />
            </span>
          </div>

          {p.max_coverage_pct != null && (
            <div className="mt-2 h-1 w-full rounded-full bg-gray-800">
              <div
                className="h-full rounded-full bg-amber-500 transition-all"
                style={{ width: `${Math.min(p.max_coverage_pct, 100)}%` }}
                aria-label={`Coverage: ${p.max_coverage_pct.toFixed(0)}%`}
              />
            </div>
          )}
        </div>
        <ConfidenceBadge score={p.confidence_score} variant="badge" />
      </div>
    </button>
  );
}

export function LeadList({ features, isLoading, error }: LeadListProps) {
  const selectedLeadId = useMapStore((s) => s.selectedLeadId);
  const setSelectedLeadId = useMapStore((s) => s.setSelectedLeadId);
  const setSelectedQuarantineId = useMapStore((s) => s.setSelectedQuarantineId);
  const hoveredLeadId = useMapStore((s) => s.hoveredLeadId);
  const setHoveredLeadId = useMapStore((s) => s.setHoveredLeadId);

  if (isLoading) {
    return (
      <div aria-busy="true" aria-label="Ładowanie leadów...">
        {Array.from({ length: 8 }).map((_, i) => <SkeletonRow key={i} />)}
      </div>
    );
  }

  if (error) {
    return <ErrorState message={error.message} />;
  }

  if (features.length === 0) {
    return <EmptyState />;
  }

  const currentFeatures = features.filter((feature) => feature.properties.strategy_type === 'current_buildable');
  const futureFeatures = features.filter((feature) => feature.properties.strategy_type === 'future_buildable');
  const sections = [
    {
      key: 'current_buildable',
      title: 'Dziś budowlane',
      description: 'przeznaczenie działa już teraz',
      tone: 'emerald',
      features: currentFeatures,
      extraSummary: 'prawo dziś',
    },
    {
      key: 'future_buildable',
      title: 'Przyszłe budowlane',
      description: 'ścieżka planistyczna i benchmark ceny',
      tone: 'sky',
      features: futureFeatures,
      extraSummary: `formal ${countByConfidenceBand(futureFeatures, 'formal')} · supported ${countByConfidenceBand(futureFeatures, 'supported')} · speculative ${countByConfidenceBand(futureFeatures, 'speculative')}`,
    },
  ].filter((section) => section.features.length > 0);

  return (
    <div role="list" aria-label="Lista leadów inwestycyjnych">
      {sections.map((section) => (
        <section key={section.key} className="border-b border-gray-800 last:border-b-0">
      <div className="flex items-start justify-between gap-3 px-4 py-3">
        <div className="min-w-0">
          <p
            className={[
              'text-[11px] font-semibold uppercase tracking-[0.18em]',
              section.tone === 'sky' ? 'text-sky-300' : 'text-emerald-300',
            ].join(' ')}
          >
                {section.title}
              </p>
              <p className="mt-1 text-[11px] text-gray-500">{section.description}</p>
            </div>
            <div className="flex flex-col items-end gap-1">
              <span
                className={[
                  'rounded-full border px-2 py-0.5 text-[10px] font-medium',
                  section.tone === 'sky'
                    ? 'border-sky-500/20 bg-sky-500/10 text-sky-300'
                    : 'border-emerald-500/20 bg-emerald-500/10 text-emerald-300',
                ].join(' ')}
              >
                {section.features.length}
              </span>
              <span className="text-[10px] text-gray-500">{section.extraSummary}</span>
            </div>
          </div>
          <ul role="list" aria-label={section.title}>
            {section.features.map((feature) => (
              <li key={feature.properties.lead_id}>
                <LeadRow
                  feature={feature}
                  isSelected={selectedLeadId === feature.properties.lead_id || hoveredLeadId === feature.properties.lead_id}
                  onSelect={(id) => {
                    setSelectedQuarantineId(null);
                    setSelectedLeadId(id);
                  }}
                  onMouseEnter={() => setHoveredLeadId(feature.properties.lead_id)}
                  onMouseLeave={() => setHoveredLeadId(null)}
                />
              </li>
            ))}
          </ul>
        </section>
      ))}
    </div>
  );
}
