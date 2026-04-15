import { BadgePercent, Building2, ChevronDown, ChevronUp, Coins, Layers3, TrendingUp } from 'lucide-react';
import { useState } from 'react';

import type { LeadFeature } from '../../types/api';
import {
  computeInvestorSnapshot,
  describeCheapnessScore,
  getConfidenceBandLabel,
} from '../../lib/investorMetrics';

interface InvestorSnapshotProps {
  features: LeadFeature[];
}

function SnapshotTile({
  icon,
  label,
  value,
  hint,
  tone = 'default',
}: {
  icon: React.ReactNode;
  label: string;
  value: string;
  hint: string;
  tone?: 'default' | 'emerald' | 'sky' | 'amber';
}) {
  const toneClass =
    tone === 'emerald'
      ? 'border-emerald-500/15 bg-emerald-500/5'
      : tone === 'sky'
        ? 'border-sky-500/15 bg-sky-500/5'
        : tone === 'amber'
          ? 'border-amber-500/15 bg-amber-500/5'
          : 'border-gray-800 bg-gray-950/70';

  return (
    <div className={`rounded-2xl border px-3 py-3 ${toneClass}`}>
      <div className="flex items-center gap-2 text-[10px] uppercase tracking-wider text-gray-500">
        <span className="text-amber-400">{icon}</span>
        {label}
      </div>
      <div className="mt-2 text-sm font-semibold text-gray-100">{value}</div>
      <div className="mt-1 text-[11px] text-gray-500">{hint}</div>
    </div>
  );
}

export function InvestorSnapshot({ features }: InvestorSnapshotProps) {
  const [collapsed, setCollapsed] = useState(false);
  const snapshot = computeInvestorSnapshot(features);
  const budgetFitPct =
    snapshot.visibleCount > 0 && snapshot.cheapestEntryPrice != null
      ? Math.round((snapshot.reliablePriceCount / snapshot.visibleCount) * 100)
      : null;

  return (
    <section className="border-b border-gray-800 px-4 py-4">
      <div className="mb-4 flex items-start justify-between gap-3">
        <div className="min-w-0">
          <p className="text-[10px] font-semibold uppercase tracking-[0.18em] text-gray-500">
            Snapshot inwestorski
          </p>
          <p className="mt-1 text-[11px] text-gray-500">
            Szybki pas KPI do decyzji: ile wyników jest dziś w grze, jak wyglądają ceny i ile okazji to future.
          </p>
        </div>
        <div className="flex flex-wrap items-center justify-end gap-2">
          <span className="rounded-full border border-gray-700 bg-gray-950 px-2 py-0.5 text-[10px] font-medium text-gray-300">
            {snapshot.visibleCount} aktywnych
          </span>
          <span className="rounded-full border border-emerald-500/20 bg-emerald-500/10 px-2 py-0.5 text-[10px] font-medium text-emerald-300">
            {snapshot.currentBuildableCount} current
          </span>
          <span className="rounded-full border border-sky-500/20 bg-sky-500/10 px-2 py-0.5 text-[10px] font-medium text-sky-300">
            {snapshot.futureBuildableCount} future
          </span>
          <button
            type="button"
            onClick={() => setCollapsed((value) => !value)}
            className="inline-flex items-center gap-1 rounded-full border border-gray-700 bg-gray-950 px-2 py-0.5 text-[10px] font-medium text-gray-300 transition-colors hover:border-gray-600 hover:bg-gray-900"
          >
            {collapsed ? 'rozwiń' : 'zwiń'}
            {collapsed ? <ChevronDown size={12} aria-hidden /> : <ChevronUp size={12} aria-hidden />}
          </button>
        </div>
      </div>

      {!collapsed && (
        <>
          <div className="grid grid-cols-2 gap-2">
        <SnapshotTile
          icon={<Coins size={12} aria-hidden />}
          label="Mediana zł / m²"
          value={
            snapshot.medianPricePerM2 != null
              ? `${snapshot.medianPricePerM2.toFixed(2)} zł/m²`
              : 'brak wiaryg. ceny'
          }
          hint={`${snapshot.reliablePriceCount}/${snapshot.visibleCount} leadów z wiarygodną ceną`}
          tone="amber"
        />
        <SnapshotTile
          icon={<TrendingUp size={12} aria-hidden />}
          label="Najtańsze wejście"
          value={
            snapshot.cheapestEntryPrice != null
              ? `${snapshot.cheapestEntryPrice.toLocaleString('pl-PL')} zł`
              : 'brak ceny wejścia'
          }
          hint={budgetFitPct != null ? `${budgetFitPct}% widoku z ceną do porównań` : 'brak wystarczających danych cenowych'}
          tone="default"
        />
        <SnapshotTile
          icon={<Layers3 size={12} aria-hidden />}
          label="Dziś budowlane"
          value={`${snapshot.currentBuildableCount}`}
          hint="prawo działa już dziś"
          tone="emerald"
        />
        <SnapshotTile
          icon={<BadgePercent size={12} aria-hidden />}
          label="Przyszłe budowlane"
          value={`${snapshot.futureBuildableCount}`}
          hint={`${snapshot.futureFormalCount} formal · ${snapshot.futureSupportedCount} supported`}
          tone="sky"
        />
      </div>

      <div className="mt-3 grid grid-cols-1 gap-2">
        <SnapshotTile
          icon={<Building2 size={12} aria-hidden />}
          label="Future cheapness"
          value={
            snapshot.medianCheapnessScore != null
              ? `${snapshot.medianCheapnessScore.toFixed(0)} / 20`
              : '—'
          }
          hint={describeCheapnessScore(snapshot.medianCheapnessScore)}
        />
        <SnapshotTile
          icon={<Layers3 size={12} aria-hidden />}
          label="Średnie pokrycie"
          value={
            snapshot.averageCoveragePct != null
              ? `${snapshot.averageCoveragePct.toFixed(0)}%`
              : 'brak coverage'
          }
          hint="średni udział strefy buildable w bieżącym widoku"
        />
        <SnapshotTile
          icon={<BadgePercent size={12} aria-hidden />}
          label="Prime confidence"
          value={`${snapshot.primeCount}`}
          hint="leady z confidence >= 0.9"
        />
      </div>

      <div className="mt-3 rounded-2xl border border-gray-800 bg-gray-950/60 px-3 py-2 text-[11px] text-gray-500">
        <span className="font-medium text-gray-400">Confidence bands:</span>{' '}
        formal = {getConfidenceBandLabel('formal')}, supported = {getConfidenceBandLabel('supported')}, speculative = {getConfidenceBandLabel('speculative')}.
      </div>
        </>
      )}
    </section>
  );
}
