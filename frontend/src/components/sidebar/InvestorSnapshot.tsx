import { BadgePercent, Building2, Coins, Layers3 } from 'lucide-react';

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
}: {
  icon: React.ReactNode;
  label: string;
  value: string;
  hint: string;
}) {
  return (
    <div className="rounded-xl border border-gray-800 bg-gray-950/70 px-3 py-3">
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
  const snapshot = computeInvestorSnapshot(features);

  return (
    <section className="border-b border-gray-800 px-4 py-4">
      <div className="mb-3 flex items-center justify-between">
        <div>
          <p className="text-[10px] font-medium uppercase tracking-wider text-gray-600">
            Snapshot Inwestycyjny
          </p>
          <p className="mt-1 text-[11px] text-gray-500">
            Segregujemy dziś budowlane od future_buildable i pokazujemy benchmark ceny.
          </p>
        </div>
        <div className="flex flex-col items-end gap-1">
          <span className="rounded-full border border-gray-700 bg-gray-950 px-2 py-0.5 text-[10px] font-medium text-gray-300">
            {snapshot.visibleCount} aktywnych
          </span>
          <div className="flex items-center gap-1">
            <span className="rounded-full border border-emerald-500/20 bg-emerald-500/10 px-2 py-0.5 text-[10px] font-medium text-emerald-300">
              {snapshot.currentBuildableCount} dziś
            </span>
            <span className="rounded-full border border-sky-500/20 bg-sky-500/10 px-2 py-0.5 text-[10px] font-medium text-sky-300">
              {snapshot.futureBuildableCount} future
            </span>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-2 gap-2">
        <SnapshotTile
          icon={<Coins size={12} aria-hidden />}
          label="Mediana ceny / m²"
          value={
            snapshot.medianPricePerM2 != null
              ? `${snapshot.medianPricePerM2.toFixed(2)} zł/m²`
              : 'brak wiaryg. ceny'
          }
          hint={`${snapshot.reliablePriceCount}/${snapshot.visibleCount} leadów z wiarygodną ceną`}
        />
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
          label="Dziś budowlane"
          value={
            `${snapshot.currentBuildableCount}`
          }
          hint="prawo już dziś"
        />
        <SnapshotTile
          icon={<BadgePercent size={12} aria-hidden />}
          label="Przyszłe budowlane"
          value={`${snapshot.futureBuildableCount}`}
          hint={`${snapshot.futureFormalCount} formal · ${snapshot.futureSupportedCount} supported · ${snapshot.futureSpeculativeCount} speculative`}
        />
      </div>

      <div className="mt-3 rounded-xl border border-gray-800 bg-gray-950/60 px-3 py-2 text-[11px] text-gray-500">
        <span className="font-medium text-gray-400">Confidence bands:</span>{' '}
        formal = {getConfidenceBandLabel('formal')}, supported = {getConfidenceBandLabel('supported')}, speculative = {getConfidenceBandLabel('speculative')}.{' '}
        Prime confidence: {snapshot.primeCount}.
      </div>
    </section>
  );
}
