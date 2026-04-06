/**
 * Sidebar lead list — sorted by confidence_score DESC (matches API order).
 * Clicking a row: selects the lead, map highlights the parcel, detail view opens.
 */

import { MapPin, TrendingUp, AlertCircle } from 'lucide-react';
import type { LeadFeature } from '../../types/api';
import { ConfidenceBadge } from '../ui/ConfidenceBadge';
import { useMapStore } from '../../store/mapStore';

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

interface LeadRowProps {
  feature: LeadFeature;
  isSelected: boolean;
  onSelect: (id: string) => void;
}

function LeadRow({ feature, isSelected, onSelect }: LeadRowProps) {
  const p = feature.properties;

  const formattedArea = p.area_m2 != null
    ? p.area_m2 >= 10_000
      ? `${(p.area_m2 / 10_000).toFixed(2)} ha`
      : `${Math.round(p.area_m2)} m²`
    : '—';

  return (
    <button
      type="button"
      onClick={() => onSelect(p.lead_id)}
      className={[
        'w-full border-b border-gray-800 px-4 py-3 text-left transition-colors',
        'hover:bg-gray-800/60 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-amber-500',
        isSelected ? 'bg-gray-800 border-l-2 border-l-amber-500' : 'border-l-2 border-l-transparent',
      ].join(' ')}
      aria-pressed={isSelected}
      aria-label={`Działka ${p.identyfikator}, confidence ${Math.round(p.confidence_score * 100)}%`}
    >
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0 flex-1">
          <p className="truncate font-mono text-xs text-gray-300 leading-tight">
            {p.identyfikator}
          </p>
          <div className="mt-1 flex items-center gap-2 text-[11px] text-gray-500">
            <span className="flex items-center gap-0.5">
              <MapPin size={9} aria-hidden />
              {p.teryt_gmina}
            </span>
            <span>·</span>
            <span>{formattedArea}</span>
            {p.dominant_przeznaczenie && (
              <>
                <span>·</span>
                <span className="font-mono text-amber-600">{p.dominant_przeznaczenie}</span>
              </>
            )}
          </div>
          {p.max_coverage_pct != null && (
            <div className="mt-1.5 h-0.5 w-full rounded-full bg-gray-700">
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

  return (
    <ul role="list" aria-label="Lista leadów inwestycyjnych">
      {features.map((feature) => (
        <li key={feature.properties.lead_id}>
          <LeadRow
            feature={feature}
            isSelected={selectedLeadId === feature.properties.lead_id}
            onSelect={setSelectedLeadId}
          />
        </li>
      ))}
    </ul>
  );
}
