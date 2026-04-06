/**
 * Lead detail panel — full breakdown of a selected investment lead.
 * Shown in the sidebar when operator clicks a parcel on the map or list.
 *
 * Displays: TERYT identifiers, area, coverage, zone designation,
 * confidence bar, and the full evidence chain.
 */

import { ArrowLeft, MapPin, Layers, Maximize2, Clock, Tag } from 'lucide-react';
import type { LeadFeature } from '../../types/api';
import { ConfidenceBadge } from '../ui/ConfidenceBadge';
import { EvidenceChain } from '../ui/EvidenceChain';

interface LeadDetailProps {
  feature: LeadFeature;
  onBack: () => void;
}

interface StatRowProps {
  icon: React.ReactNode;
  label: string;
  value: React.ReactNode;
}

function StatRow({ icon, label, value }: StatRowProps) {
  return (
    <div className="flex items-center justify-between py-2 border-b border-gray-800 last:border-0">
      <span className="flex items-center gap-1.5 text-xs text-gray-500">
        {icon}
        {label}
      </span>
      <span className="text-xs font-mono text-gray-200 text-right max-w-[60%] truncate">
        {value}
      </span>
    </div>
  );
}

function StatusBadge({ status }: { status: string }) {
  const styles: Record<string, string> = {
    new:         'bg-blue-500/20 text-blue-400 border-blue-500/30',
    reviewed:    'bg-gray-500/20 text-gray-400 border-gray-500/30',
    shortlisted: 'bg-green-500/20 text-green-400 border-green-500/30',
    rejected:    'bg-red-500/20 text-red-400 border-red-500/30',
    invested:    'bg-purple-500/20 text-purple-400 border-purple-500/30',
  };
  const cls = styles[status] ?? styles['new'];
  return (
    <span className={`inline-flex items-center rounded border px-2 py-0.5 text-[10px] font-medium uppercase tracking-wider ${cls}`}>
      {status}
    </span>
  );
}

export function LeadDetail({ feature, onBack }: LeadDetailProps) {
  const p = feature.properties;

  const formattedArea = p.area_m2 != null
    ? p.area_m2 >= 10_000
      ? `${(p.area_m2 / 10_000).toFixed(3)} ha (${Math.round(p.area_m2).toLocaleString('pl-PL')} m²)`
      : `${Math.round(p.area_m2).toLocaleString('pl-PL')} m²`
    : '—';

  const createdAt = new Date(p.created_at).toLocaleDateString('pl-PL', {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="flex items-center gap-2 border-b border-gray-800 px-4 py-3 flex-shrink-0">
        <button
          type="button"
          onClick={onBack}
          className="flex items-center gap-1 rounded p-1 text-gray-400 hover:bg-gray-800 hover:text-gray-200 transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-amber-500"
          aria-label="Wróć do listy leadów"
        >
          <ArrowLeft size={14} aria-hidden />
          <span className="text-xs">Lista</span>
        </button>
        <div className="ml-auto">
          <StatusBadge status={p.status} />
        </div>
      </div>

      {/* Scrollable content */}
      <div className="flex-1 overflow-y-auto">
        {/* Identyfikator + confidence */}
        <div className="px-4 py-4 border-b border-gray-800">
          <p className="font-mono text-sm text-gray-100 break-all leading-snug">
            {p.identyfikator}
          </p>
          <p className="mt-1 text-xs text-gray-500">{p.teryt_gmina}</p>
          <div className="mt-3">
            <ConfidenceBadge score={p.confidence_score} variant="bar" />
          </div>
        </div>

        {/* Stats */}
        <div className="px-4 py-2 border-b border-gray-800">
          <p className="text-[10px] font-medium uppercase tracking-wider text-gray-600 mb-1 pt-1">
            Parametry działki
          </p>
          <StatRow
            icon={<Maximize2 size={10} aria-hidden />}
            label="Powierzchnia"
            value={formattedArea}
          />
          {p.max_coverage_pct != null && (
            <StatRow
              icon={<Layers size={10} aria-hidden />}
              label="Pokrycie MPZP"
              value={
                <span className="text-amber-400">{p.max_coverage_pct.toFixed(1)}%</span>
              }
            />
          )}
          {p.dominant_przeznaczenie && (
            <StatRow
              icon={<Tag size={10} aria-hidden />}
              label="Przeznaczenie"
              value={
                <span className="rounded bg-amber-500/20 px-1.5 text-amber-300 border border-amber-500/30">
                  {p.dominant_przeznaczenie}
                </span>
              }
            />
          )}
          <StatRow
            icon={<MapPin size={10} aria-hidden />}
            label="Priorytet"
            value={p.priority.toUpperCase()}
          />
          <StatRow
            icon={<Clock size={10} aria-hidden />}
            label="Wykryto"
            value={createdAt}
          />
        </div>

        {/* Evidence chain */}
        <div className="px-4 py-4">
          <p className="text-[10px] font-medium uppercase tracking-wider text-gray-600 mb-3">
            Łańcuch dowodowy
          </p>
          <EvidenceChain chain={p.evidence_chain} />
        </div>
      </div>
    </div>
  );
}
