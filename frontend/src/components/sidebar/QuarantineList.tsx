import { AlertTriangle, ExternalLink, ShieldAlert } from 'lucide-react';

import { useMapStore } from '../../store/mapStore';
import type { QuarantineParcelFeature } from '../../types/api';

interface QuarantineListProps {
  features: QuarantineParcelFeature[];
  isLoading: boolean;
  error: Error | null;
}

function QuarantineSkeletonRow() {
  return (
    <div className="animate-pulse border-b border-yellow-500/10 px-4 py-3 space-y-2">
      <div className="flex items-center justify-between">
        <div className="h-3 w-28 rounded bg-gray-700" />
        <div className="h-5 w-14 rounded bg-gray-700" />
      </div>
      <div className="h-2 w-32 rounded bg-gray-800" />
    </div>
  );
}

function QuarantineErrorState({ message }: { message: string }) {
  return (
    <div className="rounded-lg border border-red-500/20 bg-red-500/5 px-4 py-4 text-xs">
      <div className="flex items-center gap-2 text-red-300">
        <AlertTriangle size={14} aria-hidden />
        <span className="font-medium">Błąd ładowania kwarantanny</span>
      </div>
      <p className="mt-2 break-all font-mono text-[11px] text-gray-500">{message}</p>
    </div>
  );
}

interface QuarantineRowProps {
  feature: QuarantineParcelFeature;
  isSelected: boolean;
  onSelect: (id: string) => void;
}

function QuarantineRow({ feature, isSelected, onSelect }: QuarantineRowProps) {
  const p = feature.properties;
  const formattedArea = p.area_m2 != null
    ? p.area_m2 >= 10_000
      ? `${(p.area_m2 / 10_000).toFixed(2)} ha`
      : `${Math.round(p.area_m2)} m²`
    : '—';

  return (
    <button
      type="button"
      onClick={() => onSelect(p.dzialka_id)}
      className={[
        'w-full border-b border-yellow-500/10 px-4 py-3 text-left transition-colors',
        'hover:bg-yellow-500/5 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-yellow-400',
        isSelected ? 'bg-yellow-500/10 border-l-2 border-l-yellow-400' : 'border-l-2 border-l-transparent',
      ].join(' ')}
      aria-pressed={isSelected}
    >
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-2">
            <p className="truncate font-mono text-xs text-gray-200 leading-tight">
              {p.identyfikator}
            </p>
            {p.source_url && (
              <ExternalLink size={10} className="text-yellow-300/80" aria-hidden />
            )}
          </div>
          <div className="mt-1 flex items-center gap-2 text-[11px] text-gray-500">
            <span>{p.teryt_gmina ?? 'brak TERYT'}</span>
            <span>·</span>
            <span>{formattedArea}</span>
          </div>
          <p className="mt-1.5 truncate text-[11px] text-yellow-200/90">
            {p.manual_przeznaczenie
              ? `override: ${p.manual_przeznaczenie}`
              : p.reason ?? 'wymaga ręcznej decyzji'}
          </p>
        </div>
        <span className="inline-flex items-center rounded border border-yellow-500/30 bg-yellow-500/10 px-2 py-0.5 text-[10px] font-medium uppercase tracking-wider text-yellow-300">
          review
        </span>
      </div>
    </button>
  );
}

export function QuarantineList({ features, isLoading, error }: QuarantineListProps) {
  const selectedQuarantineId = useMapStore((s) => s.selectedQuarantineId);
  const setSelectedQuarantineId = useMapStore((s) => s.setSelectedQuarantineId);
  const setSelectedLeadId = useMapStore((s) => s.setSelectedLeadId);

  return (
    <section aria-label="Działki w kwarantannie" className="border-b border-gray-800">
      <div className="flex items-center justify-between px-4 py-3">
        <div className="flex items-center gap-2">
          <ShieldAlert size={13} className="text-yellow-300" aria-hidden />
          <div>
            <p className="text-[11px] font-semibold uppercase tracking-wider text-yellow-300">
              Kwarantanna
            </p>
            <p className="text-[11px] text-gray-500">Działki wymagające ręcznego override</p>
          </div>
        </div>
        <span className="rounded border border-yellow-500/20 bg-yellow-500/10 px-2 py-0.5 text-[10px] font-mono text-yellow-200">
          {features.length}
        </span>
      </div>

      {isLoading && (
        <div aria-busy="true" aria-label="Ładowanie kwarantanny">
          {Array.from({ length: 3 }).map((_, i) => <QuarantineSkeletonRow key={i} />)}
        </div>
      )}

      {!isLoading && error && (
        <div className="px-4 pb-4">
          <QuarantineErrorState message={error.message} />
        </div>
      )}

      {!isLoading && !error && features.length === 0 && (
        <div className="px-4 pb-4 text-xs text-gray-500">
          Brak działek w kwarantannie.
        </div>
      )}

      {!isLoading && !error && features.length > 0 && (
        <ul role="list">
          {features.map((feature) => (
            <li key={feature.properties.dzialka_id}>
              <QuarantineRow
                feature={feature}
                isSelected={selectedQuarantineId === feature.properties.dzialka_id}
                onSelect={(id) => {
                  setSelectedLeadId(null);
                  setSelectedQuarantineId(id);
                }}
              />
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}
