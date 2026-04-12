import { Download, Star } from 'lucide-react';

import { useMapStore } from '../../store/mapStore';
import type { LeadFeature } from '../../types/api';
import { formatCurrencyPln } from '../../lib/investorMetrics';
import { exportShortlistCsv } from '../../lib/exportShortlist';

interface ShortlistSectionProps {
  features: LeadFeature[];
  isLoading: boolean;
  error: Error | null;
}

export function ShortlistSection({ features, isLoading, error }: ShortlistSectionProps) {
  const setSelectedLeadId = useMapStore((s) => s.setSelectedLeadId);
  const setSelectedQuarantineId = useMapStore((s) => s.setSelectedQuarantineId);
  const futureCount = features.filter((feature) => feature.properties.strategy_type === 'future_buildable').length;

  return (
    <section aria-label="Shortlista inwestora" className="border-b border-gray-800">
      <div className="flex items-center justify-between px-4 py-3">
        <div className="flex items-center gap-2">
          <Star size={13} className="text-emerald-300" aria-hidden />
          <div>
            <p className="text-[11px] font-semibold uppercase tracking-wider text-emerald-300">
              Shortlista inwestora
            </p>
            <p className="text-[11px] text-gray-500">Lead’y oznaczone jako shortlist.</p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <span className="rounded border border-emerald-500/20 bg-emerald-500/10 px-2 py-0.5 text-[10px] font-mono text-emerald-200">
            {features.length}
          </span>
          <button
            type="button"
            onClick={() => exportShortlistCsv(features)}
            disabled={features.length === 0}
            className="rounded border border-gray-800 bg-gray-950 px-2 py-1 text-[10px] text-gray-300 hover:border-gray-700 hover:bg-gray-900 disabled:cursor-not-allowed disabled:opacity-40"
          >
            <span className="inline-flex items-center gap-1">
              <Download size={11} aria-hidden />
              CSV
            </span>
          </button>
          <button
            type="button"
            onClick={() => exportShortlistCsv(features, {
              strategyType: 'future_buildable',
              filename: 'terrazoning-shortlist-future-buildable.csv',
            })}
            disabled={futureCount === 0}
            className="rounded border border-sky-500/20 bg-sky-500/10 px-2 py-1 text-[10px] text-sky-200 hover:border-sky-400/40 hover:bg-sky-500/15 disabled:cursor-not-allowed disabled:opacity-40"
            title="Eksport tylko future_buildable z aktualnej shortlisty"
            >
              <span className="inline-flex items-center gap-1">
                <Download size={11} aria-hidden />
                future CSV
              </span>
            </button>
          <button
            type="button"
            onClick={() => exportShortlistCsv(features, {
              strategyType: 'future_buildable',
              confidenceBands: ['formal', 'supported'],
              filename: 'terrazoning-shortlist-future-formal-supported.csv',
            })}
            disabled={futureCount === 0}
            className="rounded border border-emerald-500/20 bg-emerald-500/10 px-2 py-1 text-[10px] text-emerald-200 hover:border-emerald-400/40 hover:bg-emerald-500/15 disabled:cursor-not-allowed disabled:opacity-40"
            title="Eksport tylko future_buildable z bandami formal i supported"
          >
            <span className="inline-flex items-center gap-1">
              <Download size={11} aria-hidden />
              future safe CSV
            </span>
          </button>
        </div>
      </div>

      {isLoading && (
        <div className="px-4 pb-4 text-xs text-gray-500">Ładowanie shortlisty…</div>
      )}

      {!isLoading && error && (
        <div className="px-4 pb-4 text-xs text-red-400">{error.message}</div>
      )}

      {!isLoading && !error && features.length === 0 && (
        <div className="px-4 pb-4 text-xs text-gray-500">
          Brak leadów na shortliście.
        </div>
      )}

      {!isLoading && !error && features.length > 0 && (
        <ul role="list">
          {features.map((feature) => {
            const p = feature.properties;
            return (
              <li key={p.lead_id}>
                <button
                  type="button"
                  onClick={() => {
                    setSelectedQuarantineId(null);
                    setSelectedLeadId(p.lead_id);
                  }}
                  className="w-full border-t border-emerald-500/10 px-4 py-3 text-left hover:bg-emerald-500/5"
                >
                  <div className="flex items-start justify-between gap-2">
                    <div className="min-w-0">
                      <p className="truncate font-mono text-xs text-gray-200">{p.identyfikator}</p>
                      <div className="mt-1 flex flex-wrap items-center gap-x-2 gap-y-1 text-[11px] text-gray-500">
                        <span>{p.dominant_przeznaczenie ?? '—'}</span>
                        {p.strategy_type === 'future_buildable' && (
                          <span className="text-sky-300">
                            future
                          </span>
                        )}
                        {p.price_per_m2_zl != null && <span>{p.price_per_m2_zl.toFixed(0)} zł/m²</span>}
                        {p.price_zl != null && <span>{formatCurrencyPln(p.price_zl, { compact: true })}</span>}
                      </div>
                      {p.notes && (
                        <p className="mt-1 truncate text-[11px] text-gray-500">{p.notes}</p>
                      )}
                    </div>
                    <span className="rounded border border-emerald-500/20 bg-emerald-500/10 px-1.5 py-0.5 text-[10px] uppercase tracking-wider text-emerald-300">
                      shortlist
                    </span>
                  </div>
                </button>
              </li>
            );
          })}
        </ul>
      )}
    </section>
  );
}
