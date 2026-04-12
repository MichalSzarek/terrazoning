import { BellRing, Eye, SlidersHorizontal } from 'lucide-react';
import { useEffect, useMemo, useRef, useState } from 'react';

import { useMapStore } from '../../store/mapStore';
import type { LeadFeature } from '../../types/api';
import {
  useWatchlist,
  useWatchlistAcknowledgeMutation,
  useWatchlistUpdateMutation,
} from '../../hooks/useWatchlist';
import {
  classifyPriceSignal,
  formatAreaCompact,
  formatCurrencyPln,
} from '../../lib/investorMetrics';
import {
  DEFAULT_WATCHLIST_CRITERIA,
  FUTURE_BUILDABLE_WATCHLIST_PRESET,
  fromApiWatchlist,
  getNewWatchlistMatches,
  getWatchlistMatches,
  toApiWatchlist,
  type WatchlistMode,
  type WatchlistCriteria,
} from '../../lib/watchlist';

interface WatchlistSectionProps {
  features: LeadFeature[];
  isLoading: boolean;
  error: Error | null;
}

function isNewLead(lead: LeadFeature, acknowledgedAt: string | null): boolean {
  if (!acknowledgedAt) {
    return true;
  }

  const acknowledged = Date.parse(acknowledgedAt);
  const created = Date.parse(lead.properties.created_at);
  if (Number.isNaN(acknowledged) || Number.isNaN(created)) {
    return false;
  }

  return created > acknowledged;
}

export function WatchlistSection({ features, isLoading, error }: WatchlistSectionProps) {
  const setSelectedLeadId = useMapStore((s) => s.setSelectedLeadId);
  const setSelectedQuarantineId = useMapStore((s) => s.setSelectedQuarantineId);
  const watchlistQuery = useWatchlist();
  const updateWatchlist = useWatchlistUpdateMutation();
  const acknowledgeWatchlist = useWatchlistAcknowledgeMutation();
  const [criteria, setCriteria] = useState<WatchlistCriteria>(DEFAULT_WATCHLIST_CRITERIA);
  const [mode, setMode] = useState<WatchlistMode>('standard');
  const [notificationPermission, setNotificationPermission] = useState<NotificationPermission | 'unsupported'>(
    typeof window !== 'undefined' && 'Notification' in window ? window.Notification.permission : 'unsupported',
  );
  const isHydratedRef = useRef(false);
  const lastSyncedSignatureRef = useRef(JSON.stringify(DEFAULT_WATCHLIST_CRITERIA));
  const lastNotifiedLeadIdRef = useRef<string | null>(null);

  useEffect(() => {
    if (!watchlistQuery.data) {
      return;
    }
    const next = fromApiWatchlist(watchlistQuery.data);
    const signature = JSON.stringify(next);
    lastSyncedSignatureRef.current = signature;
    isHydratedRef.current = true;
    setCriteria((current) => (JSON.stringify(current) === signature ? current : next));
    const isFuturePreset =
      next.maxPricePerM2 == null
      && next.minCoveragePct === FUTURE_BUILDABLE_WATCHLIST_PRESET.minCoveragePct
      && next.minConfidencePct === FUTURE_BUILDABLE_WATCHLIST_PRESET.minConfidencePct
      && next.requiredDesignation === FUTURE_BUILDABLE_WATCHLIST_PRESET.requiredDesignation
      && next.onlyReliablePrice === FUTURE_BUILDABLE_WATCHLIST_PRESET.onlyReliablePrice;
    setMode(isFuturePreset ? 'future_buildable' : 'standard');
  }, [watchlistQuery.data]);

  useEffect(() => {
    if (!isHydratedRef.current) {
      return;
    }
    const signature = JSON.stringify(criteria);
    if (signature === lastSyncedSignatureRef.current) {
      return;
    }
    const timer = window.setTimeout(() => {
      lastSyncedSignatureRef.current = signature;
      updateWatchlist.mutate(toApiWatchlist(criteria));
    }, 350);
    return () => window.clearTimeout(timer);
  }, [criteria, updateWatchlist]);

  const matches = useMemo(
    () => getWatchlistMatches(features, criteria, mode),
    [features, criteria, mode],
  );
  const newMatches = useMemo(
    () => getNewWatchlistMatches(matches, criteria),
    [matches, criteria],
  );

  useEffect(() => {
    if (typeof window === 'undefined' || !('Notification' in window)) {
      return;
    }
    setNotificationPermission(window.Notification.permission);
  }, []);

  useEffect(() => {
    if (
      notificationPermission !== 'granted'
      || newMatches.length === 0
    ) {
      return;
    }

    const newest = newMatches[0]?.properties;
    if (!newest || lastNotifiedLeadIdRef.current === newest.lead_id) {
      return;
    }

    lastNotifiedLeadIdRef.current = newest.lead_id;
    const highlight = newest.dominant_future_signal ?? newest.dominant_przeznaczenie ?? '—';
    const notification = new window.Notification('TerraZoning — nowe dopasowanie', {
      body: `${newMatches.length} nowych leadów. Najnowszy: ${newest.identyfikator} · ${highlight}`,
      tag: 'terrazoning-watchlist',
    });
    notification.onclick = () => {
      window.focus();
      setSelectedQuarantineId(null);
      setSelectedLeadId(newest.lead_id);
      notification.close();
    };
  }, [newMatches, notificationPermission, setSelectedLeadId, setSelectedQuarantineId]);

  async function enableDesktopAlerts(): Promise<void> {
    if (typeof window === 'undefined' || !('Notification' in window)) {
      setNotificationPermission('unsupported');
      return;
    }
    const next = await window.Notification.requestPermission();
    setNotificationPermission(next);
  }

  return (
    <section aria-label="Watchlista inwestora" className="border-b border-gray-800">
      <div className="flex items-start justify-between gap-3 px-4 py-4">
        <div className="flex items-start gap-2">
          <BellRing size={14} className="mt-0.5 text-cyan-300" aria-hidden />
          <div>
            <p className="text-[11px] font-semibold uppercase tracking-wider text-cyan-300">
              Watchlista
            </p>
            <p className="mt-1 text-[11px] text-gray-500">
              Nowe leady, które spełniają Twoje progi wejścia.
            </p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <span className="rounded border border-cyan-500/20 bg-cyan-500/10 px-2 py-0.5 text-[10px] font-mono text-cyan-200">
            {matches.length}
          </span>
          <span className="rounded border border-amber-500/20 bg-amber-500/10 px-2 py-0.5 text-[10px] font-mono text-amber-200">
            {newMatches.length} new
          </span>
        </div>
      </div>

      <div className="flex items-center gap-2 px-4 pb-3">
        <button
          type="button"
          onClick={() => {
            setMode('standard');
            setCriteria({
              ...DEFAULT_WATCHLIST_CRITERIA,
              acknowledgedAt: criteria.acknowledgedAt,
            });
          }}
          className={[
            'rounded border px-2 py-1 text-[10px] font-medium transition-colors',
            mode === 'standard'
              ? 'border-cyan-500/30 bg-cyan-500/10 text-cyan-200'
              : 'border-gray-800 bg-gray-950 text-gray-300 hover:border-gray-700 hover:bg-gray-900',
          ].join(' ')}
        >
          standard
        </button>
        <button
          type="button"
          onClick={() => {
            setMode('future_buildable');
            setCriteria((current) => ({
              ...current,
              ...FUTURE_BUILDABLE_WATCHLIST_PRESET,
              acknowledgedAt: current.acknowledgedAt,
            }));
          }}
          className={[
            'rounded border px-2 py-1 text-[10px] font-medium transition-colors',
            mode === 'future_buildable'
              ? 'border-sky-500/30 bg-sky-500/10 text-sky-200'
              : 'border-gray-800 bg-gray-950 text-gray-300 hover:border-gray-700 hover:bg-gray-900',
          ].join(' ')}
          title="Tryb future_buildable: tylko formal + supported, cheap_only i bez progu pokrycia"
        >
          future_buildable
        </button>
        {mode === 'future_buildable' && (
          <span className="rounded border border-sky-500/20 bg-sky-500/10 px-2 py-0.5 text-[10px] font-medium text-sky-200">
            preset: formal + supported
          </span>
        )}
      </div>

      <div className="flex items-center justify-between gap-3 px-4 pb-3">
        <p className="text-[10px] text-gray-500">
          {newMatches.length > 0
            ? mode === 'future_buildable'
              ? `Alert future_buildable aktywny dla ${newMatches.length} nowych okazji.`
              : `Alert aktywny dla ${newMatches.length} nowych okazji.`
            : mode === 'future_buildable'
              ? 'Brak nowych future_buildable po ostatnim przeglądzie.'
              : 'Brak nowych dopasowań od ostatniego przeglądu.'}
        </p>
        <button
          type="button"
          onClick={() => {
            void enableDesktopAlerts();
          }}
          disabled={notificationPermission === 'granted'}
          className="rounded border border-gray-800 bg-gray-950 px-2 py-1 text-[10px] text-gray-300 hover:border-gray-700 hover:bg-gray-900 disabled:cursor-not-allowed disabled:opacity-40"
        >
          {notificationPermission === 'unsupported'
            ? 'alerty niedostępne'
            : notificationPermission === 'granted'
              ? 'alert desktop: on'
              : 'włącz alert desktop'}
        </button>
      </div>

      <div className="grid grid-cols-2 gap-2 px-4 pb-3">
        <label className="text-[10px] text-gray-500">
          {mode === 'future_buildable' ? 'max zł/m²' : 'max zł/m²'}
          <input
            type="number"
            min={0}
            value={criteria.maxPricePerM2 ?? ''}
            onChange={(event) => {
              const next = event.target.value;
              setCriteria((current) => ({
                ...current,
                maxPricePerM2: next === '' ? null : Number(next),
              }));
            }}
            className="mt-1 w-full rounded-md border border-gray-800 bg-gray-950 px-2 py-1.5 text-[11px] text-gray-200 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-cyan-400"
          />
        </label>
        <label className="text-[10px] text-gray-500">
          {mode === 'future_buildable' ? 'min score %' : 'min pokrycie %'}
          <input
            type="number"
            min={0}
            max={100}
            value={criteria.minCoveragePct}
            onChange={(event) => {
              setCriteria((current) => ({
                ...current,
                minCoveragePct: Number(event.target.value),
              }));
            }}
            className="mt-1 w-full rounded-md border border-gray-800 bg-gray-950 px-2 py-1.5 text-[11px] text-gray-200 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-cyan-400"
          />
        </label>
        <label className="text-[10px] text-gray-500">
          min confidence %
          <input
            type="number"
            min={0}
            max={100}
            value={criteria.minConfidencePct}
            onChange={(event) => {
              setCriteria((current) => ({
                ...current,
                minConfidencePct: Number(event.target.value),
              }));
            }}
            className="mt-1 w-full rounded-md border border-gray-800 bg-gray-950 px-2 py-1.5 text-[11px] text-gray-200 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-cyan-400"
          />
        </label>
        <label className="text-[10px] text-gray-500">
          {mode === 'future_buildable' ? 'przeznaczenie / sygnał' : 'przeznaczenie'}
          <input
            type="text"
            placeholder="np. MN, U, MW/U"
            value={criteria.requiredDesignation}
            onChange={(event) => {
              setCriteria((current) => ({
                ...current,
                requiredDesignation: event.target.value,
              }));
            }}
            className="mt-1 w-full rounded-md border border-gray-800 bg-gray-950 px-2 py-1.5 text-[11px] text-gray-200 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-cyan-400"
          />
        </label>
      </div>

      <div className="flex items-center justify-between gap-3 px-4 pb-4">
        <label className="inline-flex items-center gap-2 text-[11px] text-gray-400">
          <input
            type="checkbox"
            checked={criteria.onlyReliablePrice}
            onChange={(event) => {
              setCriteria((current) => ({
                ...current,
                onlyReliablePrice: event.target.checked,
              }));
            }}
            className="rounded border-gray-700 bg-gray-950 text-cyan-400 focus:ring-cyan-400"
          />
          tylko wiarygodne ceny
        </label>
        <button
          type="button"
          onClick={() => {
            const now = new Date().toISOString();
            acknowledgeWatchlist.mutate();
            setCriteria((current) => ({
              ...current,
              acknowledgedAt: now,
            }));
          }}
          className="inline-flex items-center gap-1 rounded border border-gray-800 bg-gray-950 px-2 py-1 text-[10px] text-gray-300 hover:border-gray-700 hover:bg-gray-900"
        >
          <Eye size={11} aria-hidden />
          oznacz jako obejrzane
        </button>
      </div>

      {(isLoading || watchlistQuery.isLoading) && (
        <div className="px-4 pb-4 text-xs text-gray-500">Liczenie alertów…</div>
      )}

      {!isLoading && !watchlistQuery.isLoading && (error || watchlistQuery.error) && (
        <div className="px-4 pb-4 text-xs text-red-400">{error?.message ?? watchlistQuery.error?.message}</div>
      )}

      {!isLoading && !watchlistQuery.isLoading && !error && !watchlistQuery.error && matches.length === 0 && (
        <div className="px-4 pb-4 text-xs text-gray-500">
          Brak leadów spełniających aktualne kryteria watchlisty.
        </div>
      )}

      {!isLoading && !watchlistQuery.isLoading && !error && !watchlistQuery.error && matches.length > 0 && (
        <ul role="list" className="border-t border-cyan-500/10">
          {matches.slice(0, 5).map((feature) => {
            const p = feature.properties;
            const isNew = isNewLead(feature, criteria.acknowledgedAt);
            const priceSignal = classifyPriceSignal(p);

            return (
              <li key={p.lead_id}>
                <button
                  type="button"
                  onClick={() => {
                    setSelectedQuarantineId(null);
                    setSelectedLeadId(p.lead_id);
                  }}
                  className="w-full border-b border-cyan-500/10 px-4 py-3 text-left hover:bg-cyan-500/5"
                >
                  <div className="flex items-start justify-between gap-2">
                    <div className="min-w-0">
                      <div className="flex items-center gap-2">
                        <p className="truncate font-mono text-xs text-gray-200">{p.identyfikator}</p>
                        {isNew && (
                          <span className="rounded border border-amber-500/20 bg-amber-500/10 px-1.5 py-0.5 text-[10px] uppercase tracking-wider text-amber-200">
                            new
                          </span>
                        )}
                      </div>
                      <div className="mt-1 flex flex-wrap items-center gap-x-2 gap-y-1 text-[11px] text-gray-500">
                        {(p.dominant_future_signal ?? p.dominant_przeznaczenie) && (
                          <span>{p.dominant_future_signal ?? p.dominant_przeznaczenie}</span>
                        )}
                        {p.max_coverage_pct != null && <span>{p.max_coverage_pct.toFixed(0)}% pokrycia</span>}
                        {p.max_buildable_area_m2 != null && <span>{formatAreaCompact(p.max_buildable_area_m2)} build.</span>}
                        {p.price_per_m2_zl != null && <span>{p.price_per_m2_zl.toFixed(0)} zł/m²</span>}
                        {p.price_zl != null && <span>{formatCurrencyPln(p.price_zl, { compact: true })}</span>}
                      </div>
                    </div>
                    <span
                      className={[
                        'rounded px-1.5 py-0.5 text-[10px] font-medium',
                        priceSignal === 'reliable'
                          ? 'bg-emerald-500/10 text-emerald-300'
                          : priceSignal === 'suspicious'
                            ? 'bg-yellow-500/10 text-yellow-200'
                            : 'bg-gray-800 text-gray-500',
                      ].join(' ')}
                    >
                      <span className="inline-flex items-center gap-1">
                        <SlidersHorizontal size={10} aria-hidden />
                        {Math.round(p.confidence_score * 100)}%
                      </span>
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
