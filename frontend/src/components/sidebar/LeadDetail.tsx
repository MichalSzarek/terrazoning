/**
 * Lead detail panel — full breakdown of a selected investment lead.
 * Shown in the sidebar when operator clicks a parcel on the map or list.
 *
 * Displays: TERYT identifiers, area, coverage, zone designation,
 * confidence bar, and the full evidence chain.
 */

import {
  ArrowLeft,
  Building2,
  Coins,
  Copy,
  MapPin,
  Layers,
  Maximize2,
  Clock,
  Tag,
  ExternalLink,
  ShieldCheck,
} from 'lucide-react';
import { useEffect, useState } from 'react';
import type { EvidenceStepSource, LeadFeature } from '../../types/api';
import { useLeadStatusMutation, useMarketBenchmark } from '../../hooks/useLeads';
import { ConfidenceBadge } from '../ui/ConfidenceBadge';
import { EvidenceChain } from '../ui/EvidenceChain';
import {
  classifyPriceSignal,
  describeBenchmarkAvailability,
  describeCheapnessScore,
  formatAreaCompact,
  formatCurrencyPln,
  formatBenchmarkDelta,
  getFutureLeadInsight,
  getConfidenceBandDescription,
  getConfidenceBandLabel,
  getStrategyDescription,
  getStrategyLabel,
  priceSignalLabel,
} from '../../lib/investorMetrics';
import { futureBuildabilityEnabled } from '../../lib/featureFlags';

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

function InsightCard({
  label,
  value,
  hint,
  tone,
}: {
  label: string;
  value: React.ReactNode;
  hint: string;
  tone: 'sky' | 'amber' | 'cyan' | 'violet' | 'gray';
}) {
  const toneClasses: Record<'sky' | 'amber' | 'cyan' | 'violet' | 'gray', string> = {
    sky: 'border-sky-500/15 bg-sky-500/5 text-sky-300',
    amber: 'border-amber-500/15 bg-amber-500/5 text-amber-300',
    cyan: 'border-cyan-500/15 bg-cyan-500/5 text-cyan-300',
    violet: 'border-violet-500/15 bg-violet-500/5 text-violet-300',
    gray: 'border-gray-800 bg-gray-950/60 text-gray-300',
  };

  return (
    <div className={`rounded-xl border px-3 py-3 ${toneClasses[tone]}`}>
      <div className="text-[10px] uppercase tracking-wider text-gray-400">{label}</div>
      <div className="mt-2 text-sm font-semibold text-gray-100">{value}</div>
      <div className="mt-1 text-[11px] text-gray-500">{hint}</div>
    </div>
  );
}

function StrategyBadge({
  strategyType,
  confidenceBand,
}: {
  strategyType: 'current_buildable' | 'future_buildable';
  confidenceBand: 'formal' | 'supported' | 'speculative' | null;
}) {
  const base =
    strategyType === 'future_buildable'
      ? 'border-sky-500/30 bg-sky-500/10 text-sky-300'
      : 'border-emerald-500/30 bg-emerald-500/10 text-emerald-300';
  return (
    <div className="flex flex-col items-end gap-1 text-right">
      <div className="flex flex-wrap items-center justify-end gap-2">
        <span className={`inline-flex items-center rounded border px-2 py-0.5 text-[10px] font-medium uppercase tracking-wider ${base}`}>
          {getStrategyLabel(strategyType)}
        </span>
        {confidenceBand && (
          <span className="inline-flex items-center rounded border border-cyan-500/30 bg-cyan-500/10 px-2 py-0.5 text-[10px] font-medium uppercase tracking-wider text-cyan-200">
            {getConfidenceBandLabel(confidenceBand)}
          </span>
        )}
      </div>
      <p className="text-[10px] text-gray-500">
        {getStrategyDescription(strategyType)}
      </p>
      {confidenceBand && (
        <p className="text-[10px] text-gray-500">
          {getConfidenceBandDescription(confidenceBand)}
        </p>
      )}
    </div>
  );
}

async function copyTextToClipboard(text: string): Promise<void> {
  if (navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(text);
    return;
  }

  const textarea = document.createElement('textarea');
  textarea.value = text;
  textarea.setAttribute('readonly', 'true');
  textarea.style.position = 'absolute';
  textarea.style.left = '-9999px';
  document.body.appendChild(textarea);
  textarea.select();

  const copied = document.execCommand('copy');
  document.body.removeChild(textarea);

  if (!copied) {
    throw new Error('Clipboard copy failed');
  }
}

export function LeadDetail({ feature, onBack }: LeadDetailProps) {
  const p = feature.properties;
  const [notesDraft, setNotesDraft] = useState('');
  const [kwCopyStatus, setKwCopyStatus] = useState<'idle' | 'copied' | 'error'>('idle');
  const statusMutation = useLeadStatusMutation();
  const futureInsight = p.strategy_type === 'future_buildable' ? getFutureLeadInsight(p) : null;
  const benchmarkQuery = useMarketBenchmark(p.teryt_gmina, {
    enabled: futureBuildabilityEnabled && p.strategy_type === 'future_buildable',
  });
  const sourceStep = p.evidence_chain.find(
    (step): step is EvidenceStepSource => step.step === 'source',
  );
  const sourceUrl =
    p.source_url
    ?? sourceStep?.url
    ?? null;

  const formattedArea = p.area_m2 != null
    ? p.area_m2 >= 10_000
      ? `${(p.area_m2 / 10_000).toFixed(3)} ha (${Math.round(p.area_m2).toLocaleString('pl-PL')} m²)`
      : `${Math.round(p.area_m2).toLocaleString('pl-PL')} m²`
    : '—';
  const buildableArea = p.max_buildable_area_m2;
  const priceSignal = classifyPriceSignal(p);
  const benchmark = benchmarkQuery.data ?? null;
  const benchmarkSummary = describeBenchmarkAvailability(benchmark);
  const benchmarkDelta = formatBenchmarkDelta(p.price_per_m2_zl, benchmark);
  const diligenceLabel =
    p.quality_signal === 'missing_financials' ? 'brak metryk finansowych'
      : p.quality_signal === 'review_required' ? 'wymaga dodatkowej weryfikacji'
        : p.quality_signal === 'partial' ? 'część metryk niekompletna'
          : p.confidence_score >= 0.9 ? 'dane bardzo mocne'
            : p.confidence_score >= 0.75 ? 'dane wiarygodne'
              : 'wymaga dodatkowej weryfikacji';

  const createdAt = new Date(p.created_at).toLocaleDateString('pl-PL', {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });

  useEffect(() => {
    setNotesDraft('');
  }, [p.lead_id]);

  useEffect(() => {
    setKwCopyStatus('idle');
  }, [p.lead_id]);

  const workflowActions: Array<{ status: 'reviewed' | 'shortlisted' | 'rejected' | 'invested'; label: string }> = [
    { status: 'reviewed', label: 'Reviewed' },
    { status: 'shortlisted', label: 'Shortlist' },
    { status: 'rejected', label: 'Reject' },
    { status: 'invested', label: 'Invested' },
  ];

  const handleKwCopy = async () => {
    if (!p.kw_number) {
      return;
    }

    try {
      await copyTextToClipboard(p.kw_number);
      setKwCopyStatus('copied');
    } catch {
      setKwCopyStatus('error');
    }
  };

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
          <div className="flex items-center gap-2">
            <StrategyBadge strategyType={p.strategy_type} confidenceBand={p.confidence_band} />
            <StatusBadge status={p.status} />
          </div>
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
          {sourceUrl && (
            <a
              href={sourceUrl}
              target="_blank"
              rel="noopener noreferrer"
              className="mt-3 inline-flex items-center gap-1.5 rounded border border-blue-500/30 bg-blue-500/10 px-2.5 py-1.5 text-[11px] font-medium text-blue-300 transition-colors hover:border-blue-400/50 hover:bg-blue-500/15 hover:text-blue-200"
            >
              <ExternalLink size={11} aria-hidden />
              Otwórz aukcję
            </a>
          )}
          {p.kw_number && (
            <div className="mt-3 rounded-xl border border-cyan-500/15 bg-cyan-500/5 px-3 py-3">
              <p className="text-[10px] uppercase tracking-wider text-cyan-300">
                Księga wieczysta
              </p>
              <p className="mt-2 font-mono text-sm text-gray-100 break-all">
                {p.kw_number}
              </p>
              <div className="mt-3 flex flex-wrap items-center gap-2">
                <button
                  type="button"
                  onClick={() => void handleKwCopy()}
                  className="inline-flex items-center gap-1.5 rounded border border-gray-700 bg-gray-900 px-2.5 py-1.5 text-[11px] font-medium text-gray-200 transition-colors hover:border-gray-600 hover:bg-gray-800"
                >
                  <Copy size={11} aria-hidden />
                  Kopiuj KW
                </button>
                {p.ekw_search_url && (
                  <a
                    href={p.ekw_search_url}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="inline-flex items-center gap-1.5 rounded border border-cyan-500/30 bg-cyan-500/10 px-2.5 py-1.5 text-[11px] font-medium text-cyan-200 transition-colors hover:border-cyan-400/50 hover:bg-cyan-500/15"
                  >
                    <ExternalLink size={11} aria-hidden />
                    Otwórz EKW (beta)
                  </a>
                )}
                {kwCopyStatus === 'copied' && (
                  <span className="text-[11px] text-emerald-300">Skopiowano numer KW.</span>
                )}
                {kwCopyStatus === 'error' && (
                  <span className="text-[11px] text-amber-200">
                    Kopiowanie nie powiodło się. Skopiuj numer ręcznie.
                  </span>
                )}
              </div>
              <p className="mt-2 text-[11px] text-gray-500">
                Portal EKW jest zewnętrzny, a prefill działa w trybie best-effort. Jeśli formularz nie uzupełni się poprawnie, numer KW jest gotowy do wklejenia.
              </p>
            </div>
          )}
          <div className="mt-3">
            <ConfidenceBadge score={p.confidence_score} variant="bar" />
          </div>
          <div className="mt-3 grid grid-cols-1 gap-2 sm:grid-cols-2 xl:grid-cols-3">
            <div className="rounded-xl border border-emerald-500/15 bg-emerald-500/5 px-3 py-3">
              <div className="flex items-center gap-2 text-[10px] uppercase tracking-wider text-emerald-300">
                <Coins size={12} aria-hidden />
                Cena wejścia
              </div>
              <div className="mt-2 text-sm font-semibold text-gray-100">
                {formatCurrencyPln(p.price_zl, { compact: true })}
              </div>
              <div className="mt-1 text-[11px] text-gray-500">
                {p.price_per_m2_zl != null ? `${p.price_per_m2_zl.toFixed(2)} zł/m²` : 'brak pełnej ceny / m²'}
              </div>
              <div
                className={[
                  'mt-2 inline-flex rounded px-1.5 py-0.5 text-[10px] font-medium',
                  priceSignal === 'reliable'
                    ? 'bg-emerald-500/10 text-emerald-300'
                    : priceSignal === 'suspicious'
                      ? 'bg-yellow-500/10 text-yellow-200'
                      : 'bg-gray-800 text-gray-500',
                ].join(' ')}
              >
                {priceSignalLabel(priceSignal)}
              </div>
            </div>
            <div className="rounded-xl border border-amber-500/15 bg-amber-500/5 px-3 py-3">
              <div className="flex items-center gap-2 text-[10px] uppercase tracking-wider text-amber-300">
                <Building2 size={12} aria-hidden />
                Potencjał planu
              </div>
              <div className="mt-2 text-sm font-semibold text-gray-100">
                {buildableArea != null ? formatAreaCompact(buildableArea) : 'brak wyliczenia'}
              </div>
              <div className="mt-1 text-[11px] text-gray-500">
                {p.max_coverage_pct != null ? `${p.max_coverage_pct.toFixed(1)}% działki w strefie` : 'bez pokrycia MPZP'}
              </div>
            </div>
            <div className="rounded-xl border border-cyan-500/15 bg-cyan-500/5 px-3 py-3">
              <div className="flex items-center justify-between gap-2 text-[10px] uppercase tracking-wider text-cyan-300">
                <span>Benchmark rynku</span>
                <span className="text-cyan-200">
                  {benchmarkQuery.isFetching
                    ? 'sync…'
                    : benchmarkSummary.scopeLabel}
                </span>
              </div>
              <div className="mt-2 text-sm font-semibold text-gray-100">
                {benchmarkDelta}
              </div>
              <div className="mt-1 flex flex-wrap items-center gap-2 text-[11px] text-gray-500">
                <span>{benchmarkSummary.sampleLabel}</span>
                <span className={[
                  'rounded border px-1.5 py-0.5 uppercase tracking-wider',
                  benchmarkSummary.isReliable
                    ? 'border-emerald-500/20 bg-emerald-500/10 text-emerald-300'
                    : 'border-amber-500/20 bg-amber-500/10 text-amber-200',
                ].join(' ')}>
                  {benchmarkSummary.statusLabel}
                </span>
              </div>
              <div className="mt-1 text-[11px] text-gray-500">
                {benchmarkSummary.isReliable && benchmark?.median_price_per_m2_zl != null
                  ? `p25 ${benchmark.p25_price_per_m2_zl?.toFixed(0) ?? '—'} · p40 ${benchmark.p40_price_per_m2_zl?.toFixed(0) ?? '—'} · p50 ${benchmark.median_price_per_m2_zl.toFixed(0)} zł/m²`
                  : benchmarkSummary.statusHint}
              </div>
              {benchmarkQuery.isError && (
                <div className="mt-2 text-[11px] text-red-400">
                  Benchmark chwilowo niedostępny.
                </div>
              )}
            </div>
          </div>
          <div className="mt-4 rounded-xl border border-gray-800 bg-gray-950/60 px-3 py-3">
            <div className="flex items-center justify-between gap-2">
              <div>
                <p className="text-[10px] uppercase tracking-wider text-gray-500">
                  Workflow inwestora
                </p>
                <p className="mt-1 text-[11px] text-gray-500">
                  Oznacz lead jako sprawdzony, shortlistę lub inwestycję.
                </p>
              </div>
              {statusMutation.isPending && (
                <span className="text-[10px] text-amber-300">zapisywanie…</span>
              )}
            </div>
            <textarea
              value={notesDraft}
              onChange={(event) => setNotesDraft(event.target.value)}
              placeholder="Krótka notatka analityka, np. dostęp do drogi, ryzyko MPZP, kontakt do komornika…"
              className="mt-3 min-h-20 w-full rounded-lg border border-gray-800 bg-gray-900 px-3 py-2 text-[12px] text-gray-200 placeholder:text-gray-600 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-amber-500"
            />
            <div className="mt-3 grid grid-cols-2 gap-2">
              {workflowActions.map((action) => (
                <button
                  key={action.status}
                  type="button"
                  onClick={() => statusMutation.mutate({
                    leadId: p.lead_id,
                    payload: {
                      status: action.status,
                      notes: notesDraft.trim() || undefined,
                    },
                  })}
                  disabled={statusMutation.isPending || p.status === action.status}
                  className={[
                    'rounded-lg border px-2.5 py-2 text-[11px] font-medium transition-colors',
                    p.status === action.status
                      ? 'border-amber-500/40 bg-amber-500/10 text-amber-300'
                      : 'border-gray-800 bg-gray-900 text-gray-300 hover:border-gray-700 hover:bg-gray-800',
                    'disabled:cursor-not-allowed disabled:opacity-60',
                  ].join(' ')}
                >
                  {action.label}
                </button>
              ))}
            </div>
            {statusMutation.isError && (
              <p className="mt-2 text-[11px] text-red-400">{statusMutation.error.message}</p>
            )}
            {statusMutation.isSuccess && (
              <p className="mt-2 text-[11px] text-emerald-300">
                Status leada zaktualizowany.
              </p>
            )}
          </div>
        </div>

        {p.strategy_type === 'future_buildable' && (
          <div className="px-4 py-4 border-b border-gray-800">
            <p className="text-[10px] font-medium uppercase tracking-wider text-gray-600 mb-3">
              Dlaczego system uważa, że działka może wejść w budowlane
            </p>
            <div className="grid grid-cols-1 gap-2 sm:grid-cols-2 xl:grid-cols-3">
              <InsightCard
                label="Evidence tier"
                tone={futureInsight?.evidenceTierTone === 'formal'
                  ? 'sky'
                  : futureInsight?.evidenceTierTone === 'supported'
                    ? 'cyan'
                    : futureInsight?.evidenceTierTone === 'speculative'
                      ? 'violet'
                      : 'gray'}
                value={futureInsight?.evidenceTierLabel ?? '—'}
                hint={[
                  futureInsight?.evidenceTierHint ?? 'brak sygnałów',
                  p.confidence_band ? getConfidenceBandDescription(p.confidence_band) : null,
                ].filter(Boolean).join(' · ')}
              />
              <InsightCard
                label="Cheapness"
                tone="amber"
                value={p.cheapness_score != null ? `${p.cheapness_score.toFixed(0)} / 20` : '—'}
                hint={[
                  describeCheapnessScore(p.cheapness_score),
                  p.price_per_m2_zl != null
                    ? `${p.price_per_m2_zl.toFixed(2)} zł/m² · ${benchmarkDelta}`
                    : 'brak pełnej ceny',
                ].join(' · ')}
              />
              <InsightCard
                label="Next action"
                tone="cyan"
                value={futureInsight?.nextActionLabel ?? '—'}
                hint={[
                  futureInsight?.nextActionHint ?? 'brak rekomendacji',
                  futureInsight?.spatialContextLabel ?? null,
                ].filter(Boolean).join(' · ')}
              />
            </div>
            <div className="mt-3 rounded-xl border border-gray-800 bg-gray-950/60 px-3 py-3">
              <p className="text-[10px] uppercase tracking-wider text-gray-500">Signal breakdown</p>
              <div className="mt-2 space-y-2">
                {p.signal_breakdown.length > 0 ? p.signal_breakdown.map((signal, index) => (
                  <div key={`${signal.kind}-${index}`} className="rounded-lg border border-gray-800 bg-gray-900 px-3 py-2">
                    <div className="flex items-center justify-between gap-2">
                      <span className="text-[11px] font-medium text-gray-200">{signal.evidence_label ?? signal.kind}</span>
                      <span className={`text-[10px] font-mono ${signal.weight >= 0 ? 'text-emerald-300' : 'text-red-300'}`}>
                        {signal.weight >= 0 ? '+' : ''}{signal.weight.toFixed(0)}
                      </span>
                    </div>
                    <div className="mt-1 flex flex-wrap items-center gap-2 text-[10px] text-gray-500">
                      <span>{signal.status}</span>
                      {signal.designation_raw && <span>{signal.designation_raw}</span>}
                      {signal.designation_normalized && <span>{signal.designation_normalized}</span>}
                    </div>
                  </div>
                )) : (
                  <p className="text-[11px] text-gray-500">Brak sygnałów przyszłej urbanizacji.</p>
                )}
              </div>
            </div>
            <div className="mt-3 rounded-xl border border-gray-800 bg-gray-950/60 px-3 py-3">
              <p className="text-[10px] uppercase tracking-wider text-gray-500">Ryzyka</p>
              <ul className="mt-2 space-y-1 text-[11px] text-gray-400">
                <li>Formalne źródła planistyczne są ważniejsze niż heurystyki przestrzenne.</li>
                <li>Lead future_buildable nie oznacza obowiązującego prawa do zabudowy dziś.</li>
                <li>
                  Najwyższy priorytet mają geometryczne sygnały `studium` / `POG`; same uchwały i rejestry wymagają dalszego potwierdzenia.
                </li>
                {p.confidence_band === 'speculative' && (
                  <li>To lead spekulacyjny: wymaga ręcznego sprawdzenia źródeł gminnych przed decyzją.</li>
                )}
                {p.confidence_band === 'supported' && (
                  <li>To lead wspierany: sygnał formalny jest obecny, ale heurystyki przestrzenne nadal są częścią decyzji.</li>
                )}
              </ul>
            </div>
          </div>
        )}

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
          {buildableArea != null && (
            <StatRow
              icon={<Building2 size={10} aria-hidden />}
              label="Pow. budowlana"
              value={<span className="text-amber-300">{formatAreaCompact(buildableArea)}</span>}
            />
          )}
          {p.price_zl != null && (
            <StatRow
              icon={<Coins size={10} aria-hidden />}
              label="Cena wywoławcza"
              value={<span className="text-emerald-400">{formatCurrencyPln(p.price_zl)}</span>}
            />
          )}
          {p.price_per_m2_zl != null && (
            <StatRow
              icon={<Coins size={10} aria-hidden />}
              label="Cena / m²"
              value={<span className="text-emerald-400">{p.price_per_m2_zl.toFixed(2)} zł</span>}
            />
          )}
          <StatRow
            icon={<Coins size={10} aria-hidden />}
            label="Sygnał ceny"
            value={priceSignalLabel(priceSignal)}
          />
          <StatRow
            icon={<ShieldCheck size={10} aria-hidden />}
            label="Jakość danych"
            value={diligenceLabel}
          />
          {p.missing_metrics.length > 0 && (
            <StatRow
              icon={<Tag size={10} aria-hidden />}
              label="Brakujące metryki"
              value={p.missing_metrics.join(', ')}
            />
          )}
          <StatRow
            icon={<MapPin size={10} aria-hidden />}
            label="Priorytet"
            value={p.priority.toUpperCase()}
          />
          {p.distance_to_nearest_buildable_m != null && (
            <StatRow
              icon={<MapPin size={10} aria-hidden />}
              label="Najbliższa strefa bud."
              value={`${p.distance_to_nearest_buildable_m.toFixed(1)} m`}
            />
          )}
          {p.adjacent_buildable_pct != null && (
            <StatRow
              icon={<Layers size={10} aria-hidden />}
              label="Wspólna granica z bud."
              value={`${p.adjacent_buildable_pct.toFixed(1)}%`}
            />
          )}
          <StatRow
            icon={<Clock size={10} aria-hidden />}
            label="Wykryto"
            value={createdAt}
          />
          {sourceUrl && (
            <StatRow
              icon={<ExternalLink size={10} aria-hidden />}
              label="Źródło"
              value={
                <a
                  href={sourceUrl}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="truncate text-blue-400 hover:text-blue-300"
                  title={sourceUrl}
                >
                  aukcja komornicza
                </a>
              }
            />
          )}
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
