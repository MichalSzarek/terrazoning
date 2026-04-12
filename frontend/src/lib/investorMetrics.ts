import type {
  FutureConfidenceBand,
  LeadFeature,
  LeadProperties,
  LeadStrategyType,
  MarketBenchmarkResponse,
} from '../types/api';
import type {
  ConfidenceBandFilterKey,
  LeadSortKey,
  PriceFilterKey,
  StrategyFilterKey,
} from '../store/mapStore';

export function getBuildableAreaM2(areaM2: number | null, coveragePct: number | null): number | null {
  if (areaM2 == null || coveragePct == null) return null;
  return areaM2 * (coveragePct / 100);
}

export type PriceSignal = 'reliable' | 'suspicious' | 'missing';

export function classifyPriceSignal(
  lead: Pick<LeadProperties, 'price_zl' | 'price_per_m2_zl'> & Partial<Pick<LeadProperties, 'price_signal'>>,
): PriceSignal {
  if (lead.price_signal) return lead.price_signal;
  if (lead.price_zl == null && lead.price_per_m2_zl == null) return 'missing';
  if (lead.price_zl != null && lead.price_zl < 1000) return 'suspicious';
  if (lead.price_per_m2_zl != null && (lead.price_per_m2_zl < 5 || lead.price_per_m2_zl > 20000)) {
    return 'suspicious';
  }
  return 'reliable';
}

export function priceSignalLabel(signal: PriceSignal): string {
  if (signal === 'reliable') return 'cena wiarygodna';
  if (signal === 'suspicious') return 'cena do weryfikacji';
  return 'brak ceny';
}

const STRATEGY_META: Record<LeadStrategyType, { label: string; description: string }> = {
  current_buildable: {
    label: 'Dziś budowlane',
    description: 'przeznaczenie działa już teraz',
  },
  future_buildable: {
    label: 'Przyszłe budowlane',
    description: 'oparte o ścieżkę planistyczną',
  },
};

const CONFIDENCE_BAND_META: Record<NonNullable<FutureConfidenceBand>, { label: string; description: string }> = {
  formal: {
    label: 'Formalne',
    description: 'twarde źródła planistyczne',
  },
  supported: {
    label: 'Wspierane',
    description: 'kilka mocnych sygnałów',
  },
  speculative: {
    label: 'Spekulacyjne',
    description: 'heurystyka i ręczna weryfikacja',
  },
};

export function getStrategyLabel(strategy: LeadStrategyType): string {
  return STRATEGY_META[strategy].label;
}

export function getStrategyDescription(strategy: LeadStrategyType): string {
  return STRATEGY_META[strategy].description;
}

export function getConfidenceBandLabel(band: FutureConfidenceBand | null): string {
  if (!band) return 'brak bandu';
  return CONFIDENCE_BAND_META[band].label;
}

export function getConfidenceBandDescription(band: FutureConfidenceBand | null): string {
  if (!band) return 'brak klasyfikacji';
  return CONFIDENCE_BAND_META[band].description;
}

export function getBenchmarkScopeLabel(scope: MarketBenchmarkResponse['scope'] | null | undefined): string {
  if (scope === 'gmina') return 'benchmark gminny';
  if (scope === 'powiat') return 'benchmark powiatowy';
  if (scope === 'wojewodztwo') return 'benchmark wojewódzki';
  return 'benchmark rynku';
}

export interface BenchmarkDisplaySummary {
  scopeLabel: string;
  sampleLabel: string;
  statusLabel: string;
  statusHint: string;
  isReliable: boolean;
}

export function describeBenchmarkAvailability(
  benchmark: MarketBenchmarkResponse | null | undefined,
): BenchmarkDisplaySummary {
  if (!benchmark) {
    return {
      scopeLabel: 'brak benchmarku',
      sampleLabel: 'brak danych',
      statusLabel: 'brak benchmarku',
      statusHint: 'Nie pobrano jeszcze danych porównawczych dla tej gminy.',
      isReliable: false,
    };
  }

  const scopeLabel = getBenchmarkScopeLabel(benchmark.scope);
  if (benchmark.sample_size <= 0) {
    return {
      scopeLabel,
      sampleLabel: '0 próbek',
      statusLabel: 'brak próbek',
      statusHint: 'Benchmark istnieje, ale nie ma jeszcze próbek do porównania.',
      isReliable: false,
    };
  }

  if (benchmark.median_price_per_m2_zl == null || benchmark.p25_price_per_m2_zl == null || benchmark.p40_price_per_m2_zl == null) {
    return {
      scopeLabel,
      sampleLabel: `${benchmark.sample_size} próbek`,
      statusLabel: 'benchmark niepełny',
      statusHint: 'Zakres danych jest zbyt słaby, żeby uznać benchmark za wiarygodny.',
      isReliable: false,
    };
  }

  if (benchmark.sample_size < 5) {
    return {
      scopeLabel,
      sampleLabel: `${benchmark.sample_size} próbek`,
      statusLabel: 'benchmark słaby',
      statusHint: 'Za mało próbek, żeby traktować porównanie jako wiarygodne.',
      isReliable: false,
    };
  }

  return {
    scopeLabel,
    sampleLabel: `${benchmark.sample_size} próbek`,
    statusLabel: 'benchmark wiarygodny',
    statusHint: 'Dane porównawcze są wystarczająco stabilne do decyzji inwestorskiej.',
    isReliable: true,
  };
}

export interface FutureLeadInsight {
  evidenceTierLabel: string;
  evidenceTierTone: 'formal' | 'supported' | 'speculative' | 'current';
  evidenceTierHint: string;
  nextActionLabel: string;
  nextActionHint: string;
  spatialContextLabel: string;
  spatialContextHint: string;
}

function formatSignalCount(count: number): string {
  if (count === 1) return '1 sygnał';
  if (count >= 2 && count <= 4) return `${count} sygnały`;
  return `${count} sygnałów`;
}

function formatFutureSpatialContext(
  distanceToBuildableM: number | null | undefined,
  adjacentBuildablePct: number | null | undefined,
): { label: string; hint: string } {
  const distanceLabel = distanceToBuildableM != null
    ? `${distanceToBuildableM < 100 ? distanceToBuildableM.toFixed(0) : Math.round(distanceToBuildableM)} m`
    : null;
  const adjacencyLabel = adjacentBuildablePct != null
    ? `${adjacentBuildablePct.toFixed(0)}% granicy`
    : null;

  if (distanceLabel && adjacencyLabel) {
    return {
      label: `${distanceLabel} · ${adjacencyLabel}`,
      hint: 'przybliżenie do obecnej budowlanki',
    };
  }

  if (distanceLabel) {
    return {
      label: distanceLabel,
      hint: 'odległość do strefy budowlanej',
    };
  }

  if (adjacencyLabel) {
    return {
      label: adjacencyLabel,
      hint: 'kontakt z budowlaną granicą',
    };
  }

  return {
    label: 'brak sygnału przestrzennego',
    hint: 'brak dystansu lub granicy do budowlanki',
  };
}

export function getFutureLeadInsight(
  lead: Pick<
    LeadProperties,
    | 'strategy_type'
    | 'confidence_band'
    | 'signal_breakdown'
    | 'dominant_future_signal'
    | 'distance_to_nearest_buildable_m'
    | 'adjacent_buildable_pct'
  >,
): FutureLeadInsight {
  if (lead.strategy_type !== 'future_buildable') {
    return {
      evidenceTierLabel: 'Current buildable',
      evidenceTierTone: 'current',
      evidenceTierHint: 'prawo działa dziś',
      nextActionLabel: 'Brak',
      nextActionHint: 'Lead już działa w obecnym torze',
      spatialContextLabel: 'current_buildable',
      spatialContextHint: 'nie dotyczy future buildable',
    };
  }

  const signalCount = lead.signal_breakdown.length;
  const dominant = lead.dominant_future_signal?.trim() ?? '';
  const hasSpatialAnchor =
    (lead.distance_to_nearest_buildable_m != null && lead.distance_to_nearest_buildable_m <= 120) ||
    (lead.adjacent_buildable_pct != null && lead.adjacent_buildable_pct >= 10);
  const isVeryClose =
    (lead.distance_to_nearest_buildable_m != null && lead.distance_to_nearest_buildable_m <= 75) ||
    (lead.adjacent_buildable_pct != null && lead.adjacent_buildable_pct >= 20);

  const evidenceTierTone = lead.confidence_band ?? 'speculative';
  const evidenceTierLabel =
    lead.confidence_band === 'formal'
      ? 'Formal evidence'
      : lead.confidence_band === 'supported'
        ? 'Supported evidence'
        : lead.confidence_band === 'speculative'
          ? 'Speculative'
          : 'Future buildable';
  const evidenceTierHint = [
    signalCount > 0 ? formatSignalCount(signalCount) : 'brak sygnałów',
    dominant ? `dom: ${dominant}` : null,
  ].filter(Boolean).join(' · ');

  const nextActionLabel =
    lead.confidence_band === 'formal'
      ? (isVeryClose ? 'Shortlist' : 'Verify source')
      : lead.confidence_band === 'supported'
        ? 'Manual review'
        : 'Manual check';
  const nextActionHint =
    lead.confidence_band === 'formal'
      ? (isVeryClose
        ? 'blisko budowlanki, sprawdź operat i dojazd'
        : 'potwierdź źródło i granice planu')
      : lead.confidence_band === 'supported'
        ? (hasSpatialAnchor
          ? 'formalny sygnał jest obecny, dociśnij heurystyki'
          : 'formalny sygnał wymaga potwierdzenia przestrzennego')
        : 'bez ręcznego sprawdzenia źródeł nie awansować';

  const spatial = formatFutureSpatialContext(
    lead.distance_to_nearest_buildable_m,
    lead.adjacent_buildable_pct,
  );

  return {
    evidenceTierLabel,
    evidenceTierTone,
    evidenceTierHint,
    nextActionLabel,
    nextActionHint,
    spatialContextLabel: spatial.label,
    spatialContextHint: spatial.hint,
  };
}

export function describeCheapnessScore(score: number | null | undefined): string {
  if (score == null || Number.isNaN(score)) return 'brak cheapness';
  if (score >= 20) return 'mocny dyskont vs benchmark';
  if (score >= 10) return 'dyskont vs benchmark';
  return 'bez dyskonta';
}

export function formatBenchmarkDelta(
  pricePerM2: number | null | undefined,
  benchmark: MarketBenchmarkResponse | null | undefined,
): string {
  const availability = describeBenchmarkAvailability(benchmark);
  if (!availability.isReliable || pricePerM2 == null || benchmark?.median_price_per_m2_zl == null || benchmark.median_price_per_m2_zl <= 0) {
    return 'brak wiarygodnego benchmarku';
  }

  const median = benchmark.median_price_per_m2_zl;
  const deltaPct = ((median - pricePerM2) / median) * 100;
  const rounded = Math.abs(deltaPct).toFixed(0);
  return deltaPct >= 0 ? `${rounded}% poniżej mediany` : `${rounded}% powyżej mediany`;
}

function median(values: number[]): number | null {
  if (values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0
    ? (sorted[mid - 1]! + sorted[mid]!) / 2
    : sorted[mid]!;
}

export function formatCurrencyPln(value: number | null | undefined, options?: {
  maximumFractionDigits?: number;
  compact?: boolean;
}): string {
  if (value == null || Number.isNaN(value)) return '—';

  if (options?.compact && Math.abs(value) >= 1000) {
    const compactValue = value >= 1_000_000
      ? `${(value / 1_000_000).toFixed(2)} mln zł`
      : `${(value / 1_000).toFixed(1)} tys. zł`;
    return compactValue.replace('.', ',');
  }

  return new Intl.NumberFormat('pl-PL', {
    style: 'currency',
    currency: 'PLN',
    maximumFractionDigits: options?.maximumFractionDigits ?? 0,
  }).format(value);
}

export function formatAreaCompact(areaM2: number | null | undefined): string {
  if (areaM2 == null || Number.isNaN(areaM2)) return '—';
  if (areaM2 >= 10_000) {
    return `${(areaM2 / 10_000).toFixed(2).replace('.', ',')} ha`;
  }
  return `${Math.round(areaM2).toLocaleString('pl-PL')} m²`;
}

export interface InvestorSnapshotStats {
  visibleCount: number;
  currentBuildableCount: number;
  futureBuildableCount: number;
  futureFormalCount: number;
  futureSupportedCount: number;
  futureSpeculativeCount: number;
  withPriceCount: number;
  reliablePriceCount: number;
  primeCount: number;
  bestPricePerM2: number | null;
  medianPricePerM2: number | null;
  medianCheapnessScore: number | null;
  cheapestEntryPrice: number | null;
  averageCoveragePct: number | null;
}

export function computeInvestorSnapshot(features: LeadFeature[]): InvestorSnapshotStats {
  const props = features.map((feature) => feature.properties);
  const priced = props.filter((lead) => lead.price_per_m2_zl != null);
  const reliablePriced = props.filter((lead) => classifyPriceSignal(lead) === 'reliable');
  const withCoverage = props.filter((lead) => lead.max_coverage_pct != null);
  const currentBuildable = props.filter((lead) => lead.strategy_type === 'current_buildable');
  const futureBuildable = props.filter((lead) => lead.strategy_type === 'future_buildable');
  const futureCheapness = futureBuildable
    .filter((lead) => lead.cheapness_score != null)
    .map((lead) => lead.cheapness_score as number);

  return {
    visibleCount: props.length,
    currentBuildableCount: currentBuildable.length,
    futureBuildableCount: futureBuildable.length,
    futureFormalCount: futureBuildable.filter((lead) => lead.confidence_band === 'formal').length,
    futureSupportedCount: futureBuildable.filter((lead) => lead.confidence_band === 'supported').length,
    futureSpeculativeCount: futureBuildable.filter((lead) => lead.confidence_band === 'speculative').length,
    withPriceCount: priced.length,
    reliablePriceCount: reliablePriced.length,
    primeCount: props.filter((lead) => lead.confidence_score >= 0.9).length,
    bestPricePerM2: reliablePriced.filter((lead) => lead.price_per_m2_zl != null).length > 0
      ? Math.min(...reliablePriced
        .filter((lead) => lead.price_per_m2_zl != null)
        .map((lead) => lead.price_per_m2_zl as number))
      : null,
    medianPricePerM2: median(
      reliablePriced
        .filter((lead) => lead.price_per_m2_zl != null)
        .map((lead) => lead.price_per_m2_zl as number),
    ),
    medianCheapnessScore: median(futureCheapness),
    cheapestEntryPrice: reliablePriced
      .filter((lead) => lead.price_zl != null)
      .reduce<number | null>((best, lead) => {
        const price = lead.price_zl as number;
        if (best == null) return price;
        return price < best ? price : best;
      }, null),
    averageCoveragePct: withCoverage.length > 0
      ? withCoverage.reduce((sum, lead) => sum + (lead.max_coverage_pct as number), 0) / withCoverage.length
      : null,
  };
}

export function getLeadHeadlineMetric(lead: LeadProperties): string {
  if (lead.strategy_type === 'future_buildable' && lead.overall_score != null) {
    return `${lead.overall_score.toFixed(0)} / 100`;
  }
  if (lead.price_per_m2_zl != null) {
    return `${lead.price_per_m2_zl.toFixed(0)} zł/m²`;
  }
  if (lead.price_zl != null) {
    return formatCurrencyPln(lead.price_zl, { compact: true });
  }
  return 'Brak ceny';
}

export function formatInvestmentScore(score: number | null | undefined): string {
  if (score == null || Number.isNaN(score)) return '—';
  return `${score.toFixed(0)} / 100`;
}

export function filterLeadFeaturesForView(
  features: LeadFeature[],
  strategyFilter: StrategyFilterKey,
  confidenceBandFilter: ConfidenceBandFilterKey,
  futureBuildabilityEnabled: boolean,
): LeadFeature[] {
  if (!futureBuildabilityEnabled) return features;

  const hideSpeculative = strategyFilter === 'future_buildable' && confidenceBandFilter === 'all';
  if (!hideSpeculative) return features;

  return features.filter((feature) => {
    const { strategy_type, confidence_band } = feature.properties;
    return strategy_type !== 'future_buildable' || confidence_band !== 'speculative';
  });
}

function compareNullableNumberAsc(a: number | null | undefined, b: number | null | undefined): number {
  if (a == null && b == null) return 0;
  if (a == null) return 1;
  if (b == null) return -1;
  return a - b;
}

function compareNullableNumberDesc(a: number | null | undefined, b: number | null | undefined): number {
  if (a == null && b == null) return 0;
  if (a == null) return 1;
  if (b == null) return -1;
  return b - a;
}

export function sortLeadFeatures(features: LeadFeature[], sortKey: LeadSortKey): LeadFeature[] {
  const sorted = [...features];

  sorted.sort((left, right) => {
    const a = left.properties;
    const b = right.properties;

    if (sortKey === 'investment_score_desc') {
      const scoreDelta = compareNullableNumberDesc(a.investment_score, b.investment_score);
      if (scoreDelta !== 0) return scoreDelta;
    }

    if (sortKey === 'price_per_m2_asc') {
      const signalDelta = Number(classifyPriceSignal(a) === 'reliable') - Number(classifyPriceSignal(b) === 'reliable');
      if (signalDelta !== 0) return -signalDelta;
      const priceDelta = compareNullableNumberAsc(a.price_per_m2_zl, b.price_per_m2_zl);
      if (priceDelta !== 0) return priceDelta;
    }

    if (sortKey === 'entry_price_asc') {
      const signalDelta = Number(classifyPriceSignal(a) === 'reliable') - Number(classifyPriceSignal(b) === 'reliable');
      if (signalDelta !== 0) return -signalDelta;
      const priceDelta = compareNullableNumberAsc(a.price_zl, b.price_zl);
      if (priceDelta !== 0) return priceDelta;
    }

    if (sortKey === 'buildable_area_desc') {
      const areaDelta = compareNullableNumberDesc(a.max_buildable_area_m2, b.max_buildable_area_m2);
      if (areaDelta !== 0) return areaDelta;
      const coverageDelta = compareNullableNumberDesc(a.max_coverage_pct, b.max_coverage_pct);
      if (coverageDelta !== 0) return coverageDelta;
    }

    const confidenceDelta = b.confidence_score - a.confidence_score;
    if (confidenceDelta !== 0) return confidenceDelta;

    return compareNullableNumberDesc(a.max_coverage_pct, b.max_coverage_pct);
  });

  return sorted;
}

export function filterLeadFeaturesByPrice(features: LeadFeature[], filter: PriceFilterKey): LeadFeature[] {
  if (filter === 'all') {
    return features;
  }

  return features.filter((feature) => classifyPriceSignal(feature.properties) === filter);
}
