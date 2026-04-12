import type { LeadFeature, LeadStrategyType } from '../types/api';
import {
  getConfidenceBandLabel,
  getStrategyLabel,
  priceSignalLabel,
} from './investorMetrics';

export interface ExportShortlistOptions {
  strategyType?: LeadStrategyType;
  confidenceBands?: Array<'formal' | 'supported' | 'speculative'>;
  filename?: string;
}

function csvEscape(value: string): string {
  if (value.includes('"') || value.includes(',') || value.includes('\n')) {
    return `"${value.replaceAll('"', '""')}"`;
  }
  return value;
}

export function exportShortlistCsv(features: LeadFeature[], options?: ExportShortlistOptions): void {
  const filteredFeatures = features.filter((feature) => {
    if (options?.strategyType && feature.properties.strategy_type !== options.strategyType) {
      return false;
    }
    if (options?.confidenceBands?.length) {
      const band = feature.properties.confidence_band;
      if (!band || !options.confidenceBands.includes(band)) {
        return false;
      }
    }
    return true;
  });

  const header = [
    'identyfikator',
    'status',
    'strategy_label',
    'reviewed_at',
    'priority',
    'strategy_type',
    'confidence_band_label',
    'confidence_band',
    'confidence_score',
    'investment_score',
    'future_signal_score',
    'cheapness_score',
    'overall_score',
    'price_signal_label',
    'price_signal',
    'quality_signal',
    'price_zl',
    'price_per_m2_zl',
    'area_m2',
    'max_buildable_area_m2',
    'max_coverage_pct',
    'dominant_przeznaczenie',
    'teryt_gmina',
    'source_url',
    'notes',
  ];

  const rows = filteredFeatures.map((feature) => {
    const p = feature.properties;
    return [
      p.identyfikator,
      p.status,
      getStrategyLabel(p.strategy_type),
      p.reviewed_at ?? '',
      p.priority,
      p.strategy_type,
      getConfidenceBandLabel(p.confidence_band),
      p.confidence_band ?? '',
      String(p.confidence_score),
      String(p.investment_score),
      p.future_signal_score != null ? String(p.future_signal_score) : '',
      p.cheapness_score != null ? String(p.cheapness_score) : '',
      p.overall_score != null ? String(p.overall_score) : '',
      priceSignalLabel(p.price_signal),
      p.price_signal,
      p.quality_signal,
      p.price_zl != null ? String(p.price_zl) : '',
      p.price_per_m2_zl != null ? String(p.price_per_m2_zl) : '',
      p.area_m2 != null ? String(p.area_m2) : '',
      p.max_buildable_area_m2 != null ? String(p.max_buildable_area_m2) : '',
      p.max_coverage_pct != null ? String(p.max_coverage_pct) : '',
      p.dominant_przeznaczenie ?? '',
      p.teryt_gmina,
      p.source_url ?? '',
      p.notes ?? '',
    ].map((value) => csvEscape(value));
  });

  const csv = [header.join(','), ...rows.map((row) => row.join(','))].join('\n');
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.download = options?.filename ?? 'terrazoning-shortlist.csv';
  document.body.appendChild(anchor);
  anchor.click();
  document.body.removeChild(anchor);
  URL.revokeObjectURL(url);
}
