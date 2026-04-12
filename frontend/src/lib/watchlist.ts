import type { InvestorWatchlist, InvestorWatchlistUpdatePayload, LeadFeature, LeadProperties } from '../types/api';
import { classifyPriceSignal } from './investorMetrics';

export interface WatchlistCriteria {
  maxPricePerM2: number | null;
  minCoveragePct: number;
  minConfidencePct: number;
  requiredDesignation: string;
  onlyReliablePrice: boolean;
  acknowledgedAt: string | null;
}

export type WatchlistMode = 'standard' | 'future_buildable';

export const FUTURE_BUILDABLE_WATCHLIST_PRESET: Omit<WatchlistCriteria, 'acknowledgedAt'> = {
  maxPricePerM2: null,
  minCoveragePct: 60,
  minConfidencePct: 60,
  requiredDesignation: '',
  onlyReliablePrice: true,
};

export const DEFAULT_WATCHLIST_CRITERIA: WatchlistCriteria = {
  maxPricePerM2: 150,
  minCoveragePct: 60,
  minConfidencePct: 80,
  requiredDesignation: '',
  onlyReliablePrice: true,
  acknowledgedAt: null,
};

export function fromApiWatchlist(api: InvestorWatchlist): WatchlistCriteria {
  return {
    maxPricePerM2: api.max_price_per_m2,
    minCoveragePct: api.min_coverage_pct,
    minConfidencePct: api.min_confidence_pct,
    requiredDesignation: api.required_designation,
    onlyReliablePrice: api.only_reliable_price,
    acknowledgedAt: api.acknowledged_at,
  };
}

export function toApiWatchlist(criteria: WatchlistCriteria): InvestorWatchlistUpdatePayload {
  return {
    max_price_per_m2: criteria.maxPricePerM2,
    min_coverage_pct: criteria.minCoveragePct,
    min_confidence_pct: criteria.minConfidencePct,
    required_designation: criteria.requiredDesignation,
    only_reliable_price: criteria.onlyReliablePrice,
    acknowledged_at: criteria.acknowledgedAt,
  };
}

function normalizeDesignation(value: string | null | undefined): string {
  return (value ?? '').trim().toUpperCase();
}

function normalizeFutureDesignation(lead: Pick<LeadProperties, 'dominant_future_signal' | 'dominant_przeznaczenie'>): string {
  return normalizeDesignation(lead.dominant_future_signal ?? lead.dominant_przeznaczenie);
}

export function isWatchlistMatch(
  lead: Pick<
    LeadProperties,
    | 'strategy_type'
    | 'confidence_score'
    | 'max_coverage_pct'
    | 'price_per_m2_zl'
    | 'dominant_przeznaczenie'
    | 'dominant_future_signal'
    | 'overall_score'
    | 'price_zl'
  >,
  criteria: WatchlistCriteria,
  mode: WatchlistMode = 'standard',
): boolean {
  const scoreValue = mode === 'future_buildable'
    ? (lead.overall_score ?? lead.confidence_score * 100)
    : (lead.confidence_score * 100);

  if (scoreValue < criteria.minConfidencePct) {
    return false;
  }

  if (mode === 'standard') {
    if ((lead.max_coverage_pct ?? 0) < criteria.minCoveragePct) {
      return false;
    }
  } else if (lead.strategy_type !== 'future_buildable') {
    return false;
  } else {
    const futureScore = lead.overall_score ?? (lead.confidence_score * 100);
    if (futureScore < criteria.minCoveragePct) {
      return false;
    }
  }

  if (criteria.onlyReliablePrice && classifyPriceSignal(lead) !== 'reliable') {
    return false;
  }

  if (criteria.maxPricePerM2 != null) {
    if (lead.price_per_m2_zl == null || lead.price_per_m2_zl > criteria.maxPricePerM2) {
      return false;
    }
  }

  const requiredDesignation = normalizeDesignation(criteria.requiredDesignation);
  if (!requiredDesignation) {
    return true;
  }

  const designation = mode === 'future_buildable'
    ? normalizeFutureDesignation(lead)
    : normalizeDesignation(lead.dominant_przeznaczenie);
  return designation.includes(requiredDesignation);
}

export function getWatchlistMatches(
  features: LeadFeature[],
  criteria: WatchlistCriteria,
  mode: WatchlistMode = 'standard',
): LeadFeature[] {
  return [...features]
    .filter((feature) => isWatchlistMatch(feature.properties, criteria, mode))
    .sort((left, right) => {
      const a = left.properties;
      const b = right.properties;

      if (mode === 'future_buildable') {
        const aOverall = a.overall_score ?? a.confidence_score * 100;
        const bOverall = b.overall_score ?? b.confidence_score * 100;
        if (aOverall !== bOverall) {
          return bOverall - aOverall;
        }
      }

      const aCreatedAt = Date.parse(a.created_at);
      const bCreatedAt = Date.parse(b.created_at);
      if (!Number.isNaN(aCreatedAt) && !Number.isNaN(bCreatedAt) && aCreatedAt !== bCreatedAt) {
        return bCreatedAt - aCreatedAt;
      }

      if (a.price_per_m2_zl != null && b.price_per_m2_zl != null && a.price_per_m2_zl !== b.price_per_m2_zl) {
        return a.price_per_m2_zl - b.price_per_m2_zl;
      }

      return b.confidence_score - a.confidence_score;
    });
}

export function getNewWatchlistMatches(
  features: LeadFeature[],
  criteria: WatchlistCriteria,
): LeadFeature[] {
  if (!criteria.acknowledgedAt) {
    return features;
  }

  const acknowledgedAt = Date.parse(criteria.acknowledgedAt);
  if (Number.isNaN(acknowledgedAt)) {
    return features;
  }

  return features.filter((feature) => {
    const createdAt = Date.parse(feature.properties.created_at);
    if (Number.isNaN(createdAt)) {
      return false;
    }
    return createdAt > acknowledgedAt;
  });
}
