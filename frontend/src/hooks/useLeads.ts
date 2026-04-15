/**
 * TanStack Query hook for fetching investment leads.
 *
 * staleTime: 30s — leads data is refreshed by the backend pipeline,
 * not by the user. Short stale time keeps the map in sync with new runs.
 * gcTime: 5min — keep cached data for navigating back without re-fetch.
 */

import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import axios from 'axios';

import type {
  LeadsFeatureCollection,
  LeadsQueryParams,
  LeadStatusUpdatePayload,
  LeadStatusUpdateResponse,
  MarketBenchmarkResponse,
} from '../types/api';
import { buildApiPath } from '../lib/apiBase';

const BASE_URL = buildApiPath('/api/v1');

async function fetchLeads(params: LeadsQueryParams): Promise<LeadsFeatureCollection> {
  const { data } = await axios.get<LeadsFeatureCollection>(`${BASE_URL}/leads`, {
    params: {
      min_score: params.min_score ?? 0.7,
      limit: params.limit ?? 100,
      ...(params.status_filter ? { status_filter: params.status_filter } : {}),
      ...(params.strategy_filter ? { strategy_filter: params.strategy_filter } : {}),
      ...(params.confidence_band_filter ? { confidence_band_filter: params.confidence_band_filter } : {}),
      ...(params.cheap_only ? { cheap_only: true } : {}),
      ...(params.min_price_zl != null ? { min_price_zl: params.min_price_zl } : {}),
      ...(params.max_price_zl != null ? { max_price_zl: params.max_price_zl } : {}),
      ...(params.min_price_per_m2_zl != null ? { min_price_per_m2_zl: params.min_price_per_m2_zl } : {}),
      ...(params.max_price_per_m2_zl != null ? { max_price_per_m2_zl: params.max_price_per_m2_zl } : {}),
      ...(params.min_area_m2 != null ? { min_area_m2: params.min_area_m2 } : {}),
      ...(params.max_area_m2 != null ? { max_area_m2: params.max_area_m2 } : {}),
      ...(params.min_coverage_pct != null ? { min_coverage_pct: params.min_coverage_pct } : {}),
      ...(params.min_buildable_area_m2 != null ? { min_buildable_area_m2: params.min_buildable_area_m2 } : {}),
      ...(params.teryt_prefix ? { teryt_prefix: params.teryt_prefix } : {}),
      ...(params.teryt_gmina ? { teryt_gmina: params.teryt_gmina } : {}),
      ...(params.designation ? { designation: params.designation } : {}),
      ...(params.search ? { search: params.search } : {}),
      ...(params.include_count ? { include_count: true } : {}),
    },
  });
  return data;
}

export function useLeads(
  params: LeadsQueryParams = {},
  options?: { enabled?: boolean },
) {
  return useQuery<LeadsFeatureCollection, Error>({
    queryKey: ['leads', params],
    queryFn: () => fetchLeads(params),
    staleTime: 30_000,         // 30s
    gcTime: 5 * 60_000,        // 5min
    retry: 2,
    refetchOnWindowFocus: false,
    enabled: options?.enabled ?? true,
  });
}

async function fetchMarketBenchmark(terytGmina: string): Promise<MarketBenchmarkResponse> {
  const { data } = await axios.get<MarketBenchmarkResponse>(`${BASE_URL}/leads/market_benchmarks`, {
    params: { teryt_gmina: terytGmina },
  });
  return data;
}

export function useMarketBenchmark(
  terytGmina: string | null | undefined,
  options?: { enabled?: boolean },
) {
  return useQuery<MarketBenchmarkResponse, Error>({
    queryKey: ['market-benchmark', terytGmina],
    queryFn: () => fetchMarketBenchmark(terytGmina as string),
    enabled: Boolean(terytGmina) && (options?.enabled ?? true),
    staleTime: 5 * 60_000,
    gcTime: 30 * 60_000,
    retry: 1,
    refetchOnWindowFocus: false,
  });
}

async function updateLeadStatus(
  leadId: string,
  payload: LeadStatusUpdatePayload,
): Promise<LeadStatusUpdateResponse> {
  const { data } = await axios.patch<LeadStatusUpdateResponse>(
    `${BASE_URL}/leads/${encodeURIComponent(leadId)}/status`,
    payload,
  );
  return data;
}

export function useLeadStatusMutation() {
  const queryClient = useQueryClient();

  return useMutation<
    LeadStatusUpdateResponse,
    Error,
    { leadId: string; payload: LeadStatusUpdatePayload }
  >({
    mutationFn: ({ leadId, payload }) => updateLeadStatus(leadId, payload),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ['leads'] });
    },
  });
}
