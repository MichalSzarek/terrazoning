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

const BASE_URL = '/api/v1';

async function fetchLeads(params: LeadsQueryParams): Promise<LeadsFeatureCollection> {
  const { data } = await axios.get<LeadsFeatureCollection>(`${BASE_URL}/leads`, {
    params: {
      min_score: params.min_score ?? 0.7,
      limit: params.limit ?? 100,
      ...(params.status_filter ? { status_filter: params.status_filter } : {}),
      ...(params.strategy_filter ? { strategy_filter: params.strategy_filter } : {}),
      ...(params.confidence_band_filter ? { confidence_band_filter: params.confidence_band_filter } : {}),
      ...(params.cheap_only ? { cheap_only: true } : {}),
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
