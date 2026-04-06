/**
 * TanStack Query hook for fetching investment leads.
 *
 * staleTime: 30s — leads data is refreshed by the backend pipeline,
 * not by the user. Short stale time keeps the map in sync with new runs.
 * gcTime: 5min — keep cached data for navigating back without re-fetch.
 */

import { useQuery } from '@tanstack/react-query';
import axios from 'axios';

import type { LeadsFeatureCollection, LeadsQueryParams } from '../types/api';

const BASE_URL = '/api/v1';

async function fetchLeads(params: LeadsQueryParams): Promise<LeadsFeatureCollection> {
  const { data } = await axios.get<LeadsFeatureCollection>(`${BASE_URL}/leads`, {
    params: {
      min_score: params.min_score ?? 0.7,
      limit: params.limit ?? 100,
      ...(params.include_count ? { include_count: true } : {}),
    },
  });
  return data;
}

export function useLeads(params: LeadsQueryParams = {}) {
  return useQuery<LeadsFeatureCollection, Error>({
    queryKey: ['leads', params],
    queryFn: () => fetchLeads(params),
    staleTime: 30_000,         // 30s
    gcTime: 5 * 60_000,        // 5min
    retry: 2,
    refetchOnWindowFocus: false,
  });
}
