import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import axios from 'axios';

import type { InvestorWatchlist, InvestorWatchlistUpdatePayload } from '../types/api';

const BASE_URL = '/api/v1';

async function fetchWatchlist(): Promise<InvestorWatchlist> {
  const { data } = await axios.get<InvestorWatchlist>(`${BASE_URL}/watchlist`);
  return data;
}

async function updateWatchlist(
  payload: InvestorWatchlistUpdatePayload,
): Promise<InvestorWatchlist> {
  const { data } = await axios.put<InvestorWatchlist>(`${BASE_URL}/watchlist`, payload);
  return data;
}

async function acknowledgeWatchlist(): Promise<InvestorWatchlist> {
  const { data } = await axios.post<InvestorWatchlist>(`${BASE_URL}/watchlist/acknowledge`);
  return data;
}

export function useWatchlist() {
  return useQuery<InvestorWatchlist, Error>({
    queryKey: ['watchlist'],
    queryFn: fetchWatchlist,
    staleTime: 30_000,
    gcTime: 5 * 60_000,
    retry: 2,
    refetchOnWindowFocus: false,
  });
}

export function useWatchlistUpdateMutation() {
  const queryClient = useQueryClient();
  return useMutation<InvestorWatchlist, Error, InvestorWatchlistUpdatePayload>({
    mutationFn: updateWatchlist,
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ['watchlist'] });
    },
  });
}

export function useWatchlistAcknowledgeMutation() {
  const queryClient = useQueryClient();
  return useMutation<InvestorWatchlist, Error, void>({
    mutationFn: acknowledgeWatchlist,
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ['watchlist'] });
    },
  });
}

