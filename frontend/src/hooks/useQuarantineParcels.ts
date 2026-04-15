import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import axios from 'axios';
import type { Feature, FeatureCollection, Geometry } from 'geojson';

import type {
  ManualOverridePayload,
  QuarantineParcelFeature,
  QuarantineParcelFeatureCollection,
  QuarantineParcelProperties,
} from '../types/api';
import { buildApiPath } from '../lib/apiBase';

const BASE_URL = buildApiPath('/api/v1');

function asString(value: unknown): string | null {
  return typeof value === 'string' && value.trim() !== '' ? value : null;
}

function asNumber(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function normalizeProperties(raw: Record<string, unknown>): QuarantineParcelProperties {
  return {
    dzialka_id:
      asString(raw['dzialka_id'])
      ?? asString(raw['id'])
      ?? asString(raw['parcel_id'])
      ?? '',
    identyfikator:
      asString(raw['identyfikator'])
      ?? asString(raw['parcel_identifier'])
      ?? asString(raw['dzialka_identyfikator'])
      ?? asString(raw['dzialka_id'])
      ?? '—',
    teryt_gmina:
      asString(raw['teryt_gmina'])
      ?? asString(raw['gmina_teryt']),
    area_m2:
      asNumber(raw['area_m2'])
      ?? asNumber(raw['powierzchnia_m2']),
    source_url:
      asString(raw['source_url'])
      ?? asString(raw['listing_url'])
      ?? asString(raw['url']),
    reason:
      asString(raw['reason'])
      ?? asString(raw['quarantine_reason'])
      ?? asString(raw['last_error']),
    status:
      asString(raw['status'])
      ?? asString(raw['state']),
    current_use:
      asString(raw['current_use'])
      ?? asString(raw['uzytek']),
    dominant_przeznaczenie:
      asString(raw['dominant_przeznaczenie'])
      ?? asString(raw['przeznaczenie']),
    manual_przeznaczenie:
      asString(raw['manual_przeznaczenie'])
      ?? asString(raw['manual_override']),
    created_at:
      asString(raw['created_at'])
      ?? asString(raw['updated_at']),
  };
}

function isFeatureCollection(value: unknown): value is FeatureCollection<Geometry, Record<string, unknown>> {
  if (!value || typeof value !== 'object') return false;
  const candidate = value as Record<string, unknown>;
  return candidate['type'] === 'FeatureCollection' && Array.isArray(candidate['features']);
}

function isFeature(value: unknown): value is Feature<Geometry, Record<string, unknown>> {
  if (!value || typeof value !== 'object') return false;
  const candidate = value as Record<string, unknown>;
  return candidate['type'] === 'Feature' && typeof candidate['properties'] === 'object';
}

function normalizeFeature(rawFeature: Feature<Geometry, Record<string, unknown>>): QuarantineParcelFeature {
  return {
    ...rawFeature,
    properties: normalizeProperties(rawFeature.properties),
  };
}

function normalizeQuarantineCollection(raw: unknown): QuarantineParcelFeatureCollection {
  if (isFeatureCollection(raw)) {
    const features = raw.features
      .filter(isFeature)
      .map(normalizeFeature)
      .filter((feature) => feature.properties.dzialka_id !== '');

    return {
      ...raw,
      features,
      count: typeof (raw as unknown as Record<string, unknown>)['count'] === 'number'
        ? ((raw as unknown as Record<string, unknown>)['count'] as number)
        : features.length,
    };
  }

  const container = raw && typeof raw === 'object' ? raw as Record<string, unknown> : {};
  const maybeFeatures = Array.isArray(container['features'])
    ? container['features']
    : Array.isArray(container['items'])
      ? container['items']
      : Array.isArray(container['parcels'])
        ? container['parcels']
        : [];

  const features = maybeFeatures
    .filter(isFeature)
    .map(normalizeFeature)
    .filter((feature) => feature.properties.dzialka_id !== '');

  return {
    type: 'FeatureCollection',
    features,
    count: features.length,
  };
}

async function fetchQuarantineParcels(): Promise<QuarantineParcelFeatureCollection> {
  const { data } = await axios.get<unknown>(`${BASE_URL}/quarantine_parcels`);
  return normalizeQuarantineCollection(data);
}

async function submitManualOverride(
  dzialkaId: string,
  payload: ManualOverridePayload,
): Promise<void> {
  await axios.post(`${BASE_URL}/quarantine_parcels/${encodeURIComponent(dzialkaId)}/manual_override`, payload);
}

export function useQuarantineParcels() {
  return useQuery<QuarantineParcelFeatureCollection, Error>({
    queryKey: ['quarantine-parcels'],
    queryFn: fetchQuarantineParcels,
    staleTime: 30_000,
    gcTime: 5 * 60_000,
    retry: 1,
    refetchOnWindowFocus: false,
  });
}

export function useManualOverrideMutation() {
  const queryClient = useQueryClient();

  return useMutation<void, Error, { dzialkaId: string; payload: ManualOverridePayload }>({
    mutationFn: ({ dzialkaId, payload }) => submitManualOverride(dzialkaId, payload),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ['quarantine-parcels'] }),
        queryClient.invalidateQueries({ queryKey: ['leads'] }),
      ]);
    },
  });
}
