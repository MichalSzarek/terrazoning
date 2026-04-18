import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { LeadDetail } from './LeadDetail';
import type { LeadFeature } from '../../types/api';

const { mutateMock } = vi.hoisted(() => ({
  mutateMock: vi.fn(),
}));

let writeTextMock = vi.fn();

vi.mock('../../hooks/useLeads', () => ({
  useLeadStatusMutation: () => ({
    mutate: mutateMock,
    isPending: false,
    isError: false,
    isSuccess: false,
    error: new Error('mutation failed'),
  }),
  useMarketBenchmark: () => ({
    data: null,
    isFetching: false,
    isError: false,
  }),
}));

vi.mock('../../lib/featureFlags', () => ({
  futureBuildabilityEnabled: false,
}));

vi.mock('../ui/ConfidenceBadge', () => ({
  ConfidenceBadge: () => <div data-testid="confidence-badge" />,
}));

vi.mock('../ui/EvidenceChain', () => ({
  EvidenceChain: () => <div data-testid="evidence-chain" />,
}));

function makeFeature(overrides?: Partial<LeadFeature['properties']>): LeadFeature {
  return {
    type: 'Feature',
    geometry: {
      type: 'Point',
      coordinates: [19.1, 50.8],
    },
    properties: {
      lead_id: '550e8400-e29b-41d4-a716-446655440000',
      confidence_score: 0.92,
      priority: 'high',
      strategy_type: 'current_buildable',
      confidence_band: null,
      status: 'new',
      reviewed_at: null,
      notes: null,
      display_point: {
        type: 'Point',
        coordinates: [19.1, 50.8],
      },
      area_m2: 18825,
      max_coverage_pct: 72.5,
      max_buildable_area_m2: 2505.2,
      dominant_przeznaczenie: 'MN',
      price_zl: 129000,
      price_per_m2_zl: 178.45,
      price_signal: 'reliable',
      quality_signal: 'complete',
      investment_score: 78.4,
      future_signal_score: null,
      cheapness_score: null,
      overall_score: null,
      signal_quality_tier: null,
      next_best_action: null,
      dominant_future_signal: null,
      future_signal_count: null,
      distance_to_nearest_buildable_m: null,
      adjacent_buildable_pct: null,
      missing_metrics: [],
      identyfikator: '240609206.60',
      teryt_gmina: '2406092',
      listing_id: '660e8400-e29b-41d4-a716-446655440000',
      source_url:
        'https://licytacje.komornik.pl/wyszukiwarka/obwieszczenia-o-licytacji/34251/licytacja-nieruchomosci',
      kw_number: 'KR1B/00079684/3',
      ekw_search_url:
        'https://przegladarka-ekw.ms.gov.pl/eukw_prz/KsiegiWieczyste/wyszukiwanieKW?komunikaty=true&kontakt=true&okienkoSerwisowe=false&kodEci=KR1B&kodWydzialuInput=KR1B&numerKW=00079684&cyfraKontrolna=3',
      evidence_chain: [],
      signal_breakdown: [],
      created_at: '2026-04-18T12:00:00Z',
      ...overrides,
    },
  };
}

describe('LeadDetail', () => {
  beforeEach(() => {
    mutateMock.mockReset();
    writeTextMock = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, 'clipboard', {
      configurable: true,
      value: {
        writeText: writeTextMock,
      },
    });
  });

  it('renders the KW section and copies the KW number', async () => {
    const feature = makeFeature();

    render(<LeadDetail feature={feature} onBack={() => undefined} />);

    expect(screen.getByText('KR1B/00079684/3')).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Otwórz EKW (beta)' })).toHaveAttribute(
      'href',
      feature.properties.ekw_search_url,
    );
    expect(screen.getByText(/prefill działa w trybie best-effort/i)).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Kopiuj KW' }));

    await waitFor(() => {
      expect(writeTextMock).toHaveBeenCalledWith('KR1B/00079684/3');
    });
    expect(screen.getByText('Skopiowano numer KW.')).toBeInTheDocument();
  });

  it('hides the KW section when the lead has no KW number', () => {
    const feature = makeFeature({
      kw_number: null,
      ekw_search_url: null,
    });

    render(<LeadDetail feature={feature} onBack={() => undefined} />);

    expect(screen.queryByRole('button', { name: 'Kopiuj KW' })).not.toBeInTheDocument();
    expect(screen.queryByRole('link', { name: 'Otwórz EKW (beta)' })).not.toBeInTheDocument();
  });
});
