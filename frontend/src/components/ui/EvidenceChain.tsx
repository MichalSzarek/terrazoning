/**
 * Evidence chain renderer — shows the analyst every hop from source to delta result.
 *
 * Format from backend evidence_chain JSON:
 *   { step: 'source',   ref: uuid, url: string }
 *   { step: 'parcel',   ref: uuid, teryt: string }
 *   { step: 'delta',    ref: uuid, coverage: float, przeznaczenie: string, plan: string }
 *   { step: 'document', ref: uuid, uri: 'gs://...' }
 */

import { ExternalLink, MapPin, BarChart3, FileText, Link } from 'lucide-react';
import type { EvidenceStep } from '../../types/api';

interface EvidenceChainProps {
  chain: EvidenceStep[];
}

function StepIcon({ step }: { step: EvidenceStep['step'] }) {
  switch (step) {
    case 'source':   return <ExternalLink size={12} className="text-blue-400" aria-hidden />;
    case 'parcel':   return <MapPin size={12} className="text-green-400" aria-hidden />;
    case 'delta':    return <BarChart3 size={12} className="text-amber-400" aria-hidden />;
    case 'document': return <FileText size={12} className="text-purple-400" aria-hidden />;
  }
}

function StepLabel({ step }: { step: EvidenceStep['step'] }) {
  const labels: Record<EvidenceStep['step'], string> = {
    source:   'Źródło',
    parcel:   'Działka',
    delta:    'Analiza delta',
    document: 'Dokument',
  };
  return <span className="text-xs font-medium text-gray-400 uppercase tracking-wider">{labels[step]}</span>;
}

function StepBody({ step }: { step: EvidenceStep }) {
  switch (step.step) {
    case 'source':
      return (
        <div className="text-xs text-gray-300 space-y-0.5">
          <p className="font-mono text-gray-500 truncate">{step.ref}</p>
          {step.url && (
            <a
              href={step.url}
              target="_blank"
              rel="noopener noreferrer"
              className="flex items-center gap-1 text-blue-400 hover:text-blue-300 truncate"
            >
              <Link size={10} aria-hidden />
              {step.url.replace(/^https?:\/\//, '')}
            </a>
          )}
        </div>
      );

    case 'parcel':
      return (
        <div className="text-xs text-gray-300 space-y-0.5">
          {step.teryt && (
            <p>
              <span className="text-gray-500">TERYT: </span>
              <span className="font-mono">{step.teryt}</span>
            </p>
          )}
          <p className="font-mono text-gray-500 truncate">{step.ref}</p>
        </div>
      );

    case 'delta':
      return (
        <div className="text-xs text-gray-300 space-y-1">
          <div className="flex items-center justify-between">
            <span>
              <span className="text-gray-500">Pokrycie: </span>
              <span className="font-mono font-bold text-amber-400">{step.coverage.toFixed(1)}%</span>
            </span>
            <span className="rounded bg-amber-500/20 px-1.5 py-0.5 font-mono text-amber-300 border border-amber-500/30">
              {step.przeznaczenie}
            </span>
          </div>
          <p className="text-gray-400 truncate">{step.plan}</p>
          <p className="text-gray-600 uppercase text-[10px] tracking-wider">{step.plan_type}</p>
        </div>
      );

    case 'document':
      return (
        <div className="text-xs text-gray-300 space-y-0.5">
          {step.uri && (
            <p className="font-mono text-purple-400 truncate text-[11px]">{step.uri}</p>
          )}
          <p className="font-mono text-gray-500 truncate">{step.ref}</p>
        </div>
      );
  }
}

export function EvidenceChain({ chain }: EvidenceChainProps) {
  if (chain.length === 0) {
    return (
      <p className="text-xs text-gray-600 italic">Brak danych w łańcuchu dowodowym.</p>
    );
  }

  return (
    <ol className="relative space-y-0" aria-label="Łańcuch dowodowy">
      {chain.map((step, i) => (
        <li key={`${step.step}-${i}`} className="flex gap-3">
          {/* Timeline connector */}
          <div className="flex flex-col items-center">
            <div className="flex h-6 w-6 flex-shrink-0 items-center justify-center rounded-full bg-gray-800 border border-gray-700">
              <StepIcon step={step.step} />
            </div>
            {i < chain.length - 1 && (
              <div className="w-px flex-1 bg-gray-700 my-1" aria-hidden />
            )}
          </div>

          {/* Step content */}
          <div className="pb-4 flex-1 min-w-0">
            <StepLabel step={step.step} />
            <div className="mt-1">
              <StepBody step={step} />
            </div>
          </div>
        </li>
      ))}
    </ol>
  );
}
