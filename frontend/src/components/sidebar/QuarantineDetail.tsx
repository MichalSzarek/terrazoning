import { useEffect, useState } from 'react';
import {
  AlertTriangle,
  ArrowLeft,
  Clock,
  ExternalLink,
  FileWarning,
  Flame,
  Layers,
  MapPin,
  Tag,
} from 'lucide-react';

import { useManualOverrideMutation } from '../../hooks/useQuarantineParcels';
import type { QuarantineParcelFeature } from '../../types/api';

interface QuarantineDetailProps {
  feature: QuarantineParcelFeature;
  onBack: () => void;
}

interface StatRowProps {
  icon: React.ReactNode;
  label: string;
  value: React.ReactNode;
}

function StatRow({ icon, label, value }: StatRowProps) {
  return (
    <div className="flex items-center justify-between py-2 border-b border-gray-800 last:border-0">
      <span className="flex items-center gap-1.5 text-xs text-gray-500">
        {icon}
        {label}
      </span>
      <span className="text-xs font-mono text-gray-200 text-right max-w-[60%] truncate">
        {value}
      </span>
    </div>
  );
}

export function QuarantineDetail({ feature, onBack }: QuarantineDetailProps) {
  const p = feature.properties;
  const [manualPrzeznaczenie, setManualPrzeznaczenie] = useState(p.manual_przeznaczenie ?? '');
  const mutation = useManualOverrideMutation();

  useEffect(() => {
    setManualPrzeznaczenie(p.manual_przeznaczenie ?? '');
  }, [p.manual_przeznaczenie, p.dzialka_id]);

  useEffect(() => {
    if (mutation.isSuccess) {
      onBack();
    }
  }, [mutation.isSuccess, onBack]);

  const formattedArea = p.area_m2 != null
    ? p.area_m2 >= 10_000
      ? `${(p.area_m2 / 10_000).toFixed(3)} ha (${Math.round(p.area_m2).toLocaleString('pl-PL')} m²)`
      : `${Math.round(p.area_m2).toLocaleString('pl-PL')} m²`
    : '—';

  const createdAt = p.created_at
    ? new Date(p.created_at).toLocaleDateString('pl-PL', {
      day: '2-digit',
      month: '2-digit',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    })
    : '—';

  const isValidInput = manualPrzeznaczenie.trim().length > 0;

  return (
    <div className="flex h-full flex-col">
      <div className="flex items-center gap-2 border-b border-gray-800 px-4 py-3 flex-shrink-0">
        <button
          type="button"
          onClick={onBack}
          className="flex items-center gap-1 rounded p-1 text-gray-400 hover:bg-gray-800 hover:text-gray-200 transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-yellow-400"
          aria-label="Wróć do listy"
        >
          <ArrowLeft size={14} aria-hidden />
          <span className="text-xs">Lista</span>
        </button>
        <div className="ml-auto">
          <span className="inline-flex items-center rounded border border-yellow-500/30 bg-yellow-500/10 px-2 py-0.5 text-[10px] font-medium uppercase tracking-wider text-yellow-300">
            quarantine
          </span>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto">
        <div className="border-b border-gray-800 px-4 py-4">
          <p className="font-mono text-sm text-gray-100 break-all leading-snug">
            {p.identyfikator}
          </p>
          <p className="mt-1 text-xs text-gray-500">{p.teryt_gmina ?? 'brak TERYT gminy'}</p>
          {p.source_url && (
            <a
              href={p.source_url}
              target="_blank"
              rel="noopener noreferrer"
              className="mt-3 inline-flex items-center gap-1.5 rounded border border-yellow-500/30 bg-yellow-500/10 px-2.5 py-1.5 text-[11px] font-medium text-yellow-200 transition-colors hover:border-yellow-400/50 hover:bg-yellow-500/15"
            >
              <ExternalLink size={11} aria-hidden />
              Otwórz źródło
            </a>
          )}
          <div className="mt-3 rounded-lg border border-yellow-500/15 bg-yellow-500/5 px-3 py-2">
            <div className="flex items-center gap-2 text-yellow-200">
              <AlertTriangle size={12} aria-hidden />
              <span className="text-[11px] font-medium">Wymaga ręcznego przeznaczenia</span>
            </div>
            <p className="mt-1 text-[11px] text-gray-400">
              {p.reason ?? 'Brak rozstrzygnięcia po automatycznym pipeline.'}
            </p>
          </div>
        </div>

        <div className="px-4 py-2 border-b border-gray-800">
          <p className="text-[10px] font-medium uppercase tracking-wider text-gray-600 mb-1 pt-1">
            Parametry działki
          </p>
          <StatRow
            icon={<MapPin size={10} aria-hidden />}
            label="Działka ID"
            value={p.dzialka_id}
          />
          <StatRow
            icon={<Layers size={10} aria-hidden />}
            label="Powierzchnia"
            value={formattedArea}
          />
          {p.current_use && (
            <StatRow
              icon={<FileWarning size={10} aria-hidden />}
              label="Current use"
              value={p.current_use}
            />
          )}
          {p.dominant_przeznaczenie && (
            <StatRow
              icon={<Tag size={10} aria-hidden />}
              label="Auto przeznaczenie"
              value={p.dominant_przeznaczenie}
            />
          )}
          {p.manual_przeznaczenie && (
            <StatRow
              icon={<Tag size={10} aria-hidden />}
              label="Manual override"
              value={<span className="text-yellow-300">{p.manual_przeznaczenie}</span>}
            />
          )}
          <StatRow
            icon={<Clock size={10} aria-hidden />}
            label="Wykryto"
            value={createdAt}
          />
        </div>

        <div className="px-4 py-4">
          <p className="text-[10px] font-medium uppercase tracking-wider text-gray-600 mb-3">
            Manual Override
          </p>

          <label className="block text-[11px] text-gray-500 mb-1" htmlFor="manual-przeznaczenie">
            Przeznaczenie (z PDF / operatu)
          </label>
          <input
            id="manual-przeznaczenie"
            type="text"
            value={manualPrzeznaczenie}
            onChange={(event) => setManualPrzeznaczenie(event.target.value)}
            placeholder="np. MN, U, MN/U"
            className="w-full rounded border border-gray-700 bg-gray-950 px-3 py-2 text-sm text-gray-100 placeholder:text-gray-600 focus:border-yellow-400 focus:outline-none focus:ring-1 focus:ring-yellow-400"
          />

          {mutation.isError && (
            <p className="mt-2 text-[11px] text-red-400">
              {mutation.error.message}
            </p>
          )}

          <button
            type="button"
            onClick={() => mutation.mutate({
              dzialkaId: p.dzialka_id,
              payload: { manual_przeznaczenie: manualPrzeznaczenie.trim() },
            })}
            disabled={!isValidInput || mutation.isPending}
            className="mt-3 inline-flex items-center gap-2 rounded border border-yellow-500/40 bg-yellow-500 px-3 py-2 text-[12px] font-medium text-gray-950 transition-colors hover:bg-yellow-400 disabled:cursor-not-allowed disabled:border-gray-700 disabled:bg-gray-800 disabled:text-gray-500"
          >
            <Flame size={12} aria-hidden />
            {mutation.isPending ? 'Tworzenie…' : 'Utwórz Lead'}
          </button>
        </div>
      </div>
    </div>
  );
}
