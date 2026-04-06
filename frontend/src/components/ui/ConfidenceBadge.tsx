/**
 * Confidence score visualization.
 *
 * Per Frontend Lead Commandment 7: color is NEVER the sole indicator.
 * Always paired with a numeric value + text label + icon.
 *
 * Score thresholds (investment context — higher = more actionable):
 *   ≥ 0.90  → red-500   "Prime"    (act now)
 *   ≥ 0.80  → orange-500 "Wysoki"
 *   ≥ 0.70  → amber-400  "Umiarkowany"
 *   < 0.70  → gray-500   "Niski"
 */

import { ShieldCheck, ShieldAlert, ShieldX, Zap } from 'lucide-react';

interface ConfidenceBadgeProps {
  score: number;
  /** 'badge' — compact chip | 'bar' — full progress bar */
  variant?: 'badge' | 'bar';
}

interface Tier {
  label: string;
  colorClass: string;
  bgClass: string;
  borderClass: string;
  Icon: typeof ShieldCheck;
}

function getTier(score: number): Tier {
  if (score >= 0.9) {
    return {
      label: 'Prime',
      colorClass: 'text-red-400',
      bgClass: 'bg-red-500/20',
      borderClass: 'border-red-500/40',
      Icon: Zap,
    };
  }
  if (score >= 0.8) {
    return {
      label: 'Wysoki',
      colorClass: 'text-orange-400',
      bgClass: 'bg-orange-500/20',
      borderClass: 'border-orange-500/40',
      Icon: ShieldCheck,
    };
  }
  if (score >= 0.7) {
    return {
      label: 'Umiarkowany',
      colorClass: 'text-amber-400',
      bgClass: 'bg-amber-400/20',
      borderClass: 'border-amber-400/40',
      Icon: ShieldAlert,
    };
  }
  return {
    label: 'Niski',
    colorClass: 'text-gray-400',
    bgClass: 'bg-gray-600/20',
    borderClass: 'border-gray-600/40',
    Icon: ShieldX,
  };
}

/** Mapbox-compatible color string for a given confidence_score (used in map layers too). */
export function confidenceHexColor(score: number): string {
  if (score >= 0.9) return '#ef4444';  // red-500
  if (score >= 0.8) return '#f97316';  // orange-500
  if (score >= 0.7) return '#fbbf24';  // amber-400
  return '#6b7280';                     // gray-500
}

export function ConfidenceBadge({ score, variant = 'badge' }: ConfidenceBadgeProps) {
  const tier = getTier(score);
  const { Icon } = tier;
  const pct = Math.round(score * 100);

  if (variant === 'bar') {
    return (
      <div className="space-y-1">
        <div className="flex items-center justify-between text-xs">
          <span className={`flex items-center gap-1 font-medium ${tier.colorClass}`}>
            <Icon size={12} aria-hidden />
            {tier.label}
          </span>
          <span className="font-mono text-gray-300">{pct}%</span>
        </div>
        <div
          className="h-1.5 w-full rounded-full bg-gray-700"
          role="progressbar"
          aria-valuenow={pct}
          aria-valuemin={0}
          aria-valuemax={100}
          aria-label={`Confidence: ${pct}%`}
        >
          <div
            className="h-full rounded-full transition-all duration-300"
            style={{ width: `${pct}%`, backgroundColor: confidenceHexColor(score) }}
          />
        </div>
      </div>
    );
  }

  return (
    <span
      className={`inline-flex items-center gap-1 rounded border px-1.5 py-0.5 text-xs font-medium ${tier.colorClass} ${tier.bgClass} ${tier.borderClass}`}
      aria-label={`Confidence: ${tier.label} (${pct}%)`}
    >
      <Icon size={10} aria-hidden />
      <span className="font-mono">{pct}%</span>
      <span className="hidden sm:inline">{tier.label}</span>
    </span>
  );
}
