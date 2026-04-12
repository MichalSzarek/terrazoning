function parseBooleanFlag(value: string | undefined, defaultValue: boolean): boolean {
  if (value == null || value.trim() === '') return defaultValue;
  const normalized = value.trim().toLowerCase();
  if (['1', 'true', 'yes', 'on', 'enabled'].includes(normalized)) return true;
  if (['0', 'false', 'no', 'off', 'disabled'].includes(normalized)) return false;
  return defaultValue;
}

export const futureBuildabilityEnabled = parseBooleanFlag(
  import.meta.env.VITE_FUTURE_BUILDABILITY_ENABLED,
  true,
);
