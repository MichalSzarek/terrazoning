# Vertex (Claude) Setup For This Repo

## Why You Were Seeing `429 RESOURCE_EXHAUSTED`

In GCP project `maths-489717`, Vertex AI quotas for the newer Claude base models
(for example `anthropic-claude-sonnet-4-5` / `anthropic-claude-haiku-4-5`) are
currently **0** in the quotas API, so any attempt to call those models returns:

`429 RESOURCE_EXHAUSTED (check quota)`

Quota overrides cannot fix this when the max is 0. You need quota allocation /
quota increase in Cloud Console for those base models.

## What We Do Instead (Works Immediately)

We configure Claude Code to:

- Use a **regional** Vertex endpoint (`CLOUD_ML_REGION=europe-west1`)
- Pin to Claude models that **already have quota allocated** in this project:
  - `claude-3-7-sonnet`
  - `claude-3-5-haiku`

These are set in `.claude/settings.local.json`.

## Switching Back To Sonnet/Haiku 4.5+

After you request and receive quota for the relevant base models in Cloud Console,
change pins to:

- `ANTHROPIC_DEFAULT_SONNET_MODEL=claude-sonnet-4-5@20250929`
- `ANTHROPIC_DEFAULT_HAIKU_MODEL=claude-haiku-4-5@20251001`

And optionally keep `CLOUD_ML_REGION` regional (recommended), or switch back to
`global` if your org has quotas enabled there.

