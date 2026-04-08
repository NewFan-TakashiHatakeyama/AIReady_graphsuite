/** Opt-in diagnostics: set VITE_DEBUG_GOVERNANCE_REMEDIATION=true in webui .env.local */
const STORAGE_KEY = 'graphsuite_debug_remediation_ndjson'
const MAX_LINES = 30

export function appendRemediationClientDebug(event: string, data: Record<string, unknown>): void {
  if (import.meta.env.VITE_DEBUG_GOVERNANCE_REMEDIATION !== 'true') return
  const payload = {
    location: `remediationDebug.ts:${event}`,
    message: event,
    data: { source: 'webui.governance', ...data },
    timestamp: Date.now(),
    hypothesisId: 'webui-remediation-flow',
  }
  const line = JSON.stringify(payload)
   
  console.info('[GRAPHSUITE_DEBUG_REMEDIATION]', line)
  try {
    const prev = sessionStorage.getItem(STORAGE_KEY) || ''
    const lines = (prev ? `${prev}\n${line}` : line).split('\n').slice(-MAX_LINES)
    sessionStorage.setItem(STORAGE_KEY, lines.join('\n'))
  } catch {
    /* ignore quota / private mode */
  }
}
