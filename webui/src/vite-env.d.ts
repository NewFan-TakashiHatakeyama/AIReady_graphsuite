/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_API_PROXY: string
  readonly VITE_API_ENDPOINTS: string
  readonly VITE_BACKEND_URL: string
  /** run.ps1: force relative API URLs (Vite proxy → local graphsuite_server) */
  readonly VITE_USE_VITE_PROXY?: string
  /** ガバナンス是正のクライアント診断（console + sessionStorage） */
  readonly VITE_DEBUG_GOVERNANCE_REMEDIATION?: string
}

interface ImportMeta {
  readonly env: ImportMetaEnv
}
