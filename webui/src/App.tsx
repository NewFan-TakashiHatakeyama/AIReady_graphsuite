import { useState, useCallback, useEffect, useRef } from 'react'
import ThemeProvider from '@/components/ThemeProvider'
import ApiKeyAlert from '@/components/ApiKeyAlert'
import StatusIndicator from '@/components/status/StatusIndicator'
import { webuiPrefix } from '@/lib/constants'
import { useBackendState, useAuthStore } from '@/stores/state'
import { useSettingsStore } from '@/stores/settings'
import { getAuthStatus } from '@/api/graphsuite'
import SiteHeader from '@/features/SiteHeader'
import { InvalidApiKeyError, RequireApiKeError } from '@/api/graphsuite'

import AIReadyDashboard from '@/features/AIReadyDashboard'
import ConnectOperations from '@/features/connect/ConnectOperations'
import GovernanceOperations from '@/features/governance/GovernanceOperations'
import OntologyOperations from '@/features/ontology/OntologyOperations'
import AuditCenter from '@/features/audit/AuditCenter'

function App() {
  const message = useBackendState.use.message()
  const enableHealthCheck = useSettingsStore.use.enableHealthCheck()
  const currentTab = useSettingsStore.use.currentTab()
  const [apiKeyAlertOpen, setApiKeyAlertOpen] = useState(false)
  const [initializing, setInitializing] = useState(true)
  const versionCheckRef = useRef(false)
  const healthCheckInitializedRef = useRef(false)

  const handleApiKeyAlertOpenChange = useCallback((open: boolean) => {
    setApiKeyAlertOpen(open)
    if (!open) {
      useBackendState.getState().clear()
    }
  }, [])

  const isMountedRef = useRef(true)

  useEffect(() => {
    isMountedRef.current = true
    const handleBeforeUnload = () => {
      isMountedRef.current = false
    }
    window.addEventListener('beforeunload', handleBeforeUnload)
    return () => {
      isMountedRef.current = false
      window.removeEventListener('beforeunload', handleBeforeUnload)
    }
  }, [])

  useEffect(() => {
    const performHealthCheck = async () => {
      try {
        if (isMountedRef.current) {
          await useBackendState.getState().check()
        }
      } catch (error) {
        console.error('Health check error:', error)
      }
    }

    useBackendState.getState().setHealthCheckFunction(performHealthCheck)

    if (!enableHealthCheck || apiKeyAlertOpen) {
      useBackendState.getState().clearHealthCheckTimer()
      return
    }

    if (!healthCheckInitializedRef.current) {
      healthCheckInitializedRef.current = true
    }

    useBackendState.getState().resetHealthCheckTimer()

    return () => {
      useBackendState.getState().clearHealthCheckTimer()
    }
  }, [enableHealthCheck, apiKeyAlertOpen])

  useEffect(() => {
    const checkVersion = async () => {
      if (versionCheckRef.current) return
      versionCheckRef.current = true

      const versionCheckedFromLogin = sessionStorage.getItem('VERSION_CHECKED_FROM_LOGIN') === 'true'
      if (versionCheckedFromLogin) {
        setInitializing(false)
        return
      }

      try {
        setInitializing(true)
        const token = localStorage.getItem('LIGHTRAG-API-TOKEN')
        const status = await getAuthStatus()

        if (!status.auth_configured && status.access_token) {
          useAuthStore.getState().login(
            status.access_token,
            true,
            status.core_version,
            status.api_version,
            status.webui_title || null,
            status.webui_description || null
          )
        } else if (token && (status.core_version || status.api_version || status.webui_title || status.webui_description)) {
          const isGuestMode = status.auth_mode === 'disabled' || useAuthStore.getState().isGuestMode
          useAuthStore.getState().login(
            token,
            isGuestMode,
            status.core_version,
            status.api_version,
            status.webui_title || null,
            status.webui_description || null
          )
        }
        sessionStorage.setItem('VERSION_CHECKED_FROM_LOGIN', 'true')
      } catch (error) {
        console.error('Failed to get version info:', error)
      } finally {
        setInitializing(false)
      }
    }
    checkVersion()
  }, [])

  useEffect(() => {
    if (message) {
      if (message.includes(InvalidApiKeyError) || message.includes(RequireApiKeError)) {
        setApiKeyAlertOpen(true)
      }
    }
  }, [message])

  const renderContent = () => {
    switch (currentTab) {
    case 'aiready-dashboard':
      return <AIReadyDashboard />
    case 'connect-operations':
      return <ConnectOperations />
    case 'governance-operations':
      return <GovernanceOperations />
    case 'ontology-operations':
      return <OntologyOperations />
    case 'audit-center':
      return <AuditCenter />
    default:
      return <AIReadyDashboard />
    }
  }

  return (
    <ThemeProvider>
      {initializing ? (
        <div className="liquid-glass-page flex h-screen w-screen flex-col">
          <header className="liquid-glass-surface sticky top-0 z-50 flex h-10 w-full border-b px-4">
            <div className="min-w-[200px] w-auto flex items-center">
              <a href={webuiPrefix} className="flex items-center gap-2">
                <img
                  src="/logo_lp.png"
                  alt="Logo"
                  className="h-5 w-28 dark:hidden"
                  style={{ height: '30px', width: '100px' }}
                  aria-hidden="true"
                />
                <img
                  src="/logo.png"
                  alt="Logo"
                  className="hidden h-5 w-28 dark:block"
                  style={{ height: '30px', width: '100px' }}
                  aria-hidden="true"
                />
              </a>
            </div>
            <div className="flex h-10 flex-1 items-center justify-center"></div>
            <nav className="w-[200px] flex items-center justify-end"></nav>
          </header>
          <div className="flex flex-1 items-center justify-center">
            <div className="text-center">
              <div className="mb-2 h-8 w-8 animate-spin rounded-full border-4 border-primary border-t-transparent"></div>
              <p>Initializing...</p>
            </div>
          </div>
        </div>
      ) : (
        <main className="liquid-glass-page flex h-screen w-screen flex-col overflow-hidden">
          <SiteHeader />
          <div className="relative flex-1 min-h-0 overflow-hidden">
            {renderContent()}
          </div>
          {enableHealthCheck && <StatusIndicator />}
          <ApiKeyAlert open={apiKeyAlertOpen} onOpenChange={handleApiKeyAlertOpenChange} />
        </main>
      )}
    </ThemeProvider>
  )
}

export default App
