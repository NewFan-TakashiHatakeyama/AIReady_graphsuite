import { webuiPrefix } from '@/lib/constants'
import AppSettings from '@/components/AppSettings'
import { useSettingsStore, type Tab } from '@/stores/settings'
import { useAuthStore } from '@/stores/state'
import { cn } from '@/lib/utils'
import { useTranslation } from 'react-i18next'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/Tooltip'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger
} from '@/components/ui/DropdownMenu'
import { ChevronDown } from 'lucide-react'

function headerTabClassNames(active: boolean) {
  return cn(
    'liquid-glass-interactive liquid-header-tab inline-flex cursor-pointer items-center gap-1 px-4 py-1.5 text-sm font-medium rounded-xl text-foreground/75 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/60 focus-visible:ring-offset-1 focus-visible:ring-offset-transparent',
    active
      ? 'liquid-header-tab-active font-semibold text-foreground dark:text-foreground shadow-sm'
      : 'font-medium'
  )
}

interface NavigationTabProps {
  value: Tab
  currentTab: Tab
  onClick: (value: Tab) => void
  children: React.ReactNode
}

function NavigationTab({ value, currentTab, onClick, children }: NavigationTabProps) {
  return (
    <button
      type="button"
      onClick={() => onClick(value)}
      aria-current={currentTab === value ? 'page' : undefined}
      className={headerTabClassNames(currentTab === value)}
    >
      {children}
    </button>
  )
}

function SiteNavigation() {
  const currentTab = useSettingsStore.use.currentTab()
  const setCurrentTab = useSettingsStore.use.setCurrentTab()
  const { t } = useTranslation()

  const handleTabClick = (tab: Tab) => {
    if (typeof window !== 'undefined' && tab === 'aiready-dashboard') {
      const homeUrl = `${window.location.pathname}#/app`
      window.history.replaceState(window.history.state, '', homeUrl)
    }
    setCurrentTab(tab)
  }

  const diagnosisActive =
    currentTab === 'connect-operations' || currentTab === 'governance-operations'
  const operationsActive = currentTab === 'aiready-dashboard' || currentTab === 'audit-center'

  return (
    <div className="flex h-8 self-center items-center gap-2">
      <DropdownMenu>
        <DropdownMenuTrigger
          type="button"
          aria-current={diagnosisActive ? 'page' : undefined}
          className={headerTabClassNames(diagnosisActive)}
        >
          {t('header.diagnosis', '診断')}
          <ChevronDown className="h-4 w-4 shrink-0 opacity-70" aria-hidden />
        </DropdownMenuTrigger>
        <DropdownMenuContent align="center">
          <DropdownMenuItem onSelect={() => handleTabClick('connect-operations')}>
            {t('header.sub.connect', '接続')}
          </DropdownMenuItem>
          <DropdownMenuItem onSelect={() => handleTabClick('governance-operations')}>
            {t('header.sub.governance', 'ガバナンス')}
          </DropdownMenuItem>
        </DropdownMenuContent>
      </DropdownMenu>

      <NavigationTab
        value="ontology-operations"
        currentTab={currentTab}
        onClick={handleTabClick}
      >
        {t('header.cleaning', '清掃')}
      </NavigationTab>

      <DropdownMenu>
        <DropdownMenuTrigger
          type="button"
          aria-current={operationsActive ? 'page' : undefined}
          className={headerTabClassNames(operationsActive)}
        >
          {t('header.operations', '運用')}
          <ChevronDown className="h-4 w-4 shrink-0 opacity-70" aria-hidden />
        </DropdownMenuTrigger>
        <DropdownMenuContent align="center">
          <DropdownMenuItem onSelect={() => handleTabClick('aiready-dashboard')}>
            {t('header.sub.dashboard', 'ダッシュボード')}
          </DropdownMenuItem>
          <DropdownMenuItem onSelect={() => handleTabClick('audit-center')}>
            {t('header.sub.auditLog', '監査ログ')}
          </DropdownMenuItem>
        </DropdownMenuContent>
      </DropdownMenu>
    </div>
  )
}

export default function SiteHeader() {
  const { t } = useTranslation()
  const { isGuestMode, coreVersion, apiVersion, webuiTitle, webuiDescription } = useAuthStore()

  const versionDisplay = (coreVersion && apiVersion)
    ? `${coreVersion}/${apiVersion}`
    : null;

  return (
    <header className="liquid-glass-surface liquid-header-shell sticky top-0 z-50 flex h-10 w-full px-4">
      <div className="min-w-[200px] w-auto flex items-center">
        <a
          href={webuiPrefix}
          className="flex items-center gap-2"
          aria-label={t('header.home', 'GraphSuite ホーム')}
        >
          <img
            src="/logo_lp.png"
            alt="Logo"
            className="h-10 w-28 dark:hidden"
            style={{ height: '30px', width: '100px' }}
            aria-hidden="true"
          />
          <img
            src="/logo.png"
            alt="Logo"
            className="hidden h-10 w-28 dark:block"
            style={{ height: '30px', width: '100px' }}
            aria-hidden="true"
          />
        </a>
        {webuiTitle && (
          <div className="flex items-center">
            <span className="mx-1 text-xs text-foreground/60">|</span>
            <TooltipProvider>
              <Tooltip>
                <TooltipTrigger asChild>
                  <span className="font-medium text-sm cursor-default text-foreground">
                    {webuiTitle}
                  </span>
                </TooltipTrigger>
                {webuiDescription && (
                  <TooltipContent side="bottom">
                    {webuiDescription}
                  </TooltipContent>
                )}
              </Tooltip>
            </TooltipProvider>
          </div>
        )}
      </div>

      <div className="flex h-10 flex-1 items-center justify-center">
        <SiteNavigation />
        {isGuestMode && (
          <div className="liquid-glass-surface ml-2 self-center px-2 py-1 text-xs rounded-md shadow-sm text-foreground">
            {t('login.guestMode', 'Guest Mode')}
          </div>
        )}
      </div>

      <nav className="w-[200px] flex items-center justify-end">
        <div className="flex items-center gap-2">
          {versionDisplay && (
            <span className="text-xs text-foreground/60 mr-1">
              v{versionDisplay}
            </span>
          )}
          <AppSettings />
        </div>
      </nav>
    </header>
  )
}
