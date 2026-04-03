import { useState, useCallback } from 'react'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/Popover'
import Button from '@/components/ui/Button'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/Select'
import { useSettingsStore } from '@/stores/settings'
import { useAuthStore } from '@/stores/state'
import { SettingsIcon, LogOutIcon, BrainIcon } from 'lucide-react'
import { useTranslation } from 'react-i18next'
import { cn } from '@/lib/utils'
import { navigationService } from '@/services/navigation'
import Separator from '@/components/ui/Separator'

interface AppSettingsProps {
  className?: string
}

export default function AppSettings({ className }: AppSettingsProps) {
  const [opened, setOpened] = useState<boolean>(false)
  const { t } = useTranslation()

  const language = useSettingsStore.use.language()
  const setLanguage = useSettingsStore.use.setLanguage()

  const theme = useSettingsStore.use.theme()
  const setTheme = useSettingsStore.use.setTheme()

  const { isGuestMode, username } = useAuthStore()

  const handleLanguageChange = useCallback((value: string) => {
    setLanguage(value as 'en' | 'zh' | 'ja')
  }, [setLanguage])

  const handleThemeChange = useCallback((value: string) => {
    setTheme(value as 'light' | 'dark')
  }, [setTheme])

  const handleLogout = useCallback(() => {
    setOpened(false)
    navigationService.navigateToLogin()
  }, [])

  const handleLLMSettings = useCallback(() => {
    setOpened(false)
    // TODO: Implement LLM settings dialog
    console.log('LLM settings clicked')
  }, [])

  return (
    <Popover open={opened} onOpenChange={setOpened}>
      <PopoverTrigger asChild>
        <Button
          variant="ghost"
          size="icon"
          className={cn('h-9 w-9 text-foreground/90 hover:bg-white/20 hover:text-foreground', className)}
          aria-label={t('settings.openMenu', '設定メニューを開く')}
        >
          <SettingsIcon className="h-5 w-5" />
        </Button>
      </PopoverTrigger>
      <PopoverContent side="bottom" align="end" className="w-64">
        <div className="flex flex-col gap-4">
          <div className="flex flex-col gap-3">
            <div className="flex flex-col gap-2">
              <label className="text-sm font-medium">{t('settings.language')}</label>
              <Select value={language} onValueChange={handleLanguageChange}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="ja">日本語</SelectItem>
                  <SelectItem value="en">English</SelectItem>
                  <SelectItem value="zh">中文</SelectItem>
                </SelectContent>
              </Select>
            </div>

            <div className="flex flex-col gap-2">
              <label className="text-sm font-medium">{t('settings.theme')}</label>
              <Select value={theme} onValueChange={handleThemeChange}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="light">{t('settings.light')}</SelectItem>
                  <SelectItem value="dark">{t('settings.dark')}</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>

          <Separator />

          <div className="flex flex-col gap-1">
            <Button
              variant="ghost"
              className="justify-start h-9 px-2 text-sm hover:bg-accent"
              onClick={handleLLMSettings}
            >
              <BrainIcon className="h-4 w-4 mr-2" />
              {t('settings.llmSettings', 'LLM設定')}
            </Button>

            {!isGuestMode && (
              <Button
                variant="ghost"
                className="justify-start h-9 px-2 text-sm hover:bg-accent text-red-600 hover:text-red-700"
                onClick={handleLogout}
              >
                <LogOutIcon className="h-4 w-4 mr-2" />
                {t('header.logout')} ({username})
              </Button>
            )}
          </div>
        </div>
      </PopoverContent>
    </Popover>
  )
}
