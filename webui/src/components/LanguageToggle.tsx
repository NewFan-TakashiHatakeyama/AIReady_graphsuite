import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue
} from '@/components/ui/Select'
import { useTranslation } from 'react-i18next'
import { useSettingsStore } from '@/stores/settings'

const languages = [
  { value: 'ja', label: '日本語' },
  { value: 'en', label: 'English' },
  { value: 'zh', label: '简体中文' },
  { value: 'zh_TW', label: '繁體中文' },
  { value: 'fr', label: 'Français' },
  { value: 'ar', label: 'العربية' }
]

export default function LanguageToggle() {
  const { i18n } = useTranslation()
  const currentLanguage = i18n.language
  const setLanguage = useSettingsStore.use.setLanguage()

  const handleLanguageChange = (value: string) => {
    i18n.changeLanguage(value)
    setLanguage(value)
  }

  return (
    <Select value={currentLanguage} onValueChange={handleLanguageChange}>
      <SelectTrigger className="w-[120px]">
        <SelectValue placeholder="Language" />
      </SelectTrigger>
      <SelectContent>
        {languages.map((lang) => (
          <SelectItem key={lang.value} value={lang.value}>
            {lang.label}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  )
}
