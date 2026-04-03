import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { useAuthStore } from '@/stores/state'
import { loginToServer } from '@/api/graphsuite'
import { errorMessage } from '@/lib/utils'
import { toast } from 'sonner'
import { siteConfig } from '@/lib/constants'
import { useTranslation } from 'react-i18next'
import LanguageToggle from '@/components/LanguageToggle'
import ThemeToggle from '@/components/ThemeToggle'
import Button from '@/components/ui/Button'
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle
} from '@/components/ui/Card'
import Input from '@/components/ui/Input'
import { Label } from '@/components/ui/Label'

const LoginPage = () => {
  const { t } = useTranslation()
  const navigate = useNavigate()
  const login = useAuthStore((state) => state.login)
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [loading, setLoading] = useState(false)

  const handleLogin = async (e: React.FormEvent) => {
    e.preventDefault()
    setLoading(true)
    try {
      const response = await loginToServer(username, password)
      login(
        response.access_token,
        response.auth_mode === 'disabled',
        response.core_version,
        response.api_version,
        response.webui_title,
        response.webui_description
      )
      toast.success(t('login_page.login_successful'))
      navigate('/app')
    } catch (error) {
      toast.error(t('login_page.login_failed'), {
        description: errorMessage(error)
      })
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="liquid-glass-page relative flex min-h-screen flex-col items-center justify-center">
      <div className="absolute right-4 top-4 flex items-center gap-2">
        <LanguageToggle />
        <ThemeToggle />
      </div>
      <Card className="w-full max-w-sm">
        <form onSubmit={handleLogin}>
          <CardHeader>
            <CardTitle className="text-2xl">{t(siteConfig.name)}</CardTitle>
            <CardDescription>{t('login_page.enter_your_credentials')}</CardDescription>
          </CardHeader>
          <CardContent className="grid gap-4">
            <div className="grid gap-2">
              <Label htmlFor="username">{t('login_page.username')}</Label>
              <Input
                id="username"
                type="text"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                placeholder={t('login_page.username_placeholder')}
                required
                autoComplete="username"
              />
            </div>
            <div className="grid gap-2">
              <Label htmlFor="password">{t('login_page.password')}</Label>
              <Input
                id="password"
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                placeholder="********"
                required
                autoComplete="current-password"
              />
            </div>
          </CardContent>
          <CardFooter>
            <Button type="submit" className="w-full" disabled={loading}>
              {loading ? t('login_page.logging_in') : t('login_page.login')}
            </Button>
          </CardFooter>
        </form>
      </Card>
    </div>
  )
}

export default LoginPage
