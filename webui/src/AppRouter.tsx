import { HashRouter as Router, Routes, Route, useNavigate } from 'react-router-dom'
import { useEffect, useState } from 'react'
import { useAuthStore } from '@/stores/state'
import { navigationService } from '@/services/navigation'
import { Toaster } from 'sonner'
import App from './App'
import LoginPage from '@/features/LoginPage'
import ThemeProvider from '@/components/ThemeProvider'

const LiquidGlassRuntime = () => {
  useEffect(() => {
    const root = document.documentElement
    root.style.setProperty('--lg-pointer-x', '50%')
    root.style.setProperty('--lg-pointer-y', '22%')

    const onScroll = () => {
      const scrollableHeight = document.body.scrollHeight - window.innerHeight
      if (scrollableHeight <= 0) {
        root.style.setProperty('--lg-scroll-ratio', '0')
        return
      }
      const ratio = Math.min(1, Math.max(0, window.scrollY / scrollableHeight))
      root.style.setProperty('--lg-scroll-ratio', ratio.toFixed(3))
    }

    window.addEventListener('scroll', onScroll, { passive: true })
    onScroll()

    return () => {
      window.removeEventListener('scroll', onScroll)
    }
  }, [])

  return null
}

const AppContent = () => {
  const [initializing, setInitializing] = useState(true)
  const { isAuthenticated } = useAuthStore()
  const navigate = useNavigate()

  // Set navigate function for navigation service
  useEffect(() => {
    navigationService.setNavigate(navigate)
  }, [navigate])

  // Token validity check
  useEffect(() => {

    const checkAuth = async () => {
      try {
        const token = localStorage.getItem('LIGHTRAG-API-TOKEN')

        if (token && isAuthenticated) {
          setInitializing(false);
          return;
        }

        if (!token) {
          useAuthStore.getState().logout()
        }
      } catch (error) {
        console.error('Auth initialization error:', error)
        if (!isAuthenticated) {
          useAuthStore.getState().logout()
        }
      } finally {
        setInitializing(false)
      }
    }

    checkAuth()

    return () => {
    }
  }, [isAuthenticated])

  // Redirect effect for protected routes
  useEffect(() => {
    if (!initializing && !isAuthenticated) {
      const currentPath = window.location.hash.slice(1)
      const allowAnonymousPath =
        currentPath === '/login' || currentPath.startsWith('/app')

      if (!allowAnonymousPath) {
        console.log('Not authenticated, redirecting to login page');
        navigate('/login');
      }
    }
  }, [initializing, isAuthenticated, navigate]);

  // Show nothing while initializing
  if (initializing) {
    return null
  }

  return (
    <Routes>
      <Route path="/" element={<LoginPage />} />
      <Route path="/login" element={<LoginPage />} />
      <Route
        path="/app/*"
        element={<App />}
      />
    </Routes>
  )
}

const AppRouter = () => {
  return (
    <ThemeProvider>
      <Router>
        <LiquidGlassRuntime />
        <AppContent />
        <Toaster
          position="bottom-center"
          theme="system"
          closeButton
          richColors
        />
      </Router>
    </ThemeProvider>
  )
}

export default AppRouter
