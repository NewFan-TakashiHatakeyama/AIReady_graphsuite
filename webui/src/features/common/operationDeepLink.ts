import type { Tab } from '@/stores/settings'

const OPERATION_DEEP_LINK_KEY = 'AIREADY_OPERATION_DEEP_LINK'

export interface OperationDeepLink {
  tab: Tab
  page?: string
  focus?: string
  createdAt: number
}

export const setOperationDeepLink = (payload: Omit<OperationDeepLink, 'createdAt'>): void => {
  if (typeof window === 'undefined') return
  const value: OperationDeepLink = {
    ...payload,
    createdAt: Date.now()
  }
  try {
    window.sessionStorage.setItem(OPERATION_DEEP_LINK_KEY, JSON.stringify(value))
  } catch (error) {
    console.warn('Failed to persist operation deep link', error)
  }
}

export const takeOperationDeepLinkForTab = (tab: Tab): OperationDeepLink | null => {
  if (typeof window === 'undefined') return null
  try {
    const raw = window.sessionStorage.getItem(OPERATION_DEEP_LINK_KEY)
    if (!raw) return null
    const parsed = JSON.parse(raw) as OperationDeepLink
    if (parsed.tab !== tab) return null
    window.sessionStorage.removeItem(OPERATION_DEEP_LINK_KEY)
    return parsed
  } catch (error) {
    console.warn('Failed to consume operation deep link', error)
    return null
  }
}
