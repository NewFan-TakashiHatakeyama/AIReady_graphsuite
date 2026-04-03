import { create } from 'zustand'
import { persist, createJSONStorage } from 'zustand/middleware'
import { createSelectors } from '@/lib/utils'
import { defaultQueryLabel } from '@/lib/constants'

type Theme = 'dark' | 'light' | 'system'
type Language = 'en' | 'zh' | 'fr' | 'ar' | 'zh_TW'
export type Tab =
  | 'aiready-dashboard'
  | 'connect-operations'
  | 'governance-operations'
  | 'ontology-operations'
  | 'audit-center'

interface SettingsState {
  // Document manager settings
  showFileName: boolean
  setShowFileName: (show: boolean) => void

  documentsPageSize: number
  setDocumentsPageSize: (size: number) => void

  // Graph viewer settings
  showPropertyPanel: boolean
  showNodeSearchBar: boolean
  showLegend: boolean
  setShowLegend: (show: boolean) => void

  showNodeLabel: boolean
  enableNodeDrag: boolean

  showEdgeLabel: boolean
  enableHideUnselectedEdges: boolean
  enableEdgeEvents: boolean

  minEdgeSize: number
  setMinEdgeSize: (size: number) => void

  maxEdgeSize: number
  setMaxEdgeSize: (size: number) => void

  graphQueryMaxDepth: number
  setGraphQueryMaxDepth: (depth: number) => void

  graphMaxNodes: number
  setGraphMaxNodes: (nodes: number, triggerRefresh?: boolean) => void

  backendMaxGraphNodes: number | null
  setBackendMaxGraphNodes: (maxNodes: number | null) => void

  graphLayoutMaxIterations: number
  setGraphLayoutMaxIterations: (iterations: number) => void

  queryLabel: string
  setQueryLabel: (queryLabel: string) => void

  // Auth settings
  apiKey: string | null
  setApiKey: (key: string | null) => void

  // App settings
  theme: Theme
  setTheme: (theme: Theme) => void

  language: Language
  setLanguage: (lang: Language) => void

  enableHealthCheck: boolean
  setEnableHealthCheck: (enable: boolean) => void

  currentTab: Tab
  setCurrentTab: (tab: Tab) => void
}

const useSettingsStoreBase = create<SettingsState>()(
  persist(
    (set) => ({
      theme: 'system',
      language: 'en',
      showPropertyPanel: true,
      showNodeSearchBar: true,
      showLegend: false,

      showNodeLabel: true,
      enableNodeDrag: true,

      showEdgeLabel: false,
      enableHideUnselectedEdges: true,
      enableEdgeEvents: false,

      minEdgeSize: 1,
      maxEdgeSize: 1,

      graphQueryMaxDepth: 3,
      graphMaxNodes: 1000,
      backendMaxGraphNodes: null,
      graphLayoutMaxIterations: 15,

      queryLabel: defaultQueryLabel,

      enableHealthCheck: true,

      apiKey: null,

      currentTab: 'aiready-dashboard',
      showFileName: false,
      documentsPageSize: 10,

      setTheme: (theme: Theme) => set({ theme }),

      setLanguage: (language: Language) => {
        set({ language })
        // Update i18n after state is updated
        import('i18next').then(({ default: i18n }) => {
          if (i18n.language !== language) {
            i18n.changeLanguage(language)
          }
        })
      },

      setGraphLayoutMaxIterations: (iterations: number) =>
        set({
          graphLayoutMaxIterations: iterations
        }),

      setQueryLabel: (queryLabel: string) =>
        set({
          queryLabel
        }),

      setGraphQueryMaxDepth: (depth: number) => set({ graphQueryMaxDepth: depth }),

      setGraphMaxNodes: (nodes: number, triggerRefresh: boolean = false) => {
        const state = useSettingsStore.getState();
        if (state.graphMaxNodes === nodes) {
          return;
        }

        if (triggerRefresh) {
          const currentLabel = state.queryLabel;
          // Atomically update both the node count and the query label to trigger a refresh.
          set({ graphMaxNodes: nodes, queryLabel: '' });

          // Restore the label after a short delay.
          setTimeout(() => {
            set({ queryLabel: currentLabel });
          }, 300);
        } else {
          set({ graphMaxNodes: nodes });
        }
      },

      setBackendMaxGraphNodes: (maxNodes: number | null) => set({ backendMaxGraphNodes: maxNodes }),

      setMinEdgeSize: (size: number) => set({ minEdgeSize: size }),

      setMaxEdgeSize: (size: number) => set({ maxEdgeSize: size }),

      setEnableHealthCheck: (enable: boolean) => set({ enableHealthCheck: enable }),

      setApiKey: (apiKey: string | null) => set({ apiKey }),

      setCurrentTab: (tab: Tab) => set({ currentTab: tab }),

      setShowFileName: (show: boolean) => set({ showFileName: show }),
      setShowLegend: (show: boolean) => set({ showLegend: show }),
      setDocumentsPageSize: (size: number) => set({ documentsPageSize: size })
    }),
    {
      name: 'settings-storage',
      storage: createJSONStorage(() => localStorage),
      version: 19,
      migrate: (state: any, version: number) => {
        if (version < 2) {
          state.showEdgeLabel = false
        }
        if (version < 3) {
          state.queryLabel = defaultQueryLabel
        }
        if (version < 4) {
          state.showPropertyPanel = true
          state.showNodeSearchBar = true
          state.showNodeLabel = true
          state.enableHealthCheck = true
          state.apiKey = null
        }
        if (version < 5) {
          state.currentTab = 'aiready-dashboard'
        }
        if (version < 7) {
          state.graphQueryMaxDepth = 3
          state.graphLayoutMaxIterations = 15
        }
        if (version < 8) {
          state.graphMinDegree = 0
          state.language = 'en'
        }
        if (version < 9) {
          state.showFileName = false
        }
        if (version < 10) {
          delete state.graphMinDegree // 删除废弃参数
          state.graphMaxNodes = 1000  // 添加新参数
        }
        if (version < 11) {
          state.minEdgeSize = 1
          state.maxEdgeSize = 1
        }
        if (version < 14) {
          // Add backendMaxGraphNodes field for older versions
          state.backendMaxGraphNodes = null
        }
        if (version < 16) {
          // Add documentsPageSize field for older versions
          state.documentsPageSize = 10
        }
        if (version < 18) {
          if (state.currentTab === 'knowledge-graph' || state.currentTab === 'document-management') {
            state.currentTab = 'aiready-dashboard'
          }
        }
        if (version < 19) {
          const allowedTabs = new Set([
            'aiready-dashboard',
            'connect-operations',
            'governance-operations',
            'ontology-operations',
            'audit-center'
          ])
          if (!allowedTabs.has(String(state.currentTab || ''))) {
            state.currentTab = 'governance-operations'
          }
        }
        if (state.currentTab === 'remediation-workflow') {
          state.currentTab = 'governance-operations'
        }
        return state
      }
    }
  )
)

const useSettingsStore = createSelectors(useSettingsStoreBase)

export { useSettingsStore, type Theme }
