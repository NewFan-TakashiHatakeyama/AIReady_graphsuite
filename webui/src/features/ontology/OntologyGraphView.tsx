import { useEffect, useMemo, useState } from 'react'
import { SigmaContainer, useRegisterEvents, useSigma } from '@react-sigma/core'
import { Settings as SigmaSettings } from 'sigma/settings'
import { EdgeArrowProgram, NodeCircleProgram, NodePointProgram } from 'sigma/rendering'
import { NodeBorderProgram } from '@sigma/node-border'
import { EdgeCurvedArrowProgram, createEdgeCurveProgram } from '@sigma/edge-curve'
import Button from '@/components/ui/Button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/Card'
import FocusOnNode from '@/components/graph/FocusOnNode'
import GraphControl from '@/components/graph/GraphControl'
import GraphLabels from '@/components/graph/GraphLabels'
import GraphSearch, { OptionItem } from '@/components/graph/GraphSearch'
import LayoutsControl from '@/components/graph/LayoutsControl'
import ZoomControl from '@/components/graph/ZoomControl'
import FullScreenControl from '@/components/graph/FullScreenControl'
import Settings from '@/components/graph/Settings'
import SettingsDisplay from '@/components/graph/SettingsDisplay'
import Legend from '@/components/graph/Legend'
import LegendButton from '@/components/graph/LegendButton'
import PropertiesView from '@/components/graph/PropertiesView'
import useGraphsuiteGraph from '@/hooks/useGraphsuiteGraph'
import { useSettingsStore } from '@/stores/settings'
import { RawNodeType, useGraphStore } from '@/stores/graph'

import '@react-sigma/core/lib/style.css'
import '@react-sigma/graph-search/lib/style.css'

type OntologyGraphFilterPreset = 'all' | 'document-centric' | 'lineage' | 'quality-attention' | 'creator-project'

const filterLabel: Record<OntologyGraphFilterPreset, string> = {
  all: 'すべて',
  'document-centric': '文書管理中心',
  lineage: '基準文書・系譜中心',
  'quality-attention': '品質要注意',
  'creator-project': '作成者・プロジェクト中心'
}

const drawRoundedRect = (
  ctx: CanvasRenderingContext2D,
  x: number,
  y: number,
  width: number,
  height: number,
  radius: number
) => {
  ctx.beginPath()
  ctx.moveTo(x + radius, y)
  ctx.lineTo(x + width - radius, y)
  ctx.quadraticCurveTo(x + width, y, x + width, y + radius)
  ctx.lineTo(x + width, y + height - radius)
  ctx.quadraticCurveTo(x + width, y + height, x + width - radius, y + height)
  ctx.lineTo(x + radius, y + height)
  ctx.quadraticCurveTo(x, y + height, x, y + height - radius)
  ctx.lineTo(x, y + radius)
  ctx.quadraticCurveTo(x, y, x + radius, y)
  ctx.closePath()
}

const ICON_STROKE_BY_TYPE: Record<string, string> = {
  project: '#0f766e',
  person: '#1d4ed8',
  document: '#7c3aed',
  organization: '#b45309',
  unknown: '#475569'
}

const drawNodeSvgIcon = (
  context: CanvasRenderingContext2D,
  iconType: string,
  centerX: number,
  centerY: number,
  size: number
) => {
  const strokeColor = ICON_STROKE_BY_TYPE[iconType] ?? ICON_STROKE_BY_TYPE.unknown
  const s = Math.max(14, size)
  const half = s / 2
  const left = centerX - half
  const top = centerY - half
  const right = centerX + half
  const bottom = centerY + half

  context.save()
  context.strokeStyle = strokeColor
  context.lineWidth = Math.max(1.6, s * 0.1)
  context.lineJoin = 'round'
  context.lineCap = 'round'

  if (iconType === 'project') {
    // Folder icon
    context.beginPath()
    context.moveTo(left + s * 0.12, top + s * 0.33)
    context.lineTo(left + s * 0.36, top + s * 0.33)
    context.lineTo(left + s * 0.44, top + s * 0.22)
    context.lineTo(right - s * 0.12, top + s * 0.22)
    context.lineTo(right - s * 0.08, bottom - s * 0.16)
    context.lineTo(left + s * 0.08, bottom - s * 0.16)
    context.closePath()
    context.stroke()
  } else if (iconType === 'person') {
    // User icon
    context.beginPath()
    context.arc(centerX, top + s * 0.34, s * 0.16, 0, Math.PI * 2)
    context.stroke()
    context.beginPath()
    context.moveTo(left + s * 0.24, bottom - s * 0.2)
    context.quadraticCurveTo(centerX, centerY + s * 0.03, right - s * 0.24, bottom - s * 0.2)
    context.stroke()
  } else if (iconType === 'document') {
    // Book icon (cover + spine lines)
    drawRoundedRect(context, left + s * 0.2, top + s * 0.16, s * 0.58, s * 0.68, s * 0.07)
    context.stroke()
    context.beginPath()
    context.moveTo(left + s * 0.34, top + s * 0.22)
    context.lineTo(left + s * 0.34, bottom - s * 0.2)
    context.stroke()
    context.beginPath()
    context.moveTo(left + s * 0.42, top + s * 0.34)
    context.lineTo(right - s * 0.28, top + s * 0.34)
    context.moveTo(left + s * 0.42, top + s * 0.48)
    context.lineTo(right - s * 0.28, top + s * 0.48)
    context.moveTo(left + s * 0.42, top + s * 0.62)
    context.lineTo(right - s * 0.32, top + s * 0.62)
    context.stroke()
    // small bookmark notch
    context.beginPath()
    context.moveTo(right - s * 0.24, top + s * 0.16)
    context.lineTo(right - s * 0.24, top + s * 0.34)
    context.lineTo(right - s * 0.18, top + s * 0.3)
    context.lineTo(right - s * 0.12, top + s * 0.34)
    context.lineTo(right - s * 0.12, top + s * 0.16)
    context.stroke()
  } else if (iconType === 'organization') {
    // Company building icon (roof + facade + door/windows)
    context.beginPath()
    context.moveTo(left + s * 0.18, top + s * 0.3)
    context.lineTo(centerX, top + s * 0.14)
    context.lineTo(right - s * 0.18, top + s * 0.3)
    context.stroke()
    drawRoundedRect(context, left + s * 0.22, top + s * 0.3, s * 0.56, s * 0.52, s * 0.03)
    context.stroke()
    // windows
    for (let row = 0; row < 2; row += 1) {
      for (let col = 0; col < 2; col += 1) {
        const wx = left + s * (0.3 + col * 0.2)
        const wy = top + s * (0.4 + row * 0.16)
        drawRoundedRect(context, wx, wy, s * 0.1, s * 0.08, s * 0.015)
        context.stroke()
      }
    }
    // center door
    drawRoundedRect(context, centerX - s * 0.06, bottom - s * 0.24, s * 0.12, s * 0.16, s * 0.015)
    context.stroke()
  } else {
    // Generic diamond icon
    context.beginPath()
    context.moveTo(centerX, top + s * 0.12)
    context.lineTo(right - s * 0.12, centerY)
    context.lineTo(centerX, bottom - s * 0.12)
    context.lineTo(left + s * 0.12, centerY)
    context.closePath()
    context.stroke()
  }
  context.restore()
}

const drawGlassNodeLabel = (
  context: CanvasRenderingContext2D,
  data: {
    x: number
    y: number
    size: number
    label?: string
    color?: string
    iconType?: string
    isCenterNode?: boolean
    isPeripheral?: boolean
  },
  settings: { labelSize?: number; labelFont?: string; labelWeight?: string }
) => {
  if (!data.label) return
  if (data.isPeripheral) return
  const label = String(data.label)
  const iconType = String(data.iconType ?? 'unknown')
  const isCenterNode = Boolean(data.isCenterNode)
  const fontSize = Math.max(12, Number(settings.labelSize ?? 13))
  const fontFamily = String(settings.labelFont ?? 'Inter, system-ui, sans-serif')
  const fontWeight = String(settings.labelWeight ?? '600')
  context.font = `${fontWeight} ${fontSize}px ${fontFamily}`
  const textWidth = context.measureText(label).width
  const paddingX = 14
  const paddingY = 6
  const iconSize = Math.max(14, data.size * 2)
  const iconGap = 6
  const minRectWidth = isCenterNode ? 210 : 150
  const rectWidth = Math.max(minRectWidth, textWidth + paddingX * 2)
  const rectHeight = fontSize + paddingY * 1.4
  const rectX = data.x + iconSize / 2 + iconGap
  const rectY = data.y - rectHeight / 2
  const tintColor = String(data.color ?? '')
  const glassTint = tintColor.includes('59, 130, 246') ? 'rgba(59, 130, 246, 0.14)' : 'rgba(148, 163, 184, 0.14)'

  context.save()
  drawNodeSvgIcon(context, iconType, data.x, data.y, isCenterNode ? iconSize * 1.06 : iconSize)

  context.shadowColor = 'rgba(15, 23, 42, 0.18)'
  context.shadowBlur = 16
  context.shadowOffsetY = 3
  drawRoundedRect(context, rectX, rectY, rectWidth, rectHeight, 9)
  const baseGradient = context.createLinearGradient(rectX, rectY, rectX, rectY + rectHeight)
  baseGradient.addColorStop(0, 'rgba(255, 255, 255, 0.76)')
  baseGradient.addColorStop(1, 'rgba(255, 255, 255, 0.56)')
  context.fillStyle = baseGradient
  context.fill()
  const tintGradient = context.createLinearGradient(rectX, rectY, rectX + rectWidth, rectY + rectHeight)
  tintGradient.addColorStop(0, 'rgba(255, 255, 255, 0.00)')
  tintGradient.addColorStop(1, glassTint)
  context.fillStyle = tintGradient
  context.fill()
  context.shadowBlur = 0
  context.strokeStyle = 'rgba(148, 163, 184, 0.4)'
  context.lineWidth = 1
  context.stroke()
  // Soft glossy streak near the top edge to emphasize liquid-glass feel.
  drawRoundedRect(context, rectX + 1.5, rectY + 1.5, rectWidth - 3, Math.max(5, rectHeight * 0.36), 8)
  context.fillStyle = 'rgba(255, 255, 255, 0.20)'
  context.fill()
  context.fillStyle = 'rgba(15, 23, 42, 0.88)'
  context.textBaseline = 'middle'
  context.textAlign = 'left'
  context.font = `${fontWeight} ${fontSize}px ${fontFamily}`
  context.fillText(label, rectX + paddingX, data.y)
  context.restore()
}

const suppressHoverLabel = () => {
  // Avoid duplicate text layers (base label + hover label).
}

const OntologyGraphEvents = () => {
  const registerEvents = useRegisterEvents()
  const sigma = useSigma()
  const [draggedNode, setDraggedNode] = useState<string | null>(null)

  useEffect(() => {
    registerEvents({
      downNode: (event) => {
        setDraggedNode(event.node)
      },
      mousemovebody: (event) => {
        if (!draggedNode) return
        const pos = sigma.viewportToGraph(event)
        sigma.getGraph().setNodeAttribute(draggedNode, 'x', pos.x)
        sigma.getGraph().setNodeAttribute(draggedNode, 'y', pos.y)
        event.preventSigmaDefault()
        event.original.preventDefault()
        event.original.stopPropagation()
      },
      mouseup: () => {
        if (draggedNode) setDraggedNode(null)
      },
      mousedown: (event) => {
        const mouseEvent = event.original as MouseEvent
        if (mouseEvent.buttons !== 0 && !sigma.getCustomBBox()) {
          sigma.setCustomBBox(sigma.getBBox())
        }
      }
    })
  }, [registerEvents, sigma, draggedNode])

  return null
}

const sigmaSettings: Partial<SigmaSettings> = {
  allowInvalidContainer: true,
  skipErrors: true,
  defaultNodeType: 'circle',
  defaultEdgeType: 'curvedNoArrow',
  renderEdgeLabels: false,
  edgeProgramClasses: {
    arrow: EdgeArrowProgram,
    curvedArrow: EdgeCurvedArrowProgram,
    curvedNoArrow: createEdgeCurveProgram()
  },
  nodeProgramClasses: {
    default: NodeBorderProgram,
    circle: NodeCircleProgram,
    circel: NodeCircleProgram,
    point: NodePointProgram
  },
  labelGridCellSize: 60,
  labelRenderedSizeThreshold: 0,
  labelFont: 'Inter, system-ui, sans-serif',
  labelWeight: '600',
  labelSize: 13,
  defaultDrawNodeLabel: drawGlassNodeLabel,
  defaultDrawNodeHover: suppressHoverLabel,
  enableEdgeEvents: true
}

const normalizedType = (node: RawNodeType): string => {
  const fromNode = node.properties?._normalized_entity_type
  if (typeof fromNode === 'string') return fromNode
  const entityType = node.properties?.entity_type
  if (typeof entityType === 'string') return entityType.toLowerCase()
  const label = node.labels[0]?.toLowerCase() ?? ''
  if (label.includes('.ppt') || label.includes('.doc') || label.includes('.pdf') || label.includes('document')) {
    return 'document'
  }
  return 'unknown'
}

const isDocumentLike = (node: RawNodeType): boolean => normalizedType(node) === 'document'
const isLineageDocument = (node: RawNodeType): boolean =>
  isDocumentLike(node) && String(node.properties?.document_kind ?? '') === 'derived'
const isQualityAttention = (node: RawNodeType): boolean =>
  isDocumentLike(node) && Number(node.properties?.contentQualityScore ?? 1) < 0.75

const OntologyGraphView = ({
  onNavigate,
  embedded = false,
  fixedQueryLabel
}: {
  onNavigate?: (page: 'entity-candidates' | 'lineage', focus?: string) => void
  embedded?: boolean
  fixedQueryLabel?: string
}) => {
  const enableDummyFallback = String(import.meta.env.VITE_ENABLE_ONTOLOGY_DUMMY_FALLBACK ?? 'false').toLowerCase() === 'true'
  const effectiveFixedQueryLabel = fixedQueryLabel?.trim()
  const graphIsEmpty = useGraphStore.use.graphIsEmpty()
  const [embeddedQueryLabel, setEmbeddedQueryLabel] = useState(effectiveFixedQueryLabel || '*')
  const [embeddedFallbackApplied, setEmbeddedFallbackApplied] = useState(false)

  useEffect(() => {
    if (!embedded) return
    setEmbeddedQueryLabel(effectiveFixedQueryLabel || '*')
    setEmbeddedFallbackApplied(false)
  }, [embedded, effectiveFixedQueryLabel])

  useGraphsuiteGraph(
    'ontology',
    embedded
      ? {
        queryLabelOverride: embeddedQueryLabel,
        maxDepthOverride: 2,
        maxNodesOverride: 80
      }
      : {}
  )
  const [preset, setPreset] = useState<OntologyGraphFilterPreset>('all')
  const selectedNode = useGraphStore.use.selectedNode()
  const focusedNode = useGraphStore.use.focusedNode()
  const moveToSelectedNode = useGraphStore.use.moveToSelectedNode()
  const sigmaGraph = useGraphStore.use.sigmaGraph()
  const rawGraph = useGraphStore.use.rawGraph()
  const sigmaInstance = useGraphStore.use.sigmaInstance()
  const isFetching = useGraphStore.use.isFetching()
  const showLegend = useSettingsStore.use.showLegend()
  const enableNodeDrag = useSettingsStore.use.enableNodeDrag()
  const queryLabel = useSettingsStore.use.queryLabel()

  const autoFocusedNode = useMemo(() => focusedNode ?? selectedNode, [focusedNode, selectedNode])
  const searchInitSelectedNode = useMemo(
    (): OptionItem | null => (selectedNode ? { type: 'nodes', id: selectedNode } : null),
    [selectedNode]
  )

  useEffect(() => {
    if (!embedded || isFetching) return
    // When an item-specific label has no graph, fallback to wildcard to keep panel usable.
    if (enableDummyFallback && graphIsEmpty && !embeddedFallbackApplied && embeddedQueryLabel !== '*') {
      setEmbeddedQueryLabel('*')
      setEmbeddedFallbackApplied(true)
    }
  }, [embedded, isFetching, graphIsEmpty, embeddedFallbackApplied, embeddedQueryLabel, enableDummyFallback])

  useEffect(() => {
    if (embedded) return
    if (!queryLabel) {
      useSettingsStore.getState().setQueryLabel('*')
      useGraphStore.getState().setGraphDataFetchAttempted(false)
    }
  }, [queryLabel, embedded])

  useEffect(() => {
    if (!sigmaGraph || !rawGraph) return

    const selectedNodeId = useGraphStore.getState().selectedNode
    const selectedNodeData = selectedNodeId ? rawGraph.getNode(selectedNodeId) : null
    const documentNeighborhood = new Set<string>()

    if (preset === 'document-centric') {
      const documentIds = rawGraph.nodes.filter((node) => isDocumentLike(node)).map((node) => node.id)
      documentIds.forEach((id) => documentNeighborhood.add(id))
      if (selectedNodeData && isDocumentLike(selectedNodeData)) {
        const neighbors = sigmaGraph.neighbors(selectedNodeData.id)
        neighbors.forEach((id) => documentNeighborhood.add(id))
      } else if (selectedNodeData) {
        documentNeighborhood.add(selectedNodeData.id)
        sigmaGraph.neighbors(selectedNodeData.id).forEach((id) => documentNeighborhood.add(id))
      }
    }

    sigmaGraph.forEachNode((nodeId) => {
      const node = rawGraph.getNode(nodeId)
      if (!node) return
      let visible = true
      if (preset === 'lineage') {
        visible = isLineageDocument(node) || (isDocumentLike(node) && String(node.properties?.document_kind) === 'canonical')
      } else if (preset === 'quality-attention') {
        visible = isQualityAttention(node)
      } else if (preset === 'creator-project') {
        const type = normalizedType(node)
        visible = type === 'person' || type === 'project' || type === 'document'
      } else if (preset === 'document-centric') {
        visible = documentNeighborhood.has(nodeId)
      }
      sigmaGraph.setNodeAttribute(nodeId, 'hidden', !visible)
    })

    sigmaGraph.forEachEdge((edgeId, attrs, source, target) => {
      const sourceHidden = sigmaGraph.getNodeAttribute(source, 'hidden')
      const targetHidden = sigmaGraph.getNodeAttribute(target, 'hidden')
      sigmaGraph.setEdgeAttribute(edgeId, 'hidden', Boolean(sourceHidden || targetHidden))
    })

    sigmaInstance?.refresh()
  }, [preset, sigmaGraph, rawGraph, sigmaInstance])

  return (
    <div className={embedded ? '' : 'space-y-3'}>
      {!embedded && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle>関係グラフ</CardTitle>
            <CardDescription>
              ファイル名・作成者・関連プロジェクト・基準文書系譜を軸に、AI活用に必要な文書管理状態を可視化します。
              スコアリング実行後にグラフが空の場合は、投影再構築（projection refresh）を実行してください。
            </CardDescription>
          </CardHeader>
          <CardContent className="pt-0">
            <div className="flex flex-wrap gap-2">
              {(Object.keys(filterLabel) as OntologyGraphFilterPreset[]).map((item) => (
                <Button
                  key={item}
                  size="sm"
                  variant={preset === item ? 'default' : 'outline'}
                  onClick={() => setPreset(item)}
                >
                  {filterLabel[item]}
                </Button>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      <div
        className={
          embedded
            ? 'liquid-glass-surface liquid-glass-clear relative h-[620px] w-full overflow-hidden rounded-xl border border-white/25'
            : 'liquid-glass-surface liquid-glass-clear relative h-[720px] w-full overflow-hidden rounded-xl border border-white/25'
        }
      >
        <SigmaContainer
          settings={sigmaSettings}
          className="!bg-background !size-full overflow-hidden"
          style={{ width: '100%', height: '100%' }}
        >
          {enableNodeDrag && <OntologyGraphEvents />}
          <GraphControl />
          <FocusOnNode
            node={autoFocusedNode}
            move={moveToSelectedNode}
            horizontalOffsetRatio={embedded ? 0.32 : 0.28}
          />

          {!embedded && (
            <div className="absolute top-2 left-2 flex items-start gap-2">
              <GraphLabels source="ontology" />
              <GraphSearch
                value={searchInitSelectedNode}
                onFocus={(value) => {
                  if (value === null) useGraphStore.getState().setFocusedNode(null)
                  else if (value.type === 'nodes') useGraphStore.getState().setFocusedNode(value.id)
                }}
                onChange={(value) => {
                  if (value === null) {
                    useGraphStore.getState().setSelectedNode(null)
                  } else if (value.type === 'nodes') {
                    useGraphStore.getState().setSelectedNode(value.id, true)
                  }
                }}
              />
            </div>
          )}

          <div className="liquid-glass-surface liquid-glass-clear absolute bottom-2 left-2 flex flex-col rounded-xl border border-white/30 p-1">
            <LayoutsControl />
            <ZoomControl />
            <FullScreenControl />
            {!embedded && <LegendButton />}
            <Settings />
          </div>

          <div className="absolute top-2 right-2">
            <PropertiesView
              variant="ontology"
              onNavigate={(page, focus) => onNavigate?.(page, focus)}
            />
          </div>

          {showLegend && !embedded && (
            <div className="absolute bottom-10 right-2">
              <Legend className="liquid-glass-surface liquid-glass-clear border-white/30" />
            </div>
          )}
          {!embedded && <SettingsDisplay />}
        </SigmaContainer>

        {isFetching && (
          <div className="absolute inset-0 z-10 flex items-center justify-center bg-background/80">
            <div className="text-center">
              <div className="mb-2 h-8 w-8 animate-spin rounded-full border-4 border-primary border-t-transparent" />
              <p>グラフを読み込み中...</p>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

export default OntologyGraphView
