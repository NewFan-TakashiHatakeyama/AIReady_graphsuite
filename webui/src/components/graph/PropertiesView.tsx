import { useCallback, useEffect, useState } from 'react'
import { useGraphStore, RawNodeType, RawEdgeType } from '@/stores/graph'
import Text from '@/components/ui/Text'
import Button from '@/components/ui/Button'
import { useTranslation } from 'react-i18next'
import { GitBranchPlus, Scissors } from 'lucide-react'
import EditablePropertyRow from './EditablePropertyRow'

/**
 * Component that view properties of elements in graph.
 */
const PropertiesView = ({
  variant = 'default'
}: {
  variant?: 'default' | 'ontology'
}) => {
  const rawGraph = useGraphStore.use.rawGraph()
  const getNode = useCallback((nodeId: string) => rawGraph?.getNode(nodeId) || null, [rawGraph])
  const getEdge = useCallback(
    (edgeId: string, dynamicId: boolean = true) => rawGraph?.getEdge(edgeId, dynamicId) || null,
    [rawGraph]
  )
  const selectedNode = useGraphStore.use.selectedNode()
  const focusedNode = useGraphStore.use.focusedNode()
  const selectedEdge = useGraphStore.use.selectedEdge()
  const focusedEdge = useGraphStore.use.focusedEdge()
  const graphDataVersion = useGraphStore.use.graphDataVersion()

  const [currentElement, setCurrentElement] = useState<NodeType | EdgeType | null>(null)
  const [currentType, setCurrentType] = useState<'node' | 'edge' | null>(null)

  // This effect will run when selection changes or when graph data is updated
  useEffect(() => {
    let type: 'node' | 'edge' | null = null
    let element: RawNodeType | RawEdgeType | null = null
    if (focusedNode) {
      type = 'node'
      element = getNode(focusedNode)
    } else if (selectedNode) {
      type = 'node'
      element = getNode(selectedNode)
    } else if (focusedEdge) {
      type = 'edge'
      element = getEdge(focusedEdge, true)
    } else if (selectedEdge) {
      type = 'edge'
      element = getEdge(selectedEdge, true)
    }

    if (element) {
      if (type == 'node') {
        setCurrentElement(refineNodeProperties(element as any))
      } else {
        setCurrentElement(refineEdgeProperties(element as any))
      }
      setCurrentType(type)
    } else {
      setCurrentElement(null)
      setCurrentType(null)
    }
  }, [
    focusedNode,
    selectedNode,
    focusedEdge,
    selectedEdge,
    graphDataVersion, // Add dependency on graphDataVersion to refresh when data changes
    setCurrentElement,
    setCurrentType,
    getNode,
    getEdge
  ])

  if (!currentElement) {
    return <></>
  }
  return (
    <div className="liquid-glass-surface liquid-glass-clear w-[26rem] max-w-[80vw] rounded-lg border border-white/30 p-2 text-xs">
      {currentType == 'node' ? (
        <NodePropertiesView node={currentElement as any} variant={variant} />
      ) : (
        <EdgePropertiesView edge={currentElement as any} variant={variant} />
      )}
    </div>
  )
}

type NodeType = RawNodeType & {
  relationships: {
    type: string
    id: string
    label: string
    relationType?: string
    entityType?: string
    edgeDynamicId?: string
    edgeId?: string
    edgeKeywords?: string
    edgeWeight?: number
  }[]
}

type EdgeType = RawEdgeType & {
  sourceNode?: RawNodeType
  targetNode?: RawNodeType
}

const refineNodeProperties = (node: RawNodeType): NodeType => {
  const state = useGraphStore.getState()
  const relationships = []

  if (state.sigmaGraph && state.rawGraph) {
    try {
      if (!state.sigmaGraph.hasNode(node.id)) {
        console.warn('Node not found in sigmaGraph:', node.id)
        return {
          ...node,
          relationships: []
        }
      }

      const edges = state.sigmaGraph.edges(node.id)

      for (const edgeId of edges) {
        if (!state.sigmaGraph.hasEdge(edgeId)) continue;

        const edge = state.rawGraph.getEdge(edgeId, true)
        if (edge) {
          const isTarget = node.id === edge.source
          const neighbourId = isTarget ? edge.target : edge.source

          if (!state.sigmaGraph.hasNode(neighbourId)) continue;

          const neighbour = state.rawGraph.getNode(neighbourId)
          if (neighbour) {
            relationships.push({
              type: 'Neighbour',
              id: neighbourId,
              label: neighbour.properties['entity_id'] ? neighbour.properties['entity_id'] : neighbour.labels.join(', '),
              relationType: String(edge.type ?? '').trim().toLowerCase(),
              entityType: String(neighbour.properties?.entity_type ?? '').trim().toLowerCase(),
              edgeDynamicId: String(edge.dynamicId ?? ''),
              edgeId: String(edge.id ?? ''),
              edgeKeywords: String(edge.properties?.keywords ?? '').trim(),
              edgeWeight:
                edge.properties?.weight !== undefined
                  ? Number(edge.properties.weight)
                  : edge.properties?.similarity_score !== undefined
                    ? Number(edge.properties.similarity_score)
                    : undefined
            })
          }
        }
      }
    } catch (error) {
      console.error('Error refining node properties:', error)
    }
  }

  return {
    ...node,
    relationships
  }
}

const refineEdgeProperties = (edge: RawEdgeType): EdgeType => {
  const state = useGraphStore.getState()
  let sourceNode: RawNodeType | undefined = undefined
  let targetNode: RawNodeType | undefined = undefined

  if (state.sigmaGraph && state.rawGraph) {
    try {
      if (!state.sigmaGraph.hasEdge(edge.dynamicId)) {
        console.warn('Edge not found in sigmaGraph:', edge.id, 'dynamicId:', edge.dynamicId)
        return {
          ...edge,
          sourceNode: undefined,
          targetNode: undefined
        }
      }

      if (state.sigmaGraph.hasNode(edge.source)) {
        sourceNode = state.rawGraph.getNode(edge.source)
      }

      if (state.sigmaGraph.hasNode(edge.target)) {
        targetNode = state.rawGraph.getNode(edge.target)
      }
    } catch (error) {
      console.error('Error refining edge properties:', error)
    }
  }

  return {
    ...edge,
    sourceNode,
    targetNode
  }
}

const PropertyRow = ({
  name,
  value,
  onClick,
  tooltip,
  nodeId,
  edgeId,
  dynamicId,
  entityId,
  entityType,
  sourceId,
  targetId,
  isEditable = false
}: {
  name: string
  value: any
  onClick?: () => void
  tooltip?: string
  nodeId?: string
  entityId?: string
  edgeId?: string
  dynamicId?: string
  entityType?: 'node' | 'edge'
  sourceId?: string
  targetId?: string
  isEditable?: boolean
}) => {
  const { t } = useTranslation()

  const getPropertyNameTranslation = (name: string) => {
    const translationKey = `graphPanel.propertiesView.node.propertyNames.${name}`
    const translation = t(translationKey)
    return translation === translationKey ? name : translation
  }

  // Use EditablePropertyRow for editable fields (description, entity_id and keywords)
  if (isEditable && (name === 'description' || name === 'entity_id' || name === 'keywords')) {
    return (
      <EditablePropertyRow
        name={name}
        value={value}
        onClick={onClick}
        nodeId={nodeId}
        entityId={entityId}
        edgeId={edgeId}
        dynamicId={dynamicId}
        entityType={entityType}
        sourceId={sourceId}
        targetId={targetId}
        isEditable={true}
        tooltip={tooltip || (typeof value === 'string' ? value : JSON.stringify(value, null, 2))}
      />
    )
  }

  // For non-editable fields, use the regular Text component
  return (
    <div className="flex items-center gap-2">
      <span className="text-primary/60 tracking-wide whitespace-nowrap">{getPropertyNameTranslation(name)}</span>:
      <Text
        className="hover:bg-primary/20 rounded p-1 overflow-hidden text-ellipsis"
        tooltipClassName="max-w-80 -translate-x-13"
        text={value}
        tooltip={tooltip || (typeof value === 'string' ? value : JSON.stringify(value, null, 2))}
        side="left"
        onClick={onClick}
      />
    </div>
  )
}

const NodePropertiesView = ({
  node,
  variant
}: {
  node: NodeType
  variant: 'default' | 'ontology'
}) => {
  const { t } = useTranslation()

  const handleExpandNode = () => {
    useGraphStore.getState().triggerNodeExpand(node.id)
  }

  const handlePruneNode = () => {
    useGraphStore.getState().triggerNodePrune(node.id)
  }

  const ontologyType = String(
    node.properties?._normalized_entity_type ?? node.properties?.entity_type ?? 'unknown'
  ).toLowerCase()
  const isOntologyDocument = variant === 'ontology' && ontologyType === 'document'
  const projectFallback = node.relationships
    .filter(
      (relationship) =>
        relationship.relationType === 'belongs_to_project' || relationship.entityType === 'project'
    )
    .map((relationship) => relationship.label)
    .find((label) => Boolean(String(label).trim()))
  const topicFallback = node.relationships
    .filter(
      (relationship) =>
        relationship.relationType === 'categorized_as' || relationship.entityType === 'topic_category'
    )
    .map((relationship) => relationship.label)
    .filter((label) => Boolean(String(label).trim()))
  const projectValue = String(node.properties?.project ?? node.properties?.project_name ?? '').trim()
  const topicFromProperties = (() => {
    if (Array.isArray(node.properties?.topic_categories)) {
      return node.properties.topic_categories
    }
    if (typeof node.properties?.topic_categories === 'string') {
      return String(node.properties.topic_categories)
        .split(',')
        .map((value) => value.trim())
        .filter((value) => value.length > 0)
    }
    return []
  })()
  const topicValues = topicFromProperties.length > 0 ? topicFromProperties : topicFallback
  const ontologyHiddenKeys = new Set([
    '_normalized_entity_type',
    '_resolution_status',
    'resolutionStatus',
    'resolution_status',
    'confidence',
    'contentQualityScore'
  ])
  const ontologyPriorityKeys = ['item_id', 'file_name']

  return (
    <div className="flex flex-col gap-2">
      <div className="flex justify-between items-center">
        <h3 className="text-md pl-1 font-bold tracking-wide text-blue-700">{t('graphPanel.propertiesView.node.title')}</h3>
        <div className="flex gap-3">
          <Button
            size="icon"
            variant="ghost"
            className="h-7 w-7 border border-gray-400 hover:bg-gray-200 dark:border-gray-600 dark:hover:bg-gray-700"
            onClick={handleExpandNode}
            tooltip={t('graphPanel.propertiesView.node.expandNode')}
          >
            <GitBranchPlus className="h-4 w-4 text-gray-700 dark:text-gray-300" />
          </Button>
          <Button
            size="icon"
            variant="ghost"
            className="h-7 w-7 border border-gray-400 hover:bg-gray-200 dark:border-gray-600 dark:hover:bg-gray-700"
            onClick={handlePruneNode}
            tooltip={t('graphPanel.propertiesView.node.pruneNode')}
          >
            <Scissors className="h-4 w-4 text-gray-900 dark:text-gray-300" />
          </Button>
        </div>
      </div>
      {variant === 'default' && (
        <>
          <div className="max-h-96 overflow-auto rounded p-1">
            <PropertyRow name={t('graphPanel.propertiesView.node.id')} value={String(node.id)} />
            <PropertyRow
              name={t('graphPanel.propertiesView.node.labels')}
              value={node.labels.join(', ')}
              onClick={() => {
                useGraphStore.getState().setSelectedNode(node.id, true)
              }}
            />
            <PropertyRow name={t('graphPanel.propertiesView.node.degree')} value={node.degree} />
          </div>
          <h3 className="text-md pl-1 font-bold tracking-wide text-amber-700">{t('graphPanel.propertiesView.node.properties')}</h3>
          <div className="max-h-96 overflow-auto rounded p-1">
            {[
              ...ontologyPriorityKeys,
              ...Object.keys(node.properties).sort().filter((key) => !ontologyPriorityKeys.includes(key))
            ]
              .map((name) => {
                if (!(name in node.properties)) return null
                if (name === 'created_at') return null; // Hide created_at property
                if (variant === 'ontology' && (ontologyHiddenKeys.has(name) || name.startsWith('_'))) return null
                return (
                  <PropertyRow
                    key={name}
                    name={name}
                    value={node.properties[name]}
                    nodeId={String(node.id)}
                    entityId={node.properties['entity_id']}
                    entityType="node"
                    isEditable={name === 'description' || name === 'entity_id'}
                  />
                )
              })}
          </div>
          {node.relationships.length > 0 && (
            <>
              <h3 className="text-md pl-1 font-bold tracking-wide text-emerald-700">
                {t('graphPanel.propertiesView.node.relationships')}
              </h3>
              <div className="max-h-96 overflow-auto rounded p-1">
                {node.relationships.map(({ type, id, label }) => {
                  return (
                    <PropertyRow
                      key={id}
                      name={type}
                      value={label}
                      onClick={() => {
                        useGraphStore.getState().setSelectedNode(id, true)
                      }}
                    />
                  )
                })}
              </div>
            </>
          )}
        </>
      )}
      {variant === 'ontology' && (
        <>
          <div className="max-h-96 overflow-auto rounded p-2 space-y-2">
            <PropertyRow name="ノードID" value={String(node.id)} />
            <PropertyRow
              name="ラベル"
              value={node.labels.join(', ')}
              onClick={() => {
                useGraphStore.getState().setSelectedNode(node.id, true)
              }}
            />
            <PropertyRow name="次数" value={node.degree} />
          </div>
          <h3 className="text-md pl-1 font-bold tracking-wide text-amber-700">プロパティ</h3>
          <div className="max-h-96 overflow-auto rounded p-2 space-y-2">
            <PropertyRow name="表示名" value={String(node.properties?.file_name ?? node.labels?.[0] ?? '-')} />
            <PropertyRow name="タイプ" value={String(ontologyType)} />
            {String(node.properties?.item_id ?? '').trim() && (
              <PropertyRow name="アイテムID" value={String(node.properties?.item_id)} />
            )}
            {isOntologyDocument ? (
              <>
                <PropertyRow name="所有者" value={String(node.properties?.owner ?? node.properties?.creator_name ?? '-')} />
                <PropertyRow name="プロジェクト" value={projectValue || projectFallback || '-'} />
                <PropertyRow
                  name="トピック"
                  value={topicValues.length > 0 ? topicValues.join(', ') : '-'}
                />
                <PropertyRow
                  name="データソース"
                  value={String(node.properties?.source ?? node.properties?.data_source ?? node.properties?.extraction_source ?? '-')}
                />
                <PropertyRow
                  name="文書種別"
                  value={String(node.properties?.document_kind === 'canonical' ? '基準文書' : '派生文書')}
                />
                <PropertyRow name="基準文書ID" value={String(node.properties?.canonical_doc_id ?? '-')} />
                <PropertyRow name="鮮度" value={String(node.properties?.freshnessStatus ?? '-')} />
                <PropertyRow name="系譜ID" value={String(node.properties?.lineage_id ?? '-')} />
                <PropertyRow name="相関ID" value={String(node.properties?.correlation_id ?? '-')} />
              </>
            ) : (
              <>
                <PropertyRow
                  name="解決状態"
                  value={String(node.properties?._resolution_status ?? node.properties?.resolution_status ?? 'resolved')}
                />
                <PropertyRow
                  name="抽出元"
                  value={String(node.properties?.extraction_source ?? node.properties?.source ?? '-')}
                />
                <PropertyRow name="系譜ID" value={String(node.properties?.lineage_id ?? '-')} />
                <PropertyRow name="相関ID" value={String(node.properties?.correlation_id ?? '-')} />
              </>
            )}
          </div>
          {node.relationships.length > 0 && (
            <>
              <h3 className="text-md pl-1 font-bold tracking-wide text-emerald-700">関連エンティティ / エッジ関係</h3>
              <div className="max-h-72 overflow-auto rounded p-2 space-y-2">
                {node.relationships.map((relationship) => {
                  const relationLabel = relationship.relationType || relationship.type
                  const neighbourValue =
                    relationship.entityType && relationship.entityType.length > 0
                      ? `${relationship.label} (${relationship.entityType})`
                      : relationship.label
                  const edgeMeta = [
                    relationship.edgeKeywords ? `根拠: ${relationship.edgeKeywords}` : '',
                    relationship.edgeWeight !== undefined && Number.isFinite(relationship.edgeWeight)
                      ? `重み: ${relationship.edgeWeight}`
                      : ''
                  ]
                    .filter(Boolean)
                    .join(' / ')
                  return (
                    <div key={`${relationship.id}:${relationship.edgeDynamicId ?? relationLabel}`} className="rounded border border-primary/10 p-2">
                      <PropertyRow
                        name="関係"
                        value={relationLabel}
                        onClick={() => {
                          if (relationship.edgeDynamicId) {
                            useGraphStore.getState().setSelectedEdge(relationship.edgeDynamicId)
                          }
                        }}
                      />
                      <PropertyRow
                        name="接続先"
                        value={neighbourValue}
                        onClick={() => {
                          useGraphStore.getState().setSelectedNode(relationship.id, true)
                        }}
                      />
                      {edgeMeta && <PropertyRow name="エッジ情報" value={edgeMeta} />}
                    </div>
                  )
                })}
              </div>
            </>
          )}
        </>
      )}
    </div>
  )
}

const EdgePropertiesView = ({
  edge,
  variant
}: {
  edge: EdgeType
  variant: 'default' | 'ontology'
}) => {
  const { t } = useTranslation()
  return (
    <div className="flex flex-col gap-2">
      <h3 className="text-md pl-1 font-bold tracking-wide text-violet-700">{t('graphPanel.propertiesView.edge.title')}</h3>
      <div className="max-h-96 overflow-auto rounded p-1">
        <PropertyRow name={t('graphPanel.propertiesView.edge.id')} value={edge.id} />
        {edge.type && <PropertyRow name={t('graphPanel.propertiesView.edge.type')} value={edge.type} />}
        <PropertyRow
          name={t('graphPanel.propertiesView.edge.source')}
          value={edge.sourceNode ? edge.sourceNode.labels.join(', ') : edge.source}
          onClick={() => {
            useGraphStore.getState().setSelectedNode(edge.source, true)
          }}
        />
        <PropertyRow
          name={t('graphPanel.propertiesView.edge.target')}
          value={edge.targetNode ? edge.targetNode.labels.join(', ') : edge.target}
          onClick={() => {
            useGraphStore.getState().setSelectedNode(edge.target, true)
          }}
        />
      </div>
      <h3 className="text-md pl-1 font-bold tracking-wide text-amber-700">{t('graphPanel.propertiesView.edge.properties')}</h3>
      <div className="max-h-96 overflow-auto rounded p-1">
        {Object.keys(edge.properties)
          .sort()
          .map((name) => {
            if (name === 'created_at') return null; // Hide created_at property
            return (
              <PropertyRow
                key={name}
                name={name}
                value={edge.properties[name]}
                edgeId={String(edge.id)}
                dynamicId={String(edge.dynamicId)}
                entityType="edge"
                sourceId={edge.sourceNode?.properties['entity_id'] || edge.source}
                targetId={edge.targetNode?.properties['entity_id'] || edge.target}
                isEditable={name === 'description' || name === 'keywords'}
              />
            )
          })}
      </div>
      {variant === 'ontology' && (
        <>
          <h3 className="text-md pl-1 font-bold tracking-wide text-indigo-700">関係根拠</h3>
          <div className="max-h-96 overflow-auto rounded p-1">
            <PropertyRow name="normalized_type" value={String(edge.properties?._normalized_edge_type ?? edge.type ?? '-')} />
            <PropertyRow name="keywords" value={String(edge.properties?.keywords ?? '-')} />
            <PropertyRow name="weight" value={String(edge.properties?.weight ?? '-')} />
          </div>
        </>
      )}
    </div>
  )
}

export default PropertiesView
