import Graph, { UndirectedGraph } from 'graphology'
import { useCallback, useEffect, useRef } from 'react'
import { useTranslation } from 'react-i18next'
import { errorMessage } from '@/lib/utils'
import * as Constants from '@/lib/constants'
import { useGraphStore, RawGraph, RawNodeType, RawEdgeType } from '@/stores/graph'
import { toast } from 'sonner'
import { getGraphLabels, queryGraphs } from '@/api/graphsuite'
import { useBackendState } from '@/stores/state'
import { useSettingsStore } from '@/stores/settings'

import seedrandom from 'seedrandom'

const TYPE_SYNONYMS: Record<string, string> = {
  'unknown': 'unknown',
  '未知': 'unknown',
  'other': 'unknown',

  'category': 'category',
  '类别': 'category',
  'type': 'category',
  '分类': 'category',

  'organization': 'organization',
  '组织': 'organization',
  'org': 'organization',
  'company': 'organization',
  '公司': 'organization',
  '机构': 'organization',

  'event': 'event',
  '事件': 'event',
  'activity': 'event',
  '活动': 'event',

  'person': 'person',
  '人物': 'person',
  'people': 'person',
  'human': 'person',
  '人': 'person',

  'animal': 'animal',
  '动物': 'animal',
  'creature': 'animal',
  '生物': 'animal',

  'geo': 'geo',
  '地理': 'geo',
  'geography': 'geo',
  '地域': 'geo',

  'location': 'location',
  '地点': 'location',
  'place': 'location',
  'address': 'location',
  '位置': 'location',
  '地址': 'location',

  'technology': 'technology',
  '技术': 'technology',
  'tech': 'technology',
  '科技': 'technology',

  'equipment': 'equipment',
  '设备': 'equipment',
  'device': 'equipment',
  '装备': 'equipment',

  'weapon': 'weapon',
  '武器': 'weapon',
  'arms': 'weapon',
  '军火': 'weapon',

  'object': 'object',
  '物品': 'object',
  'stuff': 'object',
  '物体': 'object',

  'group': 'group',
  '群组': 'group',
  'community': 'group',
  '社区': 'group',

  'document': 'document',
  'doc': 'document',
  'file': 'document',
  '資料': 'document',
  '文書': 'document',

  'project': 'project',
  'プロジェクト': 'project',

  'policy': 'policy',
  'ポリシー': 'policy',
  'rule': 'policy',

  'system': 'system',
  'service': 'system',
  'サービス': 'system'
};

// 节点类型到颜色的映射
const NODE_TYPE_COLORS: Record<string, string> = {
  'unknown': '#f4d371', // Yellow
  'category': '#e3493b', // GoogleRed
  'organization': '#0f705d', // Green
  'event': '#00bfa0', // Turquoise
  'person': '#4169E1', // RoyalBlue
  'animal': '#84a3e1', // SkyBlue
  'geo': '#ff99cc', // Pale Pink
  'location': '#cf6d17', // Carrot
  'technology': '#b300b3', // Purple
  'equipment': '#2F4F4F', // DarkSlateGray
  'weapon': '#4421af', // DeepPurple
  'object': '#00cc00', // Green
  'group': '#0f558a', // NavyBlue
  'document': '#1d4ed8', // Blue
  'project': '#16a34a', // Green
  'policy': '#7e22ce', // Purple
  'system': '#ea580c' // Orange
};

const EDGE_TYPE_COLORS: Record<string, string> = {
  'resolved_to': '#2563eb',
  'member_of': '#16a34a',
  'contains_topic': '#7c3aed',
  'referenced_by': '#dc2626',
  'contained_in': '#0f766e',
  'folder_parent_of': '#0e7490',
  'similar_to': '#9333ea',
  'mentions_entity': '#d97706',
  'related_to': '#64748b'
};

const hexToRgba = (hex: string, alpha: number): string => {
  const normalized = hex.replace('#', '')
  const value = normalized.length === 3
    ? normalized.split('').map((char) => char + char).join('')
    : normalized
  const intValue = Number.parseInt(value, 16)
  const r = (intValue >> 16) & 255
  const g = (intValue >> 8) & 255
  const b = intValue & 255
  return `rgba(${r}, ${g}, ${b}, ${alpha})`
}

const normalizeNodeType = (node: RawNodeType): string => {
  const rawType = String(node.properties?.entity_type ?? node.labels?.[0] ?? 'unknown').toLowerCase().trim();
  const normalized = TYPE_SYNONYMS[rawType] ?? rawType;
  if (normalized.includes('document') || normalized.includes('.ppt') || normalized.includes('.pdf') || normalized.includes('.doc')) {
    return 'document';
  }
  return normalized;
};

const normalizeResolutionStatus = (node: RawNodeType): 'resolved' | 'review' | 'pending' => {
  const status = String(node.properties?.resolutionStatus ?? node.properties?.resolution_status ?? '').toLowerCase().trim();
  if (status === 'resolved' || status === 'review' || status === 'pending') {
    return status;
  }
  const confidence = Number(node.properties?.confidence ?? node.properties?.match_confidence ?? 1);
  if (!Number.isFinite(confidence) || confidence >= 0.85) return 'resolved';
  if (confidence >= 0.7) return 'review';
  return 'pending';
};

const normalizeEdgeType = (edge: RawEdgeType): string => {
  const rawType = String(
    edge.type ??
    edge.properties?.relation_type ??
    edge.properties?.relationship ??
    edge.properties?.predicate ??
    edge.properties?.keywords ??
    'related_to'
  ).toLowerCase().trim();

  if (rawType.includes('resolve')) return 'resolved_to';
  if (rawType.includes('member')) return 'member_of';
  if (rawType.includes('similar_to') || rawType.includes('similar')) return 'similar_to';
  if (rawType.includes('mentions_entity') || rawType.includes('mention')) return 'mentions_entity';
  if (rawType.includes('folder_parent_of') || (rawType.includes('parent') && rawType.includes('folder'))) return 'folder_parent_of';
  if (rawType.includes('contained_in')) return 'contained_in';
  if (rawType.includes('contain') || rawType.includes('topic')) return 'contains_topic';
  if (rawType.includes('reference')) return 'referenced_by';
  if (rawType.includes('related')) return 'related_to';
  return rawType || 'related_to';
};

const isCenterNodeForQuery = (node: RawNodeType, queryLabel: string): boolean => {
  const normalizedQuery = String(queryLabel ?? '').trim().toLowerCase()
  if (!normalizedQuery || normalizedQuery === '*') return false
  if (String(node.id ?? '').toLowerCase() === normalizedQuery) return true
  if (Array.isArray(node.labels) && node.labels.some((label) => String(label).toLowerCase() === normalizedQuery)) {
    return true
  }
  const fileName = String(node.properties?.file_name ?? '').toLowerCase()
  if (fileName === normalizedQuery) return true
  const itemId = String(node.properties?.item_id ?? '').toLowerCase()
  return itemId === normalizedQuery
}

// Extended colors pool - Used for unknown node types
const EXTENDED_COLORS = [
  '#5a2c6d', // DeepViolet
  '#0000ff', // Blue
  '#cd071e', // ChinaRed
  '#00CED1', // DarkTurquoise
  '#9b3a31', // DarkBrown
  '#b2e061', // YellowGreen
  '#bd7ebe', // LightViolet
  '#6ef7b3', // LightGreen
  '#003366', // DarkBlue
  '#DEB887', // BurlyWood
];

// Select color based on node type
const getNodeColorByType = (nodeType: string | undefined): string => {

  const defaultColor = '#5D6D7E';

  const normalizedType = nodeType ? nodeType.toLowerCase() : 'unknown';
  const typeColorMap = useGraphStore.getState().typeColorMap;

  // Return previous color if already mapped
  if (typeColorMap.has(normalizedType)) {
    return typeColorMap.get(normalizedType) || defaultColor;
  }

  const standardType = TYPE_SYNONYMS[normalizedType];
  if (standardType) {
    const color = NODE_TYPE_COLORS[standardType];
    // Update color mapping
    const newMap = new Map(typeColorMap);
    newMap.set(normalizedType, color);
    useGraphStore.setState({ typeColorMap: newMap });
    return color;
  }

  // For unpredefind nodeTypes, use extended colors
  // Find used extended colors
  const usedExtendedColors = new Set(
    Array.from(typeColorMap.entries())
      .filter(([, color]) => !Object.values(NODE_TYPE_COLORS).includes(color))
      .map(([, color]) => color)
  );

  // Find and use the first unused extended color
  const unusedColor = EXTENDED_COLORS.find(color => !usedExtendedColors.has(color));
  const newColor = unusedColor || defaultColor;

  // Update color mapping
  const newMap = new Map(typeColorMap);
  newMap.set(normalizedType, newColor);
  useGraphStore.setState({ typeColorMap: newMap });

  return newColor;
};

const validateGraph = (graph: RawGraph) => {
  // Check if graph exists
  if (!graph) {
    console.log('Graph validation failed: graph is null');
    return false;
  }

  // Check if nodes and edges are arrays
  if (!Array.isArray(graph.nodes) || !Array.isArray(graph.edges)) {
    console.log('Graph validation failed: nodes or edges is not an array');
    return false;
  }

  // Check if nodes array is empty
  if (graph.nodes.length === 0) {
    console.log('Graph validation failed: nodes array is empty');
    return false;
  }

  // Validate each node
  for (const node of graph.nodes) {
    if (!node.id || !node.labels || !node.properties) {
      console.log('Graph validation failed: invalid node structure');
      return false;
    }
  }

  // Validate each edge
  for (const edge of graph.edges) {
    if (!edge.id || !edge.source || !edge.target) {
      console.log('Graph validation failed: invalid edge structure');
      return false;
    }
  }

  // Validate edge connections
  for (const edge of graph.edges) {
    const source = graph.getNode(edge.source);
    const target = graph.getNode(edge.target);
    if (source == undefined || target == undefined) {
      console.log('Graph validation failed: edge references non-existent node');
      return false;
    }
  }

  console.log('Graph validation passed');
  return true;
}

export type NodeType = {
  x: number
  y: number
  label: string
  size: number
  color: string
  highlighted?: boolean
}
export type EdgeType = {
  label: string
  originalWeight?: number
  size?: number
  color?: string
  hidden?: boolean
}

const fetchGraph = async (
  label: string,
  maxDepth: number,
  maxNodes: number,
  source: 'default' | 'ontology'
) => {
  let rawData: any = null;

  // Check if we need to fetch all database labels first
  const lastSuccessfulQueryLabel = useGraphStore.getState().lastSuccessfulQueryLabel;
  if (!lastSuccessfulQueryLabel) {
    console.log('Last successful queryLabel is empty');
    try {
      const labels = await getGraphLabels(source)
      useGraphStore.getState().setAllDatabaseLabels(labels)
    } catch (e) {
      console.error('Failed to fetch all database labels:', e);
      // Continue with graph fetch even if labels fetch fails
    }
  }

  // Trigger GraphLabels component to check if the label is valid
  // console.log('Setting labelsFetchAttempted to true');
  useGraphStore.getState().setLabelsFetchAttempted(true)

  // If label is empty, use default label '*'
  const queryLabel = label || '*';

  try {
    console.log(`Fetching graph label: ${queryLabel}, depth: ${maxDepth}, nodes: ${maxNodes}`);
    rawData = await queryGraphs(queryLabel, maxDepth, maxNodes, source);
  } catch (e) {
    useBackendState.getState().setErrorMessage(errorMessage(e), 'Query Graphs Error!');
    return null;
  }

  let rawGraph = null;

  if (rawData) {
    const nodeIdMap: Record<string, number> = {}
    const edgeIdMap: Record<string, number> = {}

    let centerNodeId: string | null = null
    for (let i = 0; i < rawData.nodes.length; i++) {
      const node = rawData.nodes[i]
      nodeIdMap[node.id] = i

      node.x = Math.random()
      node.y = Math.random()
      node.degree = 0
      node.size = 10
      node.properties = node.properties ?? {}
      node.properties._normalized_entity_type = normalizeNodeType(node)
      node.properties._resolution_status = normalizeResolutionStatus(node)
      node.properties._is_query_center = isCenterNodeForQuery(node, queryLabel)
      if (node.properties._is_query_center && !centerNodeId) {
        centerNodeId = node.id
      }
    }
    rawData.center_node_id = centerNodeId

    for (let i = 0; i < rawData.edges.length; i++) {
      const edge = rawData.edges[i]
      edgeIdMap[edge.id] = i

      const source = nodeIdMap[edge.source]
      const target = nodeIdMap[edge.target]
      if (source !== undefined && source !== undefined) {
        const sourceNode = rawData.nodes[source]
        const targetNode = rawData.nodes[target]
        if (!sourceNode) {
          console.error(`Source node ${edge.source} is undefined`)
          continue
        }
        if (!targetNode) {
          console.error(`Target node ${edge.target} is undefined`)
          continue
        }
        sourceNode.degree += 1
        targetNode.degree += 1
      }
    }

    // generate node size
    let minDegree = Number.MAX_SAFE_INTEGER
    let maxDegree = 0

    for (const node of rawData.nodes) {
      minDegree = Math.min(minDegree, node.degree)
      maxDegree = Math.max(maxDegree, node.degree)
    }
    const range = maxDegree - minDegree
    if (range > 0) {
      const scale = Constants.maxNodeSize - Constants.minNodeSize
      for (const node of rawData.nodes) {
        node.size = Math.round(
          Constants.minNodeSize + scale * Math.pow((node.degree - minDegree) / range, 0.5)
        )
      }
    }

    rawGraph = new RawGraph()
    rawGraph.nodes = rawData.nodes
    rawGraph.edges = rawData.edges
    rawGraph.nodeIdMap = nodeIdMap
    rawGraph.edgeIdMap = edgeIdMap

    if (!validateGraph(rawGraph)) {
      rawGraph = null
      console.warn('Invalid graph data')
    }
    console.log('Graph data loaded')
  }

  // console.debug({ data: JSON.parse(JSON.stringify(rawData)) })
  return {
    rawGraph,
    is_truncated: rawData.is_truncated,
    center_node_id: rawData?.center_node_id ?? null,
    projection_status: rawData?.projection_status
  }
}

const applyOntologyConnectivityPresentation = (
  graph: UndirectedGraph,
  rawGraph: RawGraph,
  focusNodeId?: string | null
) => {
  const queryCenterNodeId =
    rawGraph.nodes.find((node) => Boolean(node.properties?._is_query_center))?.id ?? null
  const centerNodeId =
    focusNodeId && graph.hasNode(focusNodeId) ? focusNodeId : queryCenterNodeId
  const directRelatedNodeIds = new Set<string>()
  if (centerNodeId && graph.hasNode(centerNodeId)) {
    directRelatedNodeIds.add(centerNodeId)
    for (const neighborId of graph.neighbors(centerNodeId)) {
      directRelatedNodeIds.add(neighborId)
    }
  }

  graph.forEachNode((nodeId) => {
    const rawNode = rawGraph.getNode(nodeId)
    if (!rawNode) return

    const hasAnyConnection = graph.degree(nodeId) > 0
    const isDirectlyRelatedToCenter = centerNodeId !== null && directRelatedNodeIds.has(nodeId)
    const shouldShowIconAndLabel = centerNodeId !== null ? isDirectlyRelatedToCenter : hasAnyConnection
    const isQueryCenter = Boolean(rawNode.properties?._is_query_center)
    const normalizedType = String(
      rawNode.properties?._normalized_entity_type ?? rawNode.properties?.entity_type ?? 'unknown'
    ).toLowerCase()
    const baseSize = Number(rawNode.size ?? Constants.minNodeSize)

    if (shouldShowIconAndLabel) {
      const connectedSize = isQueryCenter
        ? Math.max(baseSize + 4, Constants.maxNodeSize + 8)
        : baseSize + 1
      graph.setNodeAttribute(nodeId, 'hidden', false)
      graph.setNodeAttribute(nodeId, 'color', 'rgba(0, 0, 0, 0)')
      graph.setNodeAttribute(nodeId, 'borderColor', 'rgba(0, 0, 0, 0)')
      graph.setNodeAttribute(nodeId, 'borderSize', 0)
      graph.setNodeAttribute(nodeId, 'size', connectedSize)
      graph.setNodeAttribute(nodeId, 'isPeripheral', false)
      graph.setNodeAttribute(nodeId, 'iconType', normalizedType)
      graph.setNodeAttribute(nodeId, 'isCenterNode', isQueryCenter)
    } else {
      graph.setNodeAttribute(nodeId, 'hidden', true)
    }
  })

  graph.forEachEdge((edgeId, attrs, source, target) => {
    const sourceHidden = graph.getNodeAttribute(source, 'hidden')
    const targetHidden = graph.getNodeAttribute(target, 'hidden')
    graph.setEdgeAttribute(edgeId, 'hidden', Boolean(sourceHidden || targetHidden))
  })
}

// Create a new graph instance with the raw graph data
const createSigmaGraph = (rawGraph: RawGraph | null, source: 'default' | 'ontology' = 'default') => {
  // Get edge size settings from store
  const minEdgeSize = useSettingsStore.getState().minEdgeSize
  const maxEdgeSize = useSettingsStore.getState().maxEdgeSize
  // Skip graph creation if no data or empty nodes
  if (!rawGraph || !rawGraph.nodes.length) {
    console.log('No graph data available, skipping sigma graph creation');
    return null;
  }

  // Create new graph instance
  const graph = new UndirectedGraph()
  const centerNodeId =
    rawGraph.nodes.find((node) => Boolean(node.properties?._is_query_center))?.id ?? null
  const directRelatedNodeIds = new Set<string>()
  if (source === 'ontology' && centerNodeId) {
    directRelatedNodeIds.add(centerNodeId)
    for (const edge of rawGraph.edges) {
      if (edge.source === centerNodeId) directRelatedNodeIds.add(edge.target)
      if (edge.target === centerNodeId) directRelatedNodeIds.add(edge.source)
    }
  }

  // Add nodes from raw graph data
  for (const rawNode of rawGraph?.nodes ?? []) {
    // Ensure we have fresh random positions for nodes
    seedrandom(rawNode.id + Date.now().toString(), { global: true })
    const x = Math.random()
    const y = Math.random()

    const isQueryCenter = Boolean(rawNode.properties?._is_query_center)
    const isPeripheralOntologyNode =
      source === 'ontology' && centerNodeId !== null && !directRelatedNodeIds.has(rawNode.id)
    const baseNodeColor = String(rawNode.color ?? '#5D6D7E')
    const borderAccent = Constants.nodeBorderColor
    const normalizedType = String(rawNode.properties?._normalized_entity_type ?? rawNode.properties?.entity_type ?? 'unknown').toLowerCase()
    const nodeColor = source === 'ontology' ? hexToRgba(baseNodeColor, isQueryCenter ? 0.72 : 0.48) : baseNodeColor
    graph.addNode(rawNode.id, {
      label: rawNode.labels.join(', '),
      color:
        source === 'ontology'
          ? (isPeripheralOntologyNode ? 'rgba(148, 163, 184, 0.32)' : 'rgba(0, 0, 0, 0)')
          : nodeColor,
      x: x,
      y: y,
      size:
        source === 'ontology' && isPeripheralOntologyNode
          ? Math.max(3, Math.round((rawNode.size + 1) * 0.36))
          : (isQueryCenter ? Math.max(rawNode.size + 4, Constants.maxNodeSize + 8) : rawNode.size + 1),
      entityType: normalizedType,
      iconType: normalizedType,
      isCenterNode: isQueryCenter,
      isPeripheral: isPeripheralOntologyNode,
      // for node-border
      borderColor: source === 'ontology' ? 'rgba(0, 0, 0, 0)' : (
        isQueryCenter
          ? '#0ea5e9'
          : rawNode.properties?._resolution_status === 'pending'
            ? '#dc2626'
            : rawNode.properties?._resolution_status === 'review'
              ? '#d97706'
              : borderAccent
      ),
      borderSize: source === 'ontology' ? 0 : (isQueryCenter ? 1.6 : 0.9)
    })
  }

  // Add edges from raw graph data
  let skippedDuplicateEdges = 0
  for (const rawEdge of rawGraph?.edges ?? []) {
    // Graphology UndirectedGraph does not allow multiple edges between the same node pair.
    // Ontology projection can include parallel relations (e.g., mentions + contained_in),
    // so we skip duplicates for visualization stability.
    if (graph.hasEdge(rawEdge.source, rawEdge.target)) {
      skippedDuplicateEdges += 1
      continue
    }

    // Get weight from edge properties or default to 1
    const weight = rawEdge.properties?.weight !== undefined ? Number(rawEdge.properties.weight) : 1

    const normalizedEdgeType = normalizeEdgeType(rawEdge)
    rawEdge.type = normalizedEdgeType
    rawEdge.properties = rawEdge.properties ?? {}
    rawEdge.properties._normalized_edge_type = normalizedEdgeType

    rawEdge.dynamicId = graph.addEdge(rawEdge.source, rawEdge.target, {
      label: rawEdge.properties?.keywords || undefined,
      size: weight, // Set initial size based on weight
      originalWeight: weight, // Store original weight for recalculation
      color:
        source === 'ontology'
          ? hexToRgba(EDGE_TYPE_COLORS[normalizedEdgeType] ?? EDGE_TYPE_COLORS.related_to, 0.28)
          : EDGE_TYPE_COLORS[normalizedEdgeType] ?? EDGE_TYPE_COLORS.related_to,
      type: 'curvedNoArrow' // Explicitly set edge type to no arrow
    })
  }
  if (skippedDuplicateEdges > 0) {
    console.warn(`Skipped duplicate ontology edges for rendering: ${skippedDuplicateEdges}`)
  }

  // Calculate edge size based on weight range, similar to node size calculation
  let minWeight = Number.MAX_SAFE_INTEGER
  let maxWeight = 0

  // Find min and max weight values
  graph.forEachEdge(edge => {
    const weight = graph.getEdgeAttribute(edge, 'originalWeight') || 1
    minWeight = Math.min(minWeight, weight)
    maxWeight = Math.max(maxWeight, weight)
  })

  // Scale edge sizes based on weight range
  const weightRange = maxWeight - minWeight
  if (weightRange > 0) {
    const sizeScale = maxEdgeSize - minEdgeSize
    graph.forEachEdge(edge => {
      const weight = graph.getEdgeAttribute(edge, 'originalWeight') || 1
      const scaledSize = minEdgeSize + sizeScale * Math.pow((weight - minWeight) / weightRange, 0.5)
      graph.setEdgeAttribute(edge, 'size', scaledSize)
    })
  } else {
    // If all weights are the same, use default size
    graph.forEachEdge(edge => {
      graph.setEdgeAttribute(edge, 'size', minEdgeSize)
    })
  }

  if (source === 'ontology') {
    applyOntologyConnectivityPresentation(graph, rawGraph)
  }

  return graph
}

type UseGraphsuiteGraphOptions = {
  queryLabelOverride?: string
  maxDepthOverride?: number
  maxNodesOverride?: number
}

const useGraphsuiteGraph = (
  source: 'default' | 'ontology' = 'ontology',
  options: UseGraphsuiteGraphOptions = {}
) => {
  const { t } = useTranslation()
  const settingsQueryLabel = useSettingsStore.use.queryLabel()
  const rawGraph = useGraphStore.use.rawGraph()
  const sigmaGraph = useGraphStore.use.sigmaGraph()
  const settingsMaxQueryDepth = useSettingsStore.use.graphQueryMaxDepth()
  const settingsMaxNodes = useSettingsStore.use.graphMaxNodes()
  const isFetching = useGraphStore.use.isFetching()
  const graphDataVersion = useGraphStore.use.graphDataVersion()
  const nodeToExpand = useGraphStore.use.nodeToExpand()
  const nodeToPrune = useGraphStore.use.nodeToPrune()
  const queryLabel = options.queryLabelOverride ?? settingsQueryLabel
  const maxQueryDepth = options.maxDepthOverride ?? settingsMaxQueryDepth
  const maxNodes = options.maxNodesOverride ?? settingsMaxNodes


  // Use ref to track if data has been loaded and initial load
  const dataLoadedRef = useRef(false)
  const initialLoadRef = useRef(false)
  // Use ref to track if empty data has been handled
  const emptyDataHandledRef = useRef(false)
  const lastQueryContextRef = useRef<string>('')
  const ontologyProjectionHintKeyRef = useRef<string>('')

  const getNode = useCallback(
    (nodeId: string) => {
      return rawGraph?.getNode(nodeId) || null
    },
    [rawGraph]
  )

  const getEdge = useCallback(
    (edgeId: string, dynamicId: boolean = true) => {
      return rawGraph?.getEdge(edgeId, dynamicId) || null
    },
    [rawGraph]
  )

  // Track if a fetch is in progress to prevent multiple simultaneous fetches
  const fetchInProgressRef = useRef(false)

  // Reset graph when query label is cleared
  useEffect(() => {
    if (!queryLabel && (rawGraph !== null || sigmaGraph !== null)) {
      const state = useGraphStore.getState()
      state.reset()
      state.setGraphDataFetchAttempted(false)
      state.setLabelsFetchAttempted(false)
      dataLoadedRef.current = false
      initialLoadRef.current = false
    }
  }, [queryLabel, rawGraph, sigmaGraph])

  // Force a fresh fetch when effective query conditions change (especially embedded override mode).
  useEffect(() => {
    const queryContext = `${source}|${queryLabel}|${maxQueryDepth}|${maxNodes}|${graphDataVersion}`
    if (lastQueryContextRef.current === queryContext) return
    lastQueryContextRef.current = queryContext
    emptyDataHandledRef.current = false
    fetchInProgressRef.current = false
    useGraphStore.getState().setGraphDataFetchAttempted(false)
  }, [source, queryLabel, maxQueryDepth, maxNodes, graphDataVersion])

  // Graph data fetching logic
  useEffect(() => {
    // Skip if fetch is already in progress
    if (fetchInProgressRef.current) {
      return
    }

    // Empty queryLabel should be only handle once(avoid infinite loop)
    if (!queryLabel && emptyDataHandledRef.current) {
      return;
    }

    // Only fetch data when graphDataFetchAttempted is false (avoids re-fetching on vite dev mode)
    // GraphDataFetchAttempted must set to false when queryLabel is changed
    if (!isFetching && !useGraphStore.getState().graphDataFetchAttempted) {
      // Set flags
      fetchInProgressRef.current = true
      useGraphStore.getState().setGraphDataFetchAttempted(true)

      const state = useGraphStore.getState()
      state.setIsFetching(true)

      // Clear selection and highlighted nodes before fetching new graph
      state.clearSelection()
      if (state.sigmaGraph) {
        state.sigmaGraph.forEachNode((node) => {
          state.sigmaGraph?.setNodeAttribute(node, 'highlighted', false)
        })
      }

      console.log('Preparing graph data...')

      // Use a local copy of the parameters
      const currentQueryLabel = queryLabel
      const currentMaxQueryDepth = maxQueryDepth
      const currentMaxNodes = maxNodes

      // Declare a variable to store data promise
      let dataPromise: Promise<{
        rawGraph: RawGraph | null
        is_truncated: boolean | undefined
        center_node_id?: string | null
        projection_status?: {
          state?: string
          message?: string
          unified_total_count?: number
          error?: string
          ontology_graph_sqlite_path?: string | null
          multi_instance_note?: string
          projection_storage?: string
        }
      } | null>;

      // 1. If query label is not empty, use fetchGraph
      if (currentQueryLabel) {
        dataPromise = fetchGraph(currentQueryLabel, currentMaxQueryDepth, currentMaxNodes, source);
      } else {
        // 2. If query label is empty, set data to null
        console.log('Query label is empty, show empty graph')
        dataPromise = Promise.resolve({ rawGraph: null, is_truncated: false, center_node_id: null });
      }

      // 3. Process data
      dataPromise.then((result) => {
        const state = useGraphStore.getState()
        const data = result?.rawGraph;

        // Assign colors based on entity_type *after* fetching
        if (data && data.nodes) {
          data.nodes.forEach(node => {
            const nodeEntityType = String(node.properties?._normalized_entity_type ?? node.properties?.entity_type ?? 'unknown');
            node.color = getNodeColorByType(nodeEntityType);
          });
        }

        if (result?.is_truncated) {
          toast.info(t('graphPanel.dataIsTruncated', 'Graph data is truncated to Max Nodes'));
        }

        // Reset state
        state.reset()

        // Check if data is empty or invalid
        if (!data || !data.nodes || data.nodes.length === 0) {
          if (source === 'ontology') {
            const projectionState = String(result?.projection_status?.state ?? '')
            const projectionHintKey = `${currentQueryLabel}:${projectionState}:${String(result?.projection_status?.message ?? '')}`
            const projectionMessage = result?.projection_status?.message
            const failureDetail = result?.projection_status?.error
            const shouldShowProjectionHint =
              projectionState === 'projection_stale_or_empty' ||
              projectionState === 'projection_refresh_failed'
            if (
              shouldShowProjectionHint &&
              ontologyProjectionHintKeyRef.current !== projectionHintKey
            ) {
              ontologyProjectionHintKeyRef.current = projectionHintKey
              const baseMessage =
                projectionMessage ??
                'UnifiedMetadata にデータがあります。projection refresh を実行してください。'
              const jaAction = t(
                'graphPanel.ontologyProjectionStaleHint',
                '対処: オントロジー画面の「オントロジーグラフを更新」を実行するか、POST /ontology/graph/projection/refresh で投影を再構築してください。'
              )
              const multiNote = result?.projection_status?.multi_instance_note
                ? `\n${String(result.projection_status.multi_instance_note)}`
                : ''
              const warningMessage =
                projectionState === 'projection_refresh_failed' && failureDetail
                  ? `${baseMessage}\n${failureDetail}\n${jaAction}${multiNote}`
                  : `${baseMessage}\n${jaAction}${multiNote}`
              toast.warning(warningMessage, { duration: 10000 })
            }
          }
          // Create a graph with a single "Graph Is Empty" node
          const emptyGraph = new UndirectedGraph();

          // Add a single node with "Graph Is Empty" label
          emptyGraph.addNode('empty-graph-node', {
            label: t('graphPanel.emptyGraph'),
            color: '#5D6D7E', // gray color
            x: 0.5,
            y: 0.5,
            size: 15,
            borderColor: Constants.nodeBorderColor,
            borderSize: 0.2
          });

          // Set graph to store
          state.setSigmaGraph(emptyGraph);
          state.setRawGraph(null);

          // Still mark graph as empty for other logic
          state.setGraphIsEmpty(true);

          // Check if the empty graph is due to 401 authentication error
          const errorMessage = useBackendState.getState().message;
          const isAuthError = errorMessage && errorMessage.includes('Authentication required');

          // Only clear queryLabel if it's not an auth error and current label is not empty
          if (
            !isAuthError &&
            source !== 'ontology' &&
            currentQueryLabel &&
            options.queryLabelOverride === undefined
          ) {
            useSettingsStore.getState().setQueryLabel('');
          }

          // Only clear last successful query label if it's not an auth error
          if (!isAuthError) {
            state.setLastSuccessfulQueryLabel('');
          } else {
            console.log('Keep queryLabel for post-login reload');
          }

          console.log(`Graph data is empty, created graph with empty graph node. Auth error: ${isAuthError}`);
        } else {
          if (source === 'ontology') {
            ontologyProjectionHintKeyRef.current = ''
          }
          // Create and set new graph
          const newSigmaGraph = createSigmaGraph(data, source);
          data.buildDynamicMap();

          // Set new graph data
          state.setSigmaGraph(newSigmaGraph);
          state.setRawGraph(data);
          state.setGraphIsEmpty(false);

          // Update last successful query label
          state.setLastSuccessfulQueryLabel(currentQueryLabel);

          // Reset camera view
          state.setMoveToSelectedNode(true);
          const centerNodeId =
            result?.center_node_id ??
            data.nodes.find((node) => Boolean(node.properties?._is_query_center))?.id ??
            null
          if (centerNodeId) {
            state.setSelectedNode(centerNodeId, true)
          }
        }

        // Update flags
        dataLoadedRef.current = true
        initialLoadRef.current = true
        fetchInProgressRef.current = false
        state.setIsFetching(false)

        // Mark empty data as handled if data is empty and query label is empty
        if ((!data || !data.nodes || data.nodes.length === 0) && !currentQueryLabel) {
          emptyDataHandledRef.current = true;
        }
      }).catch((error) => {
        console.error('Error fetching graph data:', error)

        // Reset state on error
        const state = useGraphStore.getState()
        state.setIsFetching(false)
        dataLoadedRef.current = false;
        fetchInProgressRef.current = false
        state.setGraphDataFetchAttempted(false)
        state.setLastSuccessfulQueryLabel('') // Clear last successful query label on error
      })
    }
  }, [queryLabel, maxQueryDepth, maxNodes, isFetching, t, source, options.queryLabelOverride, graphDataVersion])

  // Handle node expansion
  useEffect(() => {
    const handleNodeExpand = async (nodeId: string | null) => {
      if (!nodeId || !sigmaGraph || !rawGraph) return;

      try {
        // Get the node to expand
        const nodeToExpand = rawGraph.getNode(nodeId);
        if (!nodeToExpand) {
          console.error('Node not found:', nodeId);
          return;
        }

        // Get the label of the node to expand
        const label = nodeToExpand.labels[0];
        if (!label) {
          console.error('Node has no label:', nodeId);
          return;
        }

        // Fetch the extended subgraph with depth 2
        const extendedGraph = await queryGraphs(label, 2, 1000, source);

        if (!extendedGraph || !extendedGraph.nodes || !extendedGraph.edges) {
          console.error('Failed to fetch extended graph');
          return;
        }

        // Process nodes to add required properties for RawNodeType
        const processedNodes: RawNodeType[] = [];
        for (const node of extendedGraph.nodes) {
          // Generate random color values
          seedrandom(node.id, { global: true });
          node.properties = node.properties ?? {}
          node.properties._normalized_entity_type = normalizeNodeType(node);
          node.properties._resolution_status = normalizeResolutionStatus(node);
          const nodeEntityType = String(node.properties?._normalized_entity_type ?? node.properties?.entity_type ?? 'unknown');
          const color = getNodeColorByType(nodeEntityType);

          // Create a properly typed RawNodeType
          processedNodes.push({
            id: node.id,
            labels: node.labels,
            properties: node.properties,
            size: 10, // Default size, will be calculated later
            x: Math.random(), // Random position, will be adjusted later
            y: Math.random(), // Random position, will be adjusted later
            color: color, // Random color
            degree: 0 // Initial degree, will be calculated later
          });
        }

        // Process edges to add required properties for RawEdgeType
        const processedEdges: RawEdgeType[] = [];
        for (const edge of extendedGraph.edges) {
          const normalizedEdgeType = normalizeEdgeType(edge as RawEdgeType);
          edge.properties = edge.properties ?? {};
          edge.properties._normalized_edge_type = normalizedEdgeType;
          // Create a properly typed RawEdgeType
          processedEdges.push({
            id: edge.id,
            source: edge.source,
            target: edge.target,
            type: normalizedEdgeType,
            properties: edge.properties,
            dynamicId: '' // Will be set when adding to sigma graph
          });
        }

        // Store current node positions
        const nodePositions: Record<string, {x: number, y: number}> = {};
        sigmaGraph.forEachNode((node) => {
          nodePositions[node] = {
            x: sigmaGraph.getNodeAttribute(node, 'x'),
            y: sigmaGraph.getNodeAttribute(node, 'y')
          };
        });

        // Get existing node IDs
        const existingNodeIds = new Set(sigmaGraph.nodes());

        // Identify nodes and edges to keep
        const nodesToAdd = new Set<string>();
        const edgesToAdd = new Set<string>();

        // Get degree maxDegree from existing graph for size calculations
        const minDegree = 1;
        let maxDegree = 0;

        // Initialize edge weight min and max values
        let minWeight = Number.MAX_SAFE_INTEGER;
        let maxWeight = 0;

        // Calculate node degrees and edge weights from existing graph
        sigmaGraph.forEachNode(node => {
          const degree = sigmaGraph.degree(node);
          maxDegree = Math.max(maxDegree, degree);
        });

        // Calculate edge weights from existing graph
        sigmaGraph.forEachEdge(edge => {
          const weight = sigmaGraph.getEdgeAttribute(edge, 'originalWeight') || 1;
          minWeight = Math.min(minWeight, weight);
          maxWeight = Math.max(maxWeight, weight);
        });

        // First identify connectable nodes (nodes connected to the expanded node)
        for (const node of processedNodes) {
          // Skip if node already exists
          if (existingNodeIds.has(node.id)) {
            continue;
          }

          // Check if this node is connected to the selected node
          const isConnected = processedEdges.some(
            edge => (edge.source === nodeId && edge.target === node.id) ||
                   (edge.target === nodeId && edge.source === node.id)
          );

          if (isConnected) {
            nodesToAdd.add(node.id);
          }
        }

        // Calculate node degrees and track discarded edges in one pass
        const nodeDegrees = new Map<string, number>();
        const existingNodeDegreeIncrements = new Map<string, number>(); // Track degree increments for existing nodes
        const nodesWithDiscardedEdges = new Set<string>();

        for (const edge of processedEdges) {
          const sourceExists = existingNodeIds.has(edge.source) || nodesToAdd.has(edge.source);
          const targetExists = existingNodeIds.has(edge.target) || nodesToAdd.has(edge.target);

          if (sourceExists && targetExists) {
            edgesToAdd.add(edge.id);
            // Add degrees for both new and existing nodes
            if (nodesToAdd.has(edge.source)) {
              nodeDegrees.set(edge.source, (nodeDegrees.get(edge.source) || 0) + 1);
            } else if (existingNodeIds.has(edge.source)) {
              // Track degree increments for existing nodes
              existingNodeDegreeIncrements.set(edge.source, (existingNodeDegreeIncrements.get(edge.source) || 0) + 1);
            }

            if (nodesToAdd.has(edge.target)) {
              nodeDegrees.set(edge.target, (nodeDegrees.get(edge.target) || 0) + 1);
            } else if (existingNodeIds.has(edge.target)) {
              // Track degree increments for existing nodes
              existingNodeDegreeIncrements.set(edge.target, (existingNodeDegreeIncrements.get(edge.target) || 0) + 1);
            }
          } else {
            // Track discarded edges for both new and existing nodes
            if (sigmaGraph.hasNode(edge.source)) {
              nodesWithDiscardedEdges.add(edge.source);
            } else if (nodesToAdd.has(edge.source)) {
              nodesWithDiscardedEdges.add(edge.source);
              nodeDegrees.set(edge.source, (nodeDegrees.get(edge.source) || 0) + 1); // +1 for discarded edge
            }
            if (sigmaGraph.hasNode(edge.target)) {
              nodesWithDiscardedEdges.add(edge.target);
            } else if (nodesToAdd.has(edge.target)) {
              nodesWithDiscardedEdges.add(edge.target);
              nodeDegrees.set(edge.target, (nodeDegrees.get(edge.target) || 0) + 1); // +1 for discarded edge
            }
          }
        }

        // Helper function to update node sizes
        const updateNodeSizes = (
          sigmaGraph: UndirectedGraph,
          nodesWithDiscardedEdges: Set<string>,
          minDegree: number,
          maxDegree: number
        ) => {
          // Calculate derived values inside the function
          const range = maxDegree - minDegree || 1; // Avoid division by zero
          const scale = Constants.maxNodeSize - Constants.minNodeSize;

          // Update node sizes
          for (const nodeId of nodesWithDiscardedEdges) {
            if (sigmaGraph.hasNode(nodeId)) {
              let newDegree = sigmaGraph.degree(nodeId);
              newDegree += 1; // Add +1 for discarded edges
              // Limit newDegree to maxDegree + 1 to prevent nodes from being too large
              const limitedDegree = Math.min(newDegree, maxDegree + 1);

              const newSize = Math.round(
                Constants.minNodeSize + scale * Math.pow((limitedDegree - minDegree) / range, 0.5)
              );

              sigmaGraph.setNodeAttribute(nodeId, 'size', newSize);
            }
          }
        };

        // Helper function to update edge sizes
        const updateEdgeSizes = (
          sigmaGraph: UndirectedGraph,
          minWeight: number,
          maxWeight: number
        ) => {
          // Update edge sizes
          const minEdgeSize = useSettingsStore.getState().minEdgeSize;
          const maxEdgeSize = useSettingsStore.getState().maxEdgeSize;
          const weightRange = maxWeight - minWeight || 1; // Avoid division by zero
          const sizeScale = maxEdgeSize - minEdgeSize;

          sigmaGraph.forEachEdge(edge => {
            const weight = sigmaGraph.getEdgeAttribute(edge, 'originalWeight') || 1;
            const scaledSize = minEdgeSize + sizeScale * Math.pow((weight - minWeight) / weightRange, 0.5);
            sigmaGraph.setEdgeAttribute(edge, 'size', scaledSize);
          });
        };

        // If no new connectable nodes found, show toast and return
        if (nodesToAdd.size === 0) {
          updateNodeSizes(sigmaGraph, nodesWithDiscardedEdges, minDegree, maxDegree);
          toast.info(t('graphPanel.propertiesView.node.noNewNodes'));
          return;
        }

        // Update maxDegree considering all nodes (both new and existing)
        // 1. Consider degrees of new nodes
        for (const [, degree] of nodeDegrees.entries()) {
          maxDegree = Math.max(maxDegree, degree);
        }

        // 2. Consider degree increments for existing nodes
        for (const [nodeId, increment] of existingNodeDegreeIncrements.entries()) {
          const currentDegree = sigmaGraph.degree(nodeId);
          const projectedDegree = currentDegree + increment;
          maxDegree = Math.max(maxDegree, projectedDegree);
        }

        const range = maxDegree - minDegree || 1; // Avoid division by zero
        const scale = Constants.maxNodeSize - Constants.minNodeSize;

        // SAdd nodes and edges to the graph
        // Calculate camera ratio and spread factor once before the loop
        const cameraRatio = useGraphStore.getState().sigmaInstance?.getCamera().ratio || 1;
        const spreadFactor = Math.max(
          Math.sqrt(nodeToExpand.size) * 4, // Base on node size
          Math.sqrt(nodesToAdd.size) * 3 // Scale with number of nodes
        ) / cameraRatio; // Adjust for zoom level
        seedrandom(Date.now().toString(), { global: true });
        const randomAngle = Math.random() * 2 * Math.PI

        console.log('nodeSize:', nodeToExpand.size, 'nodesToAdd:', nodesToAdd.size);
        console.log('cameraRatio:', Math.round(cameraRatio*100)/100, 'spreadFactor:', Math.round(spreadFactor*100)/100);

        // Add new nodes
        for (const nodeId of nodesToAdd) {
          const newNode = processedNodes.find(n => n.id === nodeId)!;
          const nodeDegree = nodeDegrees.get(nodeId) || 0;

          // Calculate node size
          // Limit nodeDegree to maxDegree + 1 to prevent new nodes from being too large
          const limitedDegree = Math.min(nodeDegree, maxDegree + 1);
          const nodeSize = Math.round(
            Constants.minNodeSize + scale * Math.pow((limitedDegree - minDegree) / range, 0.5)
          );

          // Calculate angle for polar coordinates
          const angle = 2 * Math.PI * (Array.from(nodesToAdd).indexOf(nodeId) / nodesToAdd.size);

          // Calculate final position
          const x = nodePositions[nodeId]?.x ||
                    (nodePositions[nodeToExpand.id].x + Math.cos(randomAngle + angle) * spreadFactor);
          const y = nodePositions[nodeId]?.y ||
                    (nodePositions[nodeToExpand.id].y + Math.sin(randomAngle + angle) * spreadFactor);

          // Add the new node to the sigma graph with calculated position
          const newNodeType = String(newNode.properties?._normalized_entity_type ?? newNode.properties?.entity_type ?? 'unknown').toLowerCase()
          sigmaGraph.addNode(nodeId, {
            label: newNode.labels.join(', '),
            color:
              source === 'ontology'
                ? 'rgba(0, 0, 0, 0)'
                : newNode.color,
            x: x,
            y: y,
            size: nodeSize,
            entityType: newNodeType,
            iconType: newNodeType,
            isCenterNode: false,
            isPeripheral: false,
            borderColor:
              source === 'ontology'
                ? 'rgba(0, 0, 0, 0)'
                : Constants.nodeBorderColor,
            borderSize: source === 'ontology' ? 0 : 0.2
          });

          // Add the node to the raw graph
          if (!rawGraph.getNode(nodeId)) {
            // Update node properties
            newNode.size = nodeSize;
            newNode.x = x;
            newNode.y = y;
            newNode.degree = nodeDegree;

            // Add to nodes array
            rawGraph.nodes.push(newNode);
            // Update nodeIdMap
            rawGraph.nodeIdMap[nodeId] = rawGraph.nodes.length - 1;
          }
        }

        // Add new edges
        for (const edgeId of edgesToAdd) {
          const newEdge = processedEdges.find(e => e.id === edgeId)!;

          // Skip if edge already exists
          if (sigmaGraph.hasEdge(newEdge.source, newEdge.target)) {
            continue;
          }

          // Get weight from edge properties or default to 1
          const weight = newEdge.properties?.weight !== undefined ? Number(newEdge.properties.weight) : 1;

          // Update min and max weight values
          minWeight = Math.min(minWeight, weight);
          maxWeight = Math.max(maxWeight, weight);

          // Add the edge to the sigma graph
          newEdge.dynamicId = sigmaGraph.addEdge(newEdge.source, newEdge.target, {
            label: newEdge.properties?.keywords || undefined,
            size: weight, // Set initial size based on weight
            originalWeight: weight, // Store original weight for recalculation
            color: EDGE_TYPE_COLORS[newEdge.type ?? 'related_to'] ?? EDGE_TYPE_COLORS.related_to,
            type: 'curvedNoArrow' // Explicitly set edge type to no arrow
          });

          // Add the edge to the raw graph
          if (!rawGraph.getEdge(newEdge.id, false)) {
            // Add to edges array
            rawGraph.edges.push(newEdge);
            // Update edgeIdMap
            rawGraph.edgeIdMap[newEdge.id] = rawGraph.edges.length - 1;
            // Update dynamic edge map
            rawGraph.edgeDynamicIdMap[newEdge.dynamicId] = rawGraph.edges.length - 1;
          } else {
            console.error('Edge already exists in rawGraph:', newEdge.id);
          }
        }

        // Update the dynamic edge map and invalidate search cache
        rawGraph.buildDynamicMap();

        // Reset search engine to force rebuild
        useGraphStore.getState().resetSearchEngine();

        // Update sizes for all nodes and edges
        updateNodeSizes(sigmaGraph, nodesWithDiscardedEdges, minDegree, maxDegree);
        updateEdgeSizes(sigmaGraph, minWeight, maxWeight);

        // Final update for the expanded node
        if (sigmaGraph.hasNode(nodeId)) {
          const finalDegree = sigmaGraph.degree(nodeId);
          const limitedDegree = Math.min(finalDegree, maxDegree + 1);
          const newSize = Math.round(
            Constants.minNodeSize + scale * Math.pow((limitedDegree - minDegree) / range, 0.5)
          );
          sigmaGraph.setNodeAttribute(nodeId, 'size', newSize);
          nodeToExpand.size = newSize;
          nodeToExpand.degree = finalDegree;
        }

        if (source === 'ontology') {
          applyOntologyConnectivityPresentation(
            sigmaGraph,
            rawGraph,
            useGraphStore.getState().selectedNode
          )
        }

      } catch (error) {
        console.error('Error expanding node:', error);
      }
    };

    // If there's a node to expand, handle it
    if (nodeToExpand) {
      handleNodeExpand(nodeToExpand);
      // Reset the nodeToExpand state after handling
      window.setTimeout(() => {
        useGraphStore.getState().triggerNodeExpand(null);
      }, 0);
    }
  }, [nodeToExpand, sigmaGraph, rawGraph, t, source]);

  // Helper function to get all nodes that will be deleted
  const getNodesThatWillBeDeleted = useCallback((nodeId: string, graph: UndirectedGraph) => {
    const nodesToDelete = new Set<string>([nodeId]);

    // Find all nodes that would become isolated after deletion
    graph.forEachNode((node) => {
      if (node === nodeId) return; // Skip the node being deleted

      // Get all neighbors of this node
      const neighbors = graph.neighbors(node);

      // If this node has only one neighbor and that neighbor is the node being deleted,
      // this node will become isolated, so we should delete it too
      if (neighbors.length === 1 && neighbors[0] === nodeId) {
        nodesToDelete.add(node);
      }
    });

    return nodesToDelete;
  }, []);

  // Handle node pruning
  useEffect(() => {
    const handleNodePrune = (nodeId: string | null) => {
      if (!nodeId || !sigmaGraph || !rawGraph) return;

      try {
        const state = useGraphStore.getState();

        // 1. Check if node exists
        if (!sigmaGraph.hasNode(nodeId)) {
          console.error('Node not found:', nodeId);
          return;
        }

        // 2. Get nodes to delete
        const nodesToDelete = getNodesThatWillBeDeleted(nodeId, sigmaGraph);

        // 3. Check if this would delete all nodes
        if (nodesToDelete.size === sigmaGraph.nodes().length) {
          toast.error(t('graphPanel.propertiesView.node.deleteAllNodesError'));
          return;
        }

        // 4. Clear selection - this will cause PropertiesView to close immediately
        state.clearSelection();

        // 5. Delete nodes and related edges
        for (const nodeToDelete of nodesToDelete) {
          // Remove the node from the sigma graph (this will also remove connected edges)
          sigmaGraph.dropNode(nodeToDelete);

          // Remove the node from the raw graph
          const nodeIndex = rawGraph.nodeIdMap[nodeToDelete];
          if (nodeIndex !== undefined) {
            // Find all edges connected to this node
            const edgesToRemove = rawGraph.edges.filter(
              edge => edge.source === nodeToDelete || edge.target === nodeToDelete
            );

            // Remove edges from raw graph
            for (const edge of edgesToRemove) {
              const edgeIndex = rawGraph.edgeIdMap[edge.id];
              if (edgeIndex !== undefined) {
                // Remove from edges array
                rawGraph.edges.splice(edgeIndex, 1);
                // Update edgeIdMap for all edges after this one
                for (const [id, idx] of Object.entries(rawGraph.edgeIdMap)) {
                  if (idx > edgeIndex) {
                    rawGraph.edgeIdMap[id] = idx - 1;
                  }
                }
                // Remove from edgeIdMap
                delete rawGraph.edgeIdMap[edge.id];
                // Remove from edgeDynamicIdMap
                delete rawGraph.edgeDynamicIdMap[edge.dynamicId];
              }
            }

            // Remove node from nodes array
            rawGraph.nodes.splice(nodeIndex, 1);

            // Update nodeIdMap for all nodes after this one
            for (const [id, idx] of Object.entries(rawGraph.nodeIdMap)) {
              if (idx > nodeIndex) {
                rawGraph.nodeIdMap[id] = idx - 1;
              }
            }

            // Remove from nodeIdMap
            delete rawGraph.nodeIdMap[nodeToDelete];
          }
        }

        // Rebuild the dynamic edge map and invalidate search cache
        rawGraph.buildDynamicMap();

        // Reset search engine to force rebuild
        useGraphStore.getState().resetSearchEngine();

        // Show notification if we deleted more than just the selected node
        if (nodesToDelete.size > 1) {
          toast.info(t('graphPanel.propertiesView.node.nodesRemoved', { count: nodesToDelete.size }));
        }

        if (source === 'ontology') {
          applyOntologyConnectivityPresentation(
            sigmaGraph,
            rawGraph,
            useGraphStore.getState().selectedNode
          )
        }


      } catch (error) {
        console.error('Error pruning node:', error);
      }
    };

    // If there's a node to prune, handle it
    if (nodeToPrune) {
      handleNodePrune(nodeToPrune);
      // Reset the nodeToPrune state after handling
      window.setTimeout(() => {
        useGraphStore.getState().triggerNodePrune(null);
      }, 0);
    }
  }, [nodeToPrune, sigmaGraph, rawGraph, getNodesThatWillBeDeleted, source, t]);

  // Recalculate icon/label visibility when selected node changes.
  useEffect(() => {
    if (source !== 'ontology' || !sigmaGraph || !rawGraph) return
    applyOntologyConnectivityPresentation(
      sigmaGraph,
      rawGraph,
      useGraphStore.getState().selectedNode
    )
    const unsubscribe = useGraphStore.subscribe((state, prevState) => {
      if (state.selectedNode === prevState.selectedNode) return
      applyOntologyConnectivityPresentation(sigmaGraph, rawGraph, state.selectedNode)
    })
    return () => unsubscribe()
  }, [source, sigmaGraph, rawGraph])

  const graphsuiteGraph = useCallback(() => {
    // If we already have a graph instance, return it
    if (sigmaGraph) {
      return sigmaGraph as Graph<NodeType, EdgeType>
    }

    // If no graph exists yet, create a new one and store it
    console.log('Creating new Sigma graph instance')
    const graph = new UndirectedGraph()
    useGraphStore.getState().setSigmaGraph(graph)
    return graph as Graph<NodeType, EdgeType>
  }, [sigmaGraph])

  return { graphsuiteGraph, getNode, getEdge }
}

export default useGraphsuiteGraph
