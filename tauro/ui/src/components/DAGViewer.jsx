import { useCallback } from 'react'
import { 
  ReactFlow, 
  Background, 
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
  MarkerType,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'

// Convert pipeline nodes to React Flow format
function convertToFlowNodes(pipelineNodes = []) {
  const nodes = pipelineNodes.map((node, index) => ({
    id: node.id || node.name,
    type: 'default',
    position: { x: 250 * index, y: 100 },
    data: { 
      label: (
        <div className="text-center">
          <div className="font-semibold">{node.name}</div>
          {node.type && <div className="text-xs text-slate-400">{node.type}</div>}
        </div>
      ),
    },
  }))

  return nodes
}

function convertToFlowEdges(pipelineNodes = []) {
  const edges = []

  pipelineNodes.forEach((node) => {
    const nodeId = node.id || node.name
    const dependencies = node.depends_on || []

    dependencies.forEach((depId) => {
      edges.push({
        id: `${depId}-${nodeId}`,
        source: depId,
        target: nodeId,
        type: 'smoothstep',
        animated: true,
        markerEnd: {
          type: MarkerType.ArrowClosed,
        },
      })
    })
  })

  return edges
}

export default function DAGViewer({ pipeline }) {
  const initialNodes = convertToFlowNodes(pipeline?.nodes)
  const initialEdges = convertToFlowEdges(pipeline?.nodes)

  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes)
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges)

  const onInit = useCallback((instance) => {
    instance.fitView({ padding: 0.2 })
  }, [])

  if (!pipeline || !pipeline.nodes || pipeline.nodes.length === 0) {
    return (
      <div className="h-96 bg-slate-800 border border-slate-700 rounded-lg flex items-center justify-center">
        <p className="text-slate-400">No pipeline DAG to display</p>
      </div>
    )
  }

  return (
    <div className="h-96 bg-slate-800 border border-slate-700 rounded-lg overflow-hidden">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onInit={onInit}
        fitView
        className="bg-slate-900"
      >
        <Background color="#334155" gap={16} />
        <Controls />
        <MiniMap 
          nodeColor="#0ea5e9"
          maskColor="rgba(0, 0, 0, 0.6)"
        />
      </ReactFlow>
    </div>
  )
}
