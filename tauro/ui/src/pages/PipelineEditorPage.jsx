import React, { useCallback, useEffect, useMemo } from 'react';
import { useParams } from 'react-router-dom';
import { ReactFlow, Background, Controls, MiniMap, useNodesState, useEdgesState, addEdge } from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import { useQuery } from '@tanstack/react-query';
import { apiClient } from '../api/client';
import CustomNode from '../components/PipelineEditor/CustomNode';
import { Save } from 'lucide-react';

const nodeTypes = {
  custom: CustomNode,
};

// Helper to convert Tauro pipeline nodes to React Flow nodes/edges
const pipelineToFlow = (pipeline) => {
  if (!pipeline || !pipeline.nodes) return { nodes: [], edges: [] };

  const nodes = pipeline.nodes.map((node, index) => ({
    id: node.name, // Use name as ID for simplicity
    type: 'custom',
    position: { x: 100 + (index * 250), y: 100 + (index % 2 === 0 ? 0 : 100) }, // Simple auto-layout
    data: { 
      label: node.name,
      type: node.type,
      implementation: node.implementation 
    },
  }));

  const edges = [];
  pipeline.nodes.forEach((node) => {
    if (node.dependencies) {
      node.dependencies.forEach((dep) => {
        edges.push({
          id: `${dep}-${node.name}`,
          source: dep,
          target: node.name,
          animated: true,
          style: { stroke: '#64748b' },
        });
      });
    }
  });

  return { nodes, edges };
};

export default function PipelineEditorPage() {
  const { projectId, pipelineId } = useParams();
  
  // Fetch pipeline data
  const { data: pipeline, isLoading } = useQuery({
    queryKey: ['pipeline', projectId, pipelineId],
    queryFn: () => apiClient.getPipeline(projectId, pipelineId),
    enabled: !!projectId && !!pipelineId,
  });

  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);

  // Initialize flow when data loads
  useEffect(() => {
    if (pipeline) {
      const { nodes: initialNodes, edges: initialEdges } = pipelineToFlow(pipeline);
      setNodes(initialNodes);
      setEdges(initialEdges);
    }
  }, [pipeline, setNodes, setEdges]);

  const onConnect = useCallback(
    (params) => setEdges((eds) => addEdge(params, eds)),
    [setEdges],
  );

  if (isLoading) return <div className="text-center py-12 text-slate-400">Loading editor...</div>;

  return (
    <div className="h-[calc(100vh-100px)] flex flex-col">
      <div className="flex items-center justify-between mb-4">
        <div>
          <h1 className="text-2xl font-bold text-white">Pipeline Editor</h1>
          <p className="text-slate-400 text-sm">{pipeline?.name || 'Untitled Pipeline'}</p>
        </div>
        <div className="flex gap-2">
          <button className="flex items-center gap-2 bg-tauro-600 hover:bg-tauro-500 text-white px-4 py-2 rounded transition">
            <Save className="w-4 h-4" />
            Save Changes
          </button>
        </div>
      </div>

      <div className="flex-1 bg-slate-900 rounded-lg border border-slate-700 overflow-hidden">
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onConnect={onConnect}
          nodeTypes={nodeTypes}
          fitView
          className="bg-slate-900"
        >
          <Background color="#334155" gap={16} />
          <Controls className="bg-slate-800 border-slate-700 fill-slate-400" />
          <MiniMap 
            className="bg-slate-800 border-slate-700" 
            nodeColor="#64748b"
            maskColor="rgba(30, 41, 59, 0.8)"
          />
        </ReactFlow>
      </div>
    </div>
  );
}
