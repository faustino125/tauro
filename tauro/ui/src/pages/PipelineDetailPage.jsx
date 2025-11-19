import { useQuery } from '@tanstack/react-query'
import { useParams, useNavigate } from 'react-router-dom'
import { apiClient } from '../api/client'
import DAGViewer from '../components/DAGViewer'
import { ArrowLeft, Play, Loader } from 'lucide-react'

const DEFAULT_PROJECT_ID = 'default'

export default function PipelineDetailPage() {
  const { pipelineId } = useParams()
  const navigate = useNavigate()

  const { data: pipeline, isLoading, error } = useQuery({
    queryKey: ['pipeline', DEFAULT_PROJECT_ID, pipelineId],
    queryFn: () => apiClient.getPipeline(DEFAULT_PROJECT_ID, pipelineId),
  })

  const handleRun = async () => {
    try {
      const run = await apiClient.runPipeline(DEFAULT_PROJECT_ID, pipelineId)
      navigate(`/runs/${run.run_id}`)
    } catch (err) {
      alert(`Failed to run pipeline: ${err.message}`)
    }
  }

  if (isLoading) {
    return (
      <div className="text-center py-12 text-slate-400">
        <Loader className="w-8 h-8 animate-spin mx-auto mb-2" />
        Loading pipeline...
      </div>
    )
  }

  if (error) {
    return (
      <div className="bg-red-950/50 border border-red-800 rounded-lg p-4">
        <p className="text-red-400">Error loading pipeline: {error.message}</p>
      </div>
    )
  }

  return (
    <div>
      {/* Back button */}
      <button
        onClick={() => navigate('/pipelines')}
        className="flex items-center gap-2 text-slate-400 hover:text-slate-200 mb-6 transition"
      >
        <ArrowLeft className="w-4 h-4" />
        Back to Pipelines
      </button>

      {/* Header */}
      <div className="bg-slate-800 border border-slate-700 rounded-lg p-6 mb-6">
        <div className="flex items-start justify-between mb-4">
          <div>
            <h1 className="text-2xl font-bold mb-2">{pipeline.name || pipeline.id}</h1>
            {pipeline.description && (
              <p className="text-slate-400">{pipeline.description}</p>
            )}
          </div>
          <button
            onClick={handleRun}
            className="flex items-center gap-2 px-4 py-2 bg-tauro-600 hover:bg-tauro-700 rounded transition"
          >
            <Play className="w-4 h-4" />
            Run Pipeline
          </button>
        </div>

        <div className="flex items-center gap-4 text-sm text-slate-400">
          <div>
            <span className="font-mono">{pipeline.nodes?.length || 0}</span> nodes
          </div>
          <div className="px-2 py-0.5 bg-slate-700 rounded text-xs">
            {pipeline.type || 'batch'}
          </div>
        </div>
      </div>

      {/* DAG Visualization */}
      <div className="mb-6">
        <h2 className="text-xl font-semibold mb-3">Pipeline DAG</h2>
        <DAGViewer pipeline={pipeline} />
      </div>

      {/* Nodes list */}
      {pipeline.nodes && pipeline.nodes.length > 0 && (
        <div>
          <h2 className="text-xl font-semibold mb-3">Nodes</h2>
          <div className="space-y-3">
            {pipeline.nodes.map((node) => (
              <div
                key={node.id || node.name}
                className="bg-slate-800 border border-slate-700 rounded-lg p-4"
              >
                <div className="flex items-start justify-between mb-2">
                  <h3 className="font-semibold">{node.name}</h3>
                  {node.type && (
                    <span className="px-2 py-0.5 bg-slate-700 rounded text-xs text-slate-300">
                      {node.type}
                    </span>
                  )}
                </div>

                {node.depends_on && node.depends_on.length > 0 && (
                  <div className="text-sm text-slate-400">
                    Depends on: {node.depends_on.join(', ')}
                  </div>
                )}

                {node.command && (
                  <div className="mt-2 text-xs">
                    <div className="text-slate-500 mb-1">Command:</div>
                    <code className="bg-slate-900 px-2 py-1 rounded text-slate-300">
                      {node.command}
                    </code>
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
