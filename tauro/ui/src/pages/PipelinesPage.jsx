import { useQuery } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import { apiClient } from '../api/client'
import { GitBranch, Loader, RefreshCw } from 'lucide-react'

export default function PipelinesPage() {
  const navigate = useNavigate()
  // Use 'default' as fallback project ID
  const projectId = 'default'

  const { data: pipelines = [], isLoading, error, refetch } = useQuery({
    queryKey: ['pipelines', projectId],
    queryFn: () => apiClient.listPipelines(projectId),
  })

  return (
    <div>
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-3xl font-bold mb-2">Pipelines</h1>
          <p className="text-slate-400">
            {pipelines.length} pipeline{pipelines.length !== 1 ? 's' : ''} available
          </p>
        </div>
        <button
          onClick={() => refetch()}
          disabled={isLoading}
          className="flex items-center gap-2 px-4 py-2 bg-tauro-600 hover:bg-tauro-700 disabled:opacity-50 rounded transition"
          title="Refresh pipelines"
        >
          <RefreshCw className={`w-4 h-4 ${isLoading ? 'animate-spin' : ''}`} />
          Refresh
        </button>
      </div>

      {/* Error state */}
      {error && (
        <div className="bg-red-950/50 border border-red-800 rounded-lg p-4 mb-6">
          <p className="text-red-400">Error loading pipelines: {error.message}</p>
        </div>
      )}

      {/* Loading state */}
      {isLoading && (
        <div className="text-center py-12 text-slate-400">
          <Loader className="w-8 h-8 animate-spin mx-auto mb-2" />
          Loading pipelines...
        </div>
      )}

      {/* Empty state */}
      {!isLoading && pipelines.length === 0 && (
        <div className="text-center py-12 text-slate-400">
          <GitBranch className="w-12 h-12 mx-auto mb-4 opacity-50" />
          <p className="text-lg mb-2">No pipelines found</p>
          <p className="text-sm">Configure pipelines in your Tauro project</p>
        </div>
      )}

      {/* Pipelines grid */}
      {pipelines.length > 0 && (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {pipelines.map((pipeline) => (
            <div
              key={pipeline.id}
              onClick={() => navigate(`/pipelines/${pipeline.id}`)}
              className="bg-slate-800 border border-slate-700 rounded-lg p-5 hover:border-tauro-500 transition cursor-pointer"
            >
              <div className="flex items-start justify-between mb-3">
                <div>
                  <h3 className="text-lg font-semibold mb-1">{pipeline.name || pipeline.id}</h3>
                  {pipeline.description && (
                    <p className="text-sm text-slate-400">{pipeline.description}</p>
                  )}
                </div>
                <GitBranch className="w-5 h-5 text-tauro-500 flex-shrink-0" />
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
          ))}
        </div>
      )}
    </div>
  )
}
