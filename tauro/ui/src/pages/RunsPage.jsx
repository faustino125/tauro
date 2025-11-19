import { useNavigate } from 'react-router-dom'
import { RunCard } from '../components/RunCard'
import { useState } from 'react'
import { RefreshCw, Filter, List } from 'lucide-react'
import { useRuns } from '../hooks'
import { Button, Spinner, EmptyState, Alert } from '../components/ui'
import { STATE_FILTERS } from '../config/constants'
import { pluralize } from '../utils/format'
import { getErrorMessage } from '../utils/errors'

export default function RunsPage() {
  const navigate = useNavigate()
  const [stateFilter, setStateFilter] = useState('ALL')

  const { data, isLoading, error, refetch, isFetching } = useRuns({
    state: stateFilter === 'ALL' ? undefined : stateFilter,
    limit: 50,
  })

  const runs = data?.runs || []
  const total = data?.total || 0

  return (
    <div>
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-3xl font-bold mb-2">Pipeline Runs</h1>
          <p className="text-slate-400">
            {total} {pluralize(total, 'run')} total
            {isFetching && !isLoading && (
              <span className="ml-2 text-xs text-tauro-400">(updating...)</span>
            )}
          </p>
        </div>

        <Button
          onClick={() => refetch()}
          disabled={isLoading}
          icon={RefreshCw}
          className={isFetching ? 'animate-pulse' : ''}
        >
          Refresh
        </Button>
      </div>

      {/* Filters */}
      <div className="mb-6 flex items-center gap-3">
        <Filter className="w-4 h-4 text-slate-400" />
        <div className="flex gap-2 flex-wrap">
          {STATE_FILTERS.map((state) => (
            <button
              key={state}
              onClick={() => setStateFilter(state)}
              className={`px-3 py-1 rounded text-sm transition ${
                stateFilter === state
                  ? 'bg-tauro-600 text-white'
                  : 'bg-slate-800 text-slate-300 hover:bg-slate-700'
              }`}
            >
              {state}
            </button>
          ))}
        </div>
      </div>

      {/* Error state */}
      {error && (
        <Alert variant="error" title="Error loading runs" className="mb-6">
          {getErrorMessage(error)}
        </Alert>
      )}

      {/* Loading state */}
      {isLoading && runs.length === 0 && (
        <Spinner size="lg" text="Loading runs..." className="py-12" />
      )}

      {/* Empty state */}
      {!isLoading && runs.length === 0 && (
        <EmptyState
          icon={List}
          title="No runs found"
          description={stateFilter === 'ALL' 
            ? "No pipeline runs have been created yet. Start a pipeline to see runs here."
            : `No runs found with state "${stateFilter}". Try changing the filter.`
          }
        />
      )}

      {/* Runs grid */}
      {runs.length > 0 && (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {runs.map((run) => (
            <RunCard
              key={run.run_id}
              run={run}
              onClick={() => navigate(`/runs/${run.run_id}`)}
            />
          ))}
        </div>
      )}
    </div>
  )
}
