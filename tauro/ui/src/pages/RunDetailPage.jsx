import { useParams, useNavigate } from 'react-router-dom'
import { RunStateBadge } from '../components/RunCard'
import LogsViewer from '../components/LogsViewer'
import { ArrowLeft, XCircle, Clock, Calendar, Timer, AlertCircle } from 'lucide-react'
import { useRun, useCancelRun } from '../hooks'
import { Button, Spinner, Alert, Card, CardHeader, CardTitle, CardContent } from '../components/ui'
import { formatDateTime, formatRelativeTime, calculateDuration, formatDuration } from '../utils/format'
import { getErrorMessage, getUserFriendlyMessage } from '../utils/errors'
import { RUN_STATES, TERMINAL_STATES } from '../config/constants'

export default function RunDetailPage() {
  const { runId } = useParams()
  const navigate = useNavigate()

  const { data: run, isLoading, error } = useRun(runId)
  const cancelMutation = useCancelRun()

  const handleCancel = async () => {
    if (!window.confirm('Are you sure you want to cancel this run?')) return
    
    try {
      await cancelMutation.mutateAsync({ 
        runId, 
        reason: 'Cancelled by user from UI' 
      })
    } catch (err) {
      alert(`Failed to cancel run: ${getUserFriendlyMessage(err)}`)
    }
  }

  if (isLoading) {
    return <Spinner size="lg" text="Loading run details..." className="py-12" />
  }

  if (error) {
    return (
      <Alert variant="error" title="Error loading run">
        {getErrorMessage(error)}
      </Alert>
    )
  }

  const duration = calculateDuration(run.started_at, run.finished_at)
  const canCancel = [RUN_STATES.PENDING, RUN_STATES.RUNNING].includes(run.state)

  return (
    <div>
      {/* Back button */}
      <Button
        onClick={() => navigate('/runs')}
        variant="ghost"
        icon={ArrowLeft}
        className="mb-6"
      >
        Back to Runs
      </Button>

      {/* Header */}
      <Card className="mb-6">
        <CardHeader>
          <div className="flex items-start justify-between">
            <div>
              <CardTitle className="text-2xl mb-2">{run.pipeline_id}</CardTitle>
              <p className="text-slate-400 font-mono text-sm">{run.run_id || runId}</p>
            </div>
            <div className="flex items-center gap-3">
              <RunStateBadge state={run.state} />
              {canCancel && (
                <Button
                  onClick={handleCancel}
                  loading={cancelMutation.isPending}
                  disabled={cancelMutation.isPending}
                  variant="danger"
                  icon={XCircle}
                  size="sm"
                >
                  Cancel
                </Button>
              )}
            </div>
          </div>
        </CardHeader>

        <CardContent>
          {/* Metadata grid */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
            {run.created_at && (
              <div className="flex items-start gap-2 text-slate-400">
                <Calendar className="w-4 h-4 mt-0.5 flex-shrink-0" />
                <div>
                  <div className="text-xs text-slate-500 mb-1">Created</div>
                  <div className="font-medium">{formatDateTime(run.created_at)}</div>
                  <div className="text-xs mt-0.5">{formatRelativeTime(run.created_at)}</div>
                </div>
              </div>
            )}

            {run.started_at && (
              <div className="flex items-start gap-2 text-slate-400">
                <Clock className="w-4 h-4 mt-0.5 flex-shrink-0" />
                <div>
                  <div className="text-xs text-slate-500 mb-1">Started</div>
                  <div className="font-medium">{formatDateTime(run.started_at)}</div>
                </div>
              </div>
            )}

            {duration !== null && (
              <div className="flex items-start gap-2 text-slate-400">
                <Timer className="w-4 h-4 mt-0.5 flex-shrink-0" />
                <div>
                  <div className="text-xs text-slate-500 mb-1">Duration</div>
                  <div className="font-medium">{formatDuration(duration)}</div>
                </div>
              </div>
            )}
          </div>

          {/* Parameters */}
          {run.params && Object.keys(run.params).length > 0 && (
            <div className="mt-4 pt-4 border-t border-slate-700">
              <h3 className="text-sm font-semibold mb-2">Parameters</h3>
              <pre className="text-xs bg-slate-900 p-3 rounded overflow-x-auto">
                {JSON.stringify(run.params, null, 2)}
              </pre>
            </div>
          )}

          {/* Error */}
          {run.error && (
            <div className="mt-4 pt-4 border-t border-slate-700">
              <Alert variant="error" title="Error">
                {run.error}
              </Alert>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Logs */}
      <LogsViewer
        runId={runId}
        streamLogs={run.state !== RUN_STATES.PENDING}
      />
    </div>
  )
}
