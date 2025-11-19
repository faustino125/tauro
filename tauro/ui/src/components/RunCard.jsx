import { CheckCircle, XCircle, Clock, Loader, Circle } from 'lucide-react'
import { Card, CardHeader, CardTitle, CardContent, Badge } from './ui'
import { formatRelativeTime, formatDuration, calculateDuration, truncate } from '../utils/format'
import { STATE_COLORS, RUN_STATES } from '../config/constants'

export function RunStateBadge({ state }) {
  const stateColors = STATE_COLORS[state] || STATE_COLORS[RUN_STATES.PENDING]
  
  const icons = {
    PENDING: Circle,
    RUNNING: Loader,
    SUCCESS: CheckCircle,
    FAILED: XCircle,
    CANCELLED: Clock,
  }
  
  const Icon = icons[state] || Circle
  const isAnimated = state === RUN_STATES.RUNNING

  return (
    <Badge 
      variant={state === RUN_STATES.SUCCESS ? 'success' : 
              state === RUN_STATES.FAILED ? 'danger' : 
              state === RUN_STATES.RUNNING ? 'info' : 
              state === RUN_STATES.CANCELLED ? 'warning' : 'default'}
      className={`${stateColors.bg} ${stateColors.text} flex items-center gap-1.5`}
    >
      <Icon className={`w-3 h-3 ${isAnimated ? 'animate-spin' : ''}`} />
      {state}
    </Badge>
  )
}

export function RunCard({ run, onClick }) {
  const duration = calculateDuration(run.started_at, run.finished_at)
  const hasError = run.error && run.error.length > 0

  return (
    <Card hover onClick={onClick} padding="p-4">
      <CardHeader className="mb-3">
        <div className="flex items-start justify-between gap-2">
          <div className="min-w-0 flex-1">
            <CardTitle className="text-lg truncate">
              {run.pipeline_id || 'Unknown Pipeline'}
            </CardTitle>
            <p className="text-sm text-slate-400 font-mono truncate">
              {run.run_id || run.id}
            </p>
          </div>
          <RunStateBadge state={run.state} />
        </div>
      </CardHeader>

      <CardContent>
        <div className="space-y-2 text-sm text-slate-400">
          {run.created_at && (
            <div className="flex items-center gap-2">
              <Clock className="w-4 h-4 flex-shrink-0" />
              <span>{formatRelativeTime(run.created_at)}</span>
            </div>
          )}
          
          {duration !== null && (
            <div className="flex items-center gap-2">
              <span className="text-slate-500">Duration:</span>
              <span className="font-medium">{formatDuration(duration)}</span>
            </div>
          )}

          {run.params && Object.keys(run.params).length > 0 && (
            <div className="flex items-center gap-2">
              <span className="text-slate-500">Params:</span>
              <span className="font-mono text-xs">
                {Object.keys(run.params).length} parameter(s)
              </span>
            </div>
          )}

          {hasError && (
            <div className="text-red-400 text-xs mt-2 p-2 bg-red-950/50 rounded border border-red-800">
              {truncate(run.error, 100)}
            </div>
          )}
        </div>
      </CardContent>
    </Card>
  )
}
