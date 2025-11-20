import React, { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { apiClient } from '../api/client'
import { Play, Pause, Trash2, Plus, Calendar, AlertCircle } from 'lucide-react'
import { format } from 'date-fns'

export default function SchedulesPage() {
  const queryClient = useQueryClient()
  const [filter, setFilter] = useState('all') // all, enabled, disabled

  const { data, isLoading, error } = useQuery({
    queryKey: ['schedules', filter],
    queryFn: () => apiClient.listSchedules({ 
      enabled: filter === 'all' ? undefined : filter === 'enabled' 
    }),
  })

  const toggleMutation = useMutation({
    mutationFn: ({ id, enabled }) => 
      enabled ? apiClient.pauseSchedule(id) : apiClient.resumeSchedule(id),
    onSuccess: () => {
      queryClient.invalidateQueries(['schedules'])
    },
  })

  const deleteMutation = useMutation({
    mutationFn: (id) => apiClient.deleteSchedule(id),
    onSuccess: () => {
      queryClient.invalidateQueries(['schedules'])
    },
  })

  if (isLoading) return <div className="text-center py-12 text-slate-400">Loading schedules...</div>
  if (error) return <div className="text-red-400 p-4">Error: {error.message}</div>

  const schedules = data?.schedules || []

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">Schedules</h1>
          <p className="text-slate-400">Manage automated pipeline executions</p>
        </div>
        <button className="flex items-center gap-2 bg-tauro-600 hover:bg-tauro-500 text-white px-4 py-2 rounded-lg transition">
          <Plus className="w-4 h-4" />
          Create Schedule
        </button>
      </div>

      {/* Filters */}
      <div className="flex gap-2 border-b border-slate-700 pb-4">
        {['all', 'enabled', 'disabled'].map((f) => (
          <button
            key={f}
            onClick={() => setFilter(f)}
            className={`px-4 py-1 rounded-full text-sm font-medium transition ${
              filter === f 
                ? 'bg-slate-700 text-white' 
                : 'text-slate-400 hover:text-white'
            }`}
          >
            {f.charAt(0).toUpperCase() + f.slice(1)}
          </button>
        ))}
      </div>

      {/* List */}
      <div className="grid gap-4">
        {schedules.length === 0 ? (
          <div className="text-center py-12 bg-slate-800/50 rounded-lg border border-slate-700 border-dashed">
            <Calendar className="w-12 h-12 text-slate-600 mx-auto mb-4" />
            <h3 className="text-lg font-medium text-slate-300">No schedules found</h3>
            <p className="text-slate-500">Create a schedule to automate your pipelines.</p>
          </div>
        ) : (
          schedules.map((schedule) => (
            <div 
              key={schedule.id} 
              className="bg-slate-800 rounded-lg border border-slate-700 p-4 flex items-center justify-between hover:border-slate-600 transition"
            >
              <div className="flex items-start gap-4">
                <div className={`p-2 rounded-lg ${schedule.enabled ? 'bg-green-900/20 text-green-400' : 'bg-slate-700 text-slate-400'}`}>
                  <Calendar className="w-5 h-5" />
                </div>
                <div>
                  <div className="flex items-center gap-2">
                    <h3 className="font-medium text-white">{schedule.pipeline_id}</h3>
                    <span className={`text-xs px-2 py-0.5 rounded-full ${
                      schedule.enabled ? 'bg-green-900/30 text-green-400' : 'bg-slate-700 text-slate-400'
                    }`}>
                      {schedule.enabled ? 'Active' : 'Paused'}
                    </span>
                  </div>
                  <div className="flex items-center gap-4 mt-1 text-sm text-slate-400">
                    <span className="font-mono bg-slate-900 px-2 py-0.5 rounded text-xs">
                      {schedule.expression}
                    </span>
                    <span>Next run: {schedule.next_run_at ? format(new Date(schedule.next_run_at), 'PP p') : 'N/A'}</span>
                  </div>
                </div>
              </div>

              <div className="flex items-center gap-2">
                <button
                  onClick={() => toggleMutation.mutate({ id: schedule.id, enabled: schedule.enabled })}
                  className="p-2 text-slate-400 hover:text-white hover:bg-slate-700 rounded transition"
                  title={schedule.enabled ? "Pause" : "Resume"}
                >
                  {schedule.enabled ? <Pause className="w-4 h-4" /> : <Play className="w-4 h-4" />}
                </button>
                <button
                  onClick={() => {
                    if (confirm('Are you sure you want to delete this schedule?')) {
                      deleteMutation.mutate(schedule.id)
                    }
                  }}
                  className="p-2 text-slate-400 hover:text-red-400 hover:bg-red-900/20 rounded transition"
                  title="Delete"
                >
                  <Trash2 className="w-4 h-4" />
                </button>
              </div>
            </div>
          ))
        )}
      </div>
    </div>
  )
}
