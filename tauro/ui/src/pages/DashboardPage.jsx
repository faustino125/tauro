import React from 'react'
import { useQuery } from '@tanstack/react-query'
import { apiClient } from '../api/client'
import { Activity, CheckCircle, XCircle, Clock, Server } from 'lucide-react'

function StatCard({ title, value, icon: Icon, color, subtext }) {
  return (
    <div className="bg-slate-800 rounded-lg p-6 border border-slate-700 shadow-sm">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-slate-400 text-sm font-medium uppercase tracking-wider">{title}</h3>
        <div className={`p-2 rounded-full bg-opacity-10 ${color.bg}`}>
          <Icon className={`w-5 h-5 ${color.text}`} />
        </div>
      </div>
      <div className="flex items-baseline gap-2">
        <span className="text-3xl font-bold text-white">{value}</span>
        {subtext && <span className="text-sm text-slate-500">{subtext}</span>}
      </div>
    </div>
  )
}

export default function DashboardPage() {
  const { data: stats, isLoading, error } = useQuery({
    queryKey: ['stats'],
    queryFn: () => apiClient.getStats(),
    refetchInterval: 30000, // Refresh every 30s
  })

  const { data: health } = useQuery({
    queryKey: ['health'],
    queryFn: () => apiClient.getHealth(),
    refetchInterval: 60000,
  })

  if (isLoading) {
    return <div className="text-center py-12 text-slate-400">Loading dashboard...</div>
  }

  if (error) {
    return (
      <div className="bg-red-900/20 border border-red-900 text-red-200 p-4 rounded-lg">
        Error loading dashboard: {error.message}
      </div>
    )
  }

  return (
    <div className="space-y-8">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold text-white">Dashboard</h1>
        <div className="flex items-center gap-2 text-sm">
          <span className={`w-2 h-2 rounded-full ${health?.status === 'healthy' ? 'bg-green-500' : 'bg-red-500'}`} />
          <span className="text-slate-400">System Status: {health?.status || 'Unknown'}</span>
        </div>
      </div>

      {/* Stats Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <StatCard
          title="Active Runs"
          value={stats?.active_runs || 0}
          icon={Activity}
          color={{ bg: 'bg-blue-500', text: 'text-blue-500' }}
          subtext="Currently executing"
        />
        <StatCard
          title="Total Pipelines"
          value={stats?.total_pipelines || 0}
          icon={Server}
          color={{ bg: 'bg-purple-500', text: 'text-purple-500' }}
          subtext="Defined in project"
        />
        <StatCard
          title="Failed Runs"
          value={stats?.failed_runs || 0}
          icon={XCircle}
          color={{ bg: 'bg-red-500', text: 'text-red-500' }}
          subtext="Total failures"
        />
        <StatCard
          title="Active Schedules"
          value={stats?.total_schedules || 0}
          icon={Clock}
          color={{ bg: 'bg-green-500', text: 'text-green-500' }}
          subtext="Enabled triggers"
        />
      </div>

      {/* Recent Activity Section Placeholder */}
      <div className="bg-slate-800 rounded-lg border border-slate-700 p-6">
        <h2 className="text-lg font-semibold text-white mb-4">System Health</h2>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {health?.components && Object.entries(health.components).map(([component, status]) => (
            <div key={component} className="flex items-center justify-between p-3 bg-slate-900 rounded border border-slate-700">
              <span className="capitalize text-slate-300">{component}</span>
              <span className={`px-2 py-1 rounded text-xs font-medium ${
                status === 'healthy' ? 'bg-green-900/30 text-green-400' : 'bg-red-900/30 text-red-400'
              }`}>
                {status}
              </span>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}
