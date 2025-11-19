/**
 * Custom hook for managing runs
 */
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { apiClient } from '../api/client'
import { POLLING_INTERVALS } from '../config/constants'

/**
 * Hook to list runs with filters
 */
export function useRuns(filters = {}) {
  return useQuery({
    queryKey: ['runs', filters],
    queryFn: () => apiClient.listRuns(filters),
    refetchInterval: POLLING_INTERVALS.RUNS_LIST,
    staleTime: 1000,
    retry: (failureCount, error) => {
      // Don't retry on 4xx errors
      if (error?.status >= 400 && error?.status < 500) return false
      return failureCount < 3
    },
  })
}

/**
 * Hook to get a single run by ID
 */
export function useRun(runId, options = {}) {
  return useQuery({
    queryKey: ['run', runId],
    queryFn: () => apiClient.getRun(runId),
    enabled: !!runId,
    refetchInterval: (data) => {
      // Stop refetching if run is in terminal state
      const terminalStates = ['SUCCESS', 'FAILED', 'CANCELLED']
      const state = data?.state
      return terminalStates.includes(state) ? false : POLLING_INTERVALS.RUN_DETAIL
    },
    staleTime: 1000,
    ...options,
  })
}

/**
 * Hook to cancel a run
 */
export function useCancelRun() {
  const queryClient = useQueryClient()
  
  return useMutation({
    mutationFn: ({ runId, reason }) => apiClient.cancelRun(runId, reason),
    onSuccess: (data, variables) => {
      // Invalidate run queries to trigger refetch
      queryClient.invalidateQueries(['run', variables.runId])
      queryClient.invalidateQueries(['runs'])
    },
  })
}

/**
 * Hook to get run logs
 */
export function useRunLogs(runId, filters = {}) {
  return useQuery({
    queryKey: ['run-logs', runId, filters],
    queryFn: () => apiClient.getRunLogs(runId, filters),
    enabled: !!runId,
    refetchInterval: POLLING_INTERVALS.LOGS,
    staleTime: 500,
  })
}

/**
 * Hook to get run tasks
 */
export function useRunTasks(runId, filters = {}) {
  return useQuery({
    queryKey: ['run-tasks', runId, filters],
    queryFn: () => apiClient.getRunTasks(runId, filters),
    enabled: !!runId,
    staleTime: 2000,
  })
}
