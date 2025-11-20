/**
 * Tauro API Client
 * 
 * Centralized client for all API interactions with Tauro backend.
 * Includes smart error handling, request cancellation, and request deduplication.
 */

import { API_CONFIG } from '../config/constants'
import { APIError } from '../utils/errors'

class TauroAPIClient {
  constructor(baseURL = API_CONFIG.BASE_URL) {
    this.baseURL = baseURL
    this.pendingRequests = new Map() // Track pending requests by key
    this.authToken = null // Will be set by auth context
  }

  /**
   * Set authentication token
   */
  setAuthToken(token) {
    this.authToken = token
  }

  /**
   * Clear authentication token
   */
  clearAuthToken() {
    this.authToken = null
  }

  /**
   * Make an HTTP request with automatic cancellation of duplicate requests
   */
  async request(endpoint, options = {}) {
    const url = `${this.baseURL}${endpoint}`
    const controller = new AbortController()
    const key = `${options.method || 'GET'} ${endpoint}`
    let timeoutId
    
    // Cancel previous request to same endpoint
    this._cancelPreviousRequest(key, controller)

    try {
      const headers = this._buildHeaders(options.headers)
      
      // Implement manual timeout since fetch does not support a timeout option
      timeoutId = setTimeout(() => controller.abort(), API_CONFIG.TIMEOUT)

      const response = await fetch(url, {
        signal: controller.signal,
        headers,
        ...options,
      })

      return await this._handleResponse(response)
      
    } catch (error) {
      throw this._handleRequestError(error, endpoint, timeoutId)
    } finally {
      if (timeoutId) clearTimeout(timeoutId)
      this.pendingRequests.delete(key)
    }
  }

  /**
   * Cancel previous pending request to the same endpoint
   */
  _cancelPreviousRequest(key, controller) {
    if (this.pendingRequests.has(key)) {
      const previous = this.pendingRequests.get(key)
      console.debug(`Cancelling previous request: ${key}`)
      previous.abort()
    }
    this.pendingRequests.set(key, controller)
  }

  /**
   * Build request headers with authentication
   */
  _buildHeaders(customHeaders = {}) {
    const headers = {
      'Content-Type': 'application/json',
      ...customHeaders,
    }

    // Add auth token if available
    if (this.authToken) {
      headers['Authorization'] = `Bearer ${this.authToken}`
    }

    return headers
  }

  /**
   * Handle fetch response and parse JSON
   */
  async _handleResponse(response) {
    const contentType = response.headers.get('content-type')
    const isJson = contentType?.includes('application/json')

    if (!response.ok) {
      const error = isJson 
        ? await response.json().catch(() => ({ detail: response.statusText }))
        : { detail: response.statusText }
      
      throw new APIError(
        error.detail || error.message || 'API request failed',
        response.status,
        error.code,
        error.details
      )
    }

    return isJson ? await response.json() : await response.text()
  }

  /**
   * Handle request errors (abort, network, etc)
   */
  _handleRequestError(error, endpoint, timeoutId) {
    if (error.name === 'AbortError') {
      const isTimeout = timeoutId == null
      console.debug(`Request aborted (${isTimeout ? 'timeout' : 'cancel'}): ${endpoint}`)
      
      const abortErr = new Error(isTimeout ? 'Request timed out' : 'Request was cancelled')
      abortErr.cancelled = !isTimeout
      abortErr.timeout = isTimeout
      return abortErr
    }
    
    // Re-throw APIError as-is
    if (error instanceof APIError) {
      return error
    }
    
    // Wrap other errors
    return new APIError(
      error.message || 'Network request failed',
      null,
      'NETWORK_ERROR',
      { originalError: error }
    )
  }

  // ===== Projects =====
  
  async listProjects() {
    return this.request('/projects')
  }

  async getProject(projectId) {
    return this.request(`/projects/${projectId}`)
  }

  // ===== Pipelines =====

  async listPipelines(projectId) {
    const response = await this.request(`/projects/${projectId}/pipelines`)
    return response.pipelines || []
  }

  async getPipeline(projectId, pipelineId) {
    return this.request(`/projects/${projectId}/pipelines/${pipelineId}`)
  }

  async runPipeline(projectId, pipelineId, params = {}, tags = {}) {
    return this.request(`/projects/${projectId}/pipelines/${pipelineId}/runs`, {
      method: 'POST',
      body: JSON.stringify({ params, tags }),
    })
  }

  // ===== Runs =====

  async listRuns(filters = {}) {
    const params = new URLSearchParams()
    
    if (filters.projectId) params.append('project_id', filters.projectId)
    if (filters.pipelineId) params.append('pipeline_id', filters.pipelineId)
    if (filters.state) params.append('state', filters.state)
    if (filters.skip !== undefined) params.append('skip', filters.skip)
    if (filters.limit !== undefined) params.append('limit', filters.limit)

    const query = params.toString() ? `?${params}` : ''
    const response = await this.request(`/runs${query}`)
    return {
      runs: response.data || [],
      total: response.pagination?.total || 0,
      pagination: response.pagination || null,
    }
  }

  async getRun(runId) {
    const response = await this.request(`/runs/${runId}`)
    return response.data || response
  }

  async cancelRun(runId, reason = '') {
    return this.request(`/runs/${runId}/cancel`, {
      method: 'POST',
      body: JSON.stringify({ reason }),
    })
  }

  // ===== Logs =====

  async getRunLogs(runId, filters = {}) {
    const params = new URLSearchParams()
    
    if (filters.level) params.append('level', filters.level)
    if (filters.skip !== undefined) params.append('skip', filters.skip)
    if (filters.limit !== undefined) params.append('limit', filters.limit)

    const query = params.toString() ? `?${params}` : ''
    const response = await this.request(`/runs/${runId}/logs${query}`)
    return {
      logs: response.data || [],
      total: response.pagination?.total || 0,
      pagination: response.pagination || null,
    }
  }

  /**
   * Stream logs in real-time using Server-Sent Events (SSE)
   * 
   * @param {string} runId - Run ID
   * @param {function} onLog - Callback for each log message
   * @param {function} onError - Callback for errors
   * @returns {EventSource} - EventSource instance (call .close() to stop)
   */
  streamLogs(runId, onLog, onError = console.error) {
    const url = `${this.baseURL}/logs/runs/${runId}/stream-sse`
    const eventSource = new EventSource(url)

    eventSource.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data)
        
        if (data.type === 'complete') {
          eventSource.close()
        } else if (data.error) {
          onError(new Error(data.error))
          eventSource.close()
        } else {
          onLog(data)
        }
      } catch (err) {
        onError(err)
      }
    }

    eventSource.onerror = (error) => {
      onError(error)
      eventSource.close()
    }

    return eventSource
  }

  // ===== Tasks =====

  async getRunTasks(runId, filters = {}) {
    const params = new URLSearchParams()
    
    if (filters.skip !== undefined) params.append('skip', filters.skip)
    if (filters.limit !== undefined) params.append('limit', filters.limit)

    const query = params.toString() ? `?${params}` : ''
    const response = await this.request(`/runs/${runId}/tasks${query}`)
    return {
      tasks: response.data || [],
      total: response.pagination?.total || 0,
      pagination: response.pagination || null,
    }
  }

  // ===== Schedules =====

  async listSchedules(filters = {}) {
    const params = new URLSearchParams()
    if (filters.pipelineId) params.append('pipeline_id', filters.pipelineId)
    if (filters.enabled !== undefined) params.append('enabled', filters.enabled)
    if (filters.limit !== undefined) params.append('limit', filters.limit)
    if (filters.offset !== undefined) params.append('offset', filters.offset)

    const query = params.toString() ? `?${params}` : ''
    const response = await this.request(`/schedules${query}`)
    return {
      schedules: response.data || [],
      total: response.pagination?.total || 0,
      pagination: response.pagination || null,
    }
  }

  async createSchedule(data) {
    return this.request('/schedules', {
      method: 'POST',
      body: JSON.stringify(data),
    })
  }

  async updateSchedule(scheduleId, data) {
    return this.request(`/schedules/${scheduleId}`, {
      method: 'PATCH',
      body: JSON.stringify(data),
    })
  }

  async deleteSchedule(scheduleId) {
    return this.request(`/schedules/${scheduleId}`, {
      method: 'DELETE',
    })
  }

  async pauseSchedule(scheduleId) {
    return this.request(`/schedules/${scheduleId}/pause`, {
      method: 'POST',
    })
  }

  async resumeSchedule(scheduleId) {
    return this.request(`/schedules/${scheduleId}/resume`, {
      method: 'POST',
    })
  }

  // ===== Monitoring & Stats =====

  async getStats() {
    return this.request('/stats')
  }

  async getHealth() {
    return this.request('/health')
  }
}

// Singleton instance
export const apiClient = new TauroAPIClient()
