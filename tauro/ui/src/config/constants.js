/**
 * Application constants and configuration
 */

// API Configuration
export const API_CONFIG = {
  BASE_URL: import.meta.env.VITE_API_BASE_URL || '/api/v1',
  TIMEOUT: 30000,
  RETRY_ATTEMPTS: 3,
}

// Application Info
export const APP_INFO = {
  NAME: import.meta.env.VITE_APP_NAME || 'Tauro',
  VERSION: import.meta.env.VITE_APP_VERSION || '0.1.0',
}

// Feature Flags
export const FEATURES = {
  LOGS_STREAMING: import.meta.env.VITE_ENABLE_LOGS_STREAMING !== 'false',
  REAL_TIME_UPDATES: import.meta.env.VITE_ENABLE_REAL_TIME_UPDATES !== 'false',
}

// Polling Intervals
export const POLLING_INTERVALS = {
  RUNS_LIST: parseInt(import.meta.env.VITE_RUNS_REFRESH_INTERVAL || '5000'),
  RUN_DETAIL: parseInt(import.meta.env.VITE_RUN_DETAIL_REFRESH_INTERVAL || '3000'),
  LOGS: 2000,
}

// Pagination
export const PAGINATION = {
  DEFAULT_PAGE_SIZE: parseInt(import.meta.env.VITE_DEFAULT_PAGE_SIZE || '50'),
  MAX_PAGE_SIZE: parseInt(import.meta.env.VITE_MAX_PAGE_SIZE || '100'),
}

// Run States
export const RUN_STATES = {
  PENDING: 'PENDING',
  RUNNING: 'RUNNING',
  SUCCESS: 'SUCCESS',
  FAILED: 'FAILED',
  CANCELLED: 'CANCELLED',
}

// Terminal states (run won't change anymore)
export const TERMINAL_STATES = [
  RUN_STATES.SUCCESS,
  RUN_STATES.FAILED,
  RUN_STATES.CANCELLED,
]

// Run State Filters
export const STATE_FILTERS = ['ALL', ...Object.values(RUN_STATES)]

// Run State Colors
export const STATE_COLORS = {
  [RUN_STATES.PENDING]: {
    bg: 'bg-slate-700',
    text: 'text-slate-300',
    border: 'border-slate-600',
  },
  [RUN_STATES.RUNNING]: {
    bg: 'bg-blue-700',
    text: 'text-blue-100',
    border: 'border-blue-600',
  },
  [RUN_STATES.SUCCESS]: {
    bg: 'bg-green-700',
    text: 'text-green-100',
    border: 'border-green-600',
  },
  [RUN_STATES.FAILED]: {
    bg: 'bg-red-700',
    text: 'text-red-100',
    border: 'border-red-600',
  },
  [RUN_STATES.CANCELLED]: {
    bg: 'bg-orange-700',
    text: 'text-orange-100',
    border: 'border-orange-600',
  },
}

// Log Levels
export const LOG_LEVELS = {
  DEBUG: 'DEBUG',
  INFO: 'INFO',
  WARNING: 'WARNING',
  ERROR: 'ERROR',
}

// Routes
export const ROUTES = {
  HOME: '/',
  RUNS: '/runs',
  RUN_DETAIL: '/runs/:runId',
  PIPELINES: '/pipelines',
  PIPELINE_DETAIL: '/pipelines/:pipelineId',
}
