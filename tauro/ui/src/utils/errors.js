/**
 * Error handling utilities
 */

/**
 * Custom error class for API errors
 */
export class APIError extends Error {
  constructor(message, status, code, details) {
    super(message)
    this.name = 'APIError'
    this.status = status
    this.code = code
    this.details = details
  }

  get isClientError() {
    return this.status >= 400 && this.status < 500
  }

  get isServerError() {
    return this.status >= 500
  }

  get isNetworkError() {
    return !this.status
  }
}

/**
 * Extract error message from various error types
 */
export function getErrorMessage(error) {
  if (!error) return 'An unknown error occurred'
  
  // Handle custom APIError
  if (error instanceof APIError) {
    return error.message
  }
  
  // Handle standard Error
  if (error instanceof Error) {
    return error.message
  }
  
  // Handle string error
  if (typeof error === 'string') {
    return error
  }
  
  // Handle object with message property
  if (error.message) {
    return error.message
  }
  
  // Handle object with detail property (FastAPI format)
  if (error.detail) {
    return typeof error.detail === 'string' 
      ? error.detail 
      : JSON.stringify(error.detail)
  }
  
  // Fallback
  return 'An unexpected error occurred'
}

/**
 * Check if error should trigger retry
 */
export function shouldRetry(error, attemptNumber = 0) {
  // Don't retry client errors (4xx)
  if (error instanceof APIError && error.isClientError) {
    return false
  }
  
  // Don't retry after max attempts
  if (attemptNumber >= 3) {
    return false
  }
  
  // Retry network errors and server errors
  return true
}

/**
 * Get user-friendly error message
 */
export function getUserFriendlyMessage(error) {
  if (error instanceof APIError) {
    switch (error.status) {
      case 400:
        return 'Invalid request. Please check your input.'
      case 401:
        return 'Authentication required. Please log in.'
      case 403:
        return 'You do not have permission to perform this action.'
      case 404:
        return 'The requested resource was not found.'
      case 409:
        return 'This action conflicts with existing data.'
      case 429:
        return 'Too many requests. Please try again later.'
      case 500:
        return 'Server error. Please try again later.'
      case 503:
        return 'Service unavailable. Please try again later.'
      default:
        return error.message
    }
  }
  
  return getErrorMessage(error)
}
