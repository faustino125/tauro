/**
 * Formatting utilities
 */
import { formatDistanceToNow, format as formatDate } from 'date-fns'

/**
 * Format a date with fallback
 */
export function formatDateTime(date, formatStr = 'PPpp') {
  if (!date) return 'N/A'
  
  try {
    const d = typeof date === 'string' ? new Date(date) : date
    if (isNaN(d.getTime())) return 'Invalid date'
    return formatDate(d, formatStr)
  } catch (error) {
    console.error('Date formatting error:', error)
    return 'Invalid date'
  }
}

/**
 * Format relative time (e.g., "2 hours ago")
 */
export function formatRelativeTime(date) {
  if (!date) return 'N/A'
  
  try {
    const d = typeof date === 'string' ? new Date(date) : date
    if (isNaN(d.getTime())) return 'Invalid date'
    return formatDistanceToNow(d, { addSuffix: true })
  } catch (error) {
    console.error('Relative time formatting error:', error)
    return 'Invalid date'
  }
}

/**
 * Format duration in seconds to human-readable format
 */
export function formatDuration(seconds) {
  if (seconds == null || isNaN(seconds)) return 'N/A'
  
  const hrs = Math.floor(seconds / 3600)
  const mins = Math.floor((seconds % 3600) / 60)
  const secs = Math.floor(seconds % 60)
  
  if (hrs > 0) {
    return `${hrs}h ${mins}m ${secs}s`
  } else if (mins > 0) {
    return `${mins}m ${secs}s`
  } else {
    return `${secs}s`
  }
}

/**
 * Calculate duration between two dates
 */
export function calculateDuration(startDate, endDate) {
  if (!startDate || !endDate) return null
  
  try {
    const start = typeof startDate === 'string' ? new Date(startDate) : startDate
    const end = typeof endDate === 'string' ? new Date(endDate) : endDate
    
    if (isNaN(start.getTime()) || isNaN(end.getTime())) return null
    
    return Math.round((end - start) / 1000)
  } catch (error) {
    console.error('Duration calculation error:', error)
    return null
  }
}

/**
 * Format file size
 */
export function formatBytes(bytes, decimals = 2) {
  if (bytes === 0) return '0 Bytes'
  if (!bytes) return 'N/A'
  
  const k = 1024
  const dm = decimals < 0 ? 0 : decimals
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB']
  
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  
  return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i]
}

/**
 * Truncate text with ellipsis
 */
export function truncate(text, maxLength = 50) {
  if (!text) return ''
  if (text.length <= maxLength) return text
  return text.substring(0, maxLength) + '...'
}

/**
 * Pluralize word based on count
 */
export function pluralize(count, singular, plural = null) {
  if (count === 1) return singular
  return plural || `${singular}s`
}
