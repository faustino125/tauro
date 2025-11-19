/**
 * Validation utilities
 */

/**
 * Validate UUID format
 */
export function isValidUUID(uuid) {
  if (!uuid) return false
  
  const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i
  return uuidRegex.test(uuid)
}

/**
 * Validate run ID (can be UUID or custom format)
 */
export function isValidRunId(runId) {
  if (!runId || typeof runId !== 'string') return false
  return runId.length > 0 && runId.length < 256
}

/**
 * Validate email
 */
export function isValidEmail(email) {
  if (!email) return false
  
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/
  return emailRegex.test(email)
}

/**
 * Validate JSON string
 */
export function isValidJSON(str) {
  try {
    JSON.parse(str)
    return true
  } catch {
    return false
  }
}

/**
 * Sanitize user input (basic XSS prevention)
 */
export function sanitizeInput(input) {
  if (!input || typeof input !== 'string') return input
  
  return input
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#x27;')
    .replace(/\//g, '&#x2F;')
}
