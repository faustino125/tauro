import { useEffect, useState, useRef } from 'react'
import { Terminal, Trash2, Download } from 'lucide-react'

const LOG_LEVEL_COLORS = {
  DEBUG: 'text-slate-400',
  INFO: 'text-blue-400',
  WARNING: 'text-yellow-400',
  ERROR: 'text-red-400',
}

export default function LogsViewer({ runId, streamLogs }) {
  const [logs, setLogs] = useState([])
  const [isStreaming, setIsStreaming] = useState(false)
  const [error, setError] = useState(null)
  const logsEndRef = useRef(null)
  const eventSourceRef = useRef(null)

  // Auto-scroll to bottom
  const scrollToBottom = () => {
    logsEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }

  useEffect(() => {
    scrollToBottom()
  }, [logs])

  // Start streaming
  useEffect(() => {
    if (!streamLogs || !runId) return

    setIsStreaming(true)
    setError(null)

    const eventSource = streamLogs(
      runId,
      (log) => {
        setLogs((prev) => [...prev, log])
      },
      (err) => {
        setError(err.message)
        setIsStreaming(false)
      }
    )

    eventSourceRef.current = eventSource

    return () => {
      eventSource?.close()
      setIsStreaming(false)
    }
  }, [runId, streamLogs])

  const clearLogs = () => {
    setLogs([])
  }

  const downloadLogs = () => {
    const content = logs.map(log => {
      const timestamp = log.timestamp || new Date().toISOString()
      const level = log.level || 'INFO'
      const message = log.message || JSON.stringify(log)
      return `[${timestamp}] ${level}: ${message}`
    }).join('\n')

    const blob = new Blob([content], { type: 'text/plain' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `tauro-run-${runId}-logs.txt`
    a.click()
    URL.revokeObjectURL(url)
  }

  return (
    <div className="bg-slate-900 border border-slate-700 rounded-lg overflow-hidden">
      {/* Header */}
      <div className="bg-slate-800 border-b border-slate-700 px-4 py-2 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Terminal className="w-4 h-4 text-tauro-500" />
          <span className="text-sm font-medium">Logs</span>
          {isStreaming && (
            <span className="flex items-center gap-1.5 text-xs text-green-400">
              <span className="w-2 h-2 bg-green-400 rounded-full animate-pulse" />
              Streaming
            </span>
          )}
        </div>

        <div className="flex items-center gap-2">
          <button
            onClick={downloadLogs}
            disabled={logs.length === 0}
            className="p-1.5 text-slate-400 hover:text-slate-200 disabled:opacity-50 disabled:cursor-not-allowed"
            title="Download logs"
          >
            <Download className="w-4 h-4" />
          </button>
          <button
            onClick={clearLogs}
            disabled={logs.length === 0}
            className="p-1.5 text-slate-400 hover:text-slate-200 disabled:opacity-50 disabled:cursor-not-allowed"
            title="Clear logs"
          >
            <Trash2 className="w-4 h-4" />
          </button>
        </div>
      </div>

      {/* Logs content */}
      <div className="h-96 overflow-y-auto p-4 font-mono text-sm">
        {error && (
          <div className="text-red-400 mb-2">
            Error: {error}
          </div>
        )}

        {logs.length === 0 && !error && (
          <div className="text-slate-500 text-center py-8">
            {isStreaming ? 'Waiting for logs...' : 'No logs available'}
          </div>
        )}

        {logs.map((log, index) => {
          const level = log.level || 'INFO'
          const color = LOG_LEVEL_COLORS[level] || LOG_LEVEL_COLORS.INFO
          const timestamp = log.timestamp ? new Date(log.timestamp).toLocaleTimeString() : ''
          const message = log.message || JSON.stringify(log)

          return (
            <div key={index} className="mb-1 leading-relaxed">
              <span className="text-slate-500">[{timestamp}]</span>{' '}
              <span className={`font-semibold ${color}`}>{level}</span>:{' '}
              <span className="text-slate-300">{message}</span>
            </div>
          )
        })}

        <div ref={logsEndRef} />
      </div>
    </div>
  )
}
