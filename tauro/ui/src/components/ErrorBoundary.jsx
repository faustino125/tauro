/**
 * Error Boundary Component
 * Catches React errors and displays fallback UI
 */
import { Component } from 'react'
import { AlertCircle } from 'lucide-react'
import { Button } from './ui'

class ErrorBoundary extends Component {
  constructor(props) {
    super(props)
    this.state = { hasError: false, error: null, errorInfo: null }
  }

  static getDerivedStateFromError(error) {
    return { hasError: true }
  }

  componentDidCatch(error, errorInfo) {
    console.error('ErrorBoundary caught an error:', error, errorInfo)
    this.setState({
      error,
      errorInfo,
    })
  }

  handleReset = () => {
    this.setState({ hasError: false, error: null, errorInfo: null })
    window.location.href = '/'
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="min-h-screen bg-slate-900 flex items-center justify-center p-4">
          <div className="bg-slate-800 border-2 border-red-700 rounded-lg p-8 max-w-lg w-full">
            <div className="flex items-center gap-3 mb-4">
              <AlertCircle className="w-8 h-8 text-red-400 flex-shrink-0" />
              <h1 className="text-2xl font-bold text-red-400">Something went wrong</h1>
            </div>
            
            <p className="text-slate-300 text-sm mb-4">
              {this.state.error?.message || 'An unexpected error occurred'}
            </p>

            {import.meta.env.DEV && this.state.errorInfo && (
              <details className="mb-6">
                <summary className="text-sm text-slate-400 cursor-pointer hover:text-slate-300 mb-2">
                  Error details (dev only)
                </summary>
                <pre className="text-xs bg-slate-900 p-3 rounded overflow-auto max-h-48 text-red-300">
                  {this.state.error?.stack}
                </pre>
              </details>
            )}

            <Button onClick={this.handleReset} className="w-full">
              Go Home
            </Button>
          </div>
        </div>
      )
    }

    return this.props.children
  }
}

export default ErrorBoundary
