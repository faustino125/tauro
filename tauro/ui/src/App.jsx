import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { ReactQueryDevtools } from '@tanstack/react-query-devtools'
import Layout from './components/Layout'
import PipelinesPage from './pages/PipelinesPage'
import PipelineDetailPage from './pages/PipelineDetailPage'
import RunsPage from './pages/RunsPage'
import RunDetailPage from './pages/RunDetailPage'
import ErrorBoundary from './components/ErrorBoundary'
import { API_CONFIG } from './config/constants'

// Create React Query client with smart retry logic
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      // Smart retry: don't retry 4xx errors, retry up to 3 times for network errors
      retry: (failureCount, error) => {
        if (error?.status >= 400 && error?.status < 500) return false
        return failureCount < 3
      },
      staleTime: 5000,
      gcTime: 300000, // 5 minutes (formerly cacheTime)
    },
    mutations: {
      retry: false, // Don't retry mutations by default
    },
  },
})

function App() {
  return (
    <ErrorBoundary>
      <QueryClientProvider client={queryClient}>
        <Router>
          <Layout>
            <Routes>
              <Route path="/" element={<Navigate to="/runs" replace />} />
              <Route path="/runs" element={<RunsPage />} />
              <Route path="/runs/:runId" element={<RunDetailPage />} />
              <Route path="/pipelines" element={<PipelinesPage />} />
              <Route path="/pipelines/:pipelineId" element={<PipelineDetailPage />} />
              <Route path="*" element={<Navigate to="/runs" replace />} />
            </Routes>
          </Layout>
        </Router>
        {/* Only show devtools in development */}
        {import.meta.env.DEV && <ReactQueryDevtools initialIsOpen={false} />}
      </QueryClientProvider>
    </ErrorBoundary>
  )
}

export default App
