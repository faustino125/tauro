# 🎨 Tauro UI

> **Modern React interface for the Tauro data pipeline & orchestration platform**

[![React](https://img.shields.io/badge/React-18.3-61dafb?logo=react)](https://reactjs.org/)
[![Vite](https://img.shields.io/badge/Vite-5.4-646cff?logo=vite)](https://vitejs.dev/)
[![TailwindCSS](https://img.shields.io/badge/Tailwind-3.4-38bdf8?logo=tailwindcss)](https://tailwindcss.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](../../LICENSE)

**Tauro UI** is a modern, responsive web interface built with React 18 that provides real-time monitoring, visualization, and management of data pipelines and orchestration workflows.

---

## ✨ Features

### 🎯 Core Capabilities
- **📊 Dashboard** - Real-time overview of pipeline health, active runs, and system metrics
- **🔄 Pipeline Management** - List, view, and trigger pipeline executions
- **📈 Run Monitoring** - Track pipeline execution status, progress, and timing
- **🌳 DAG Visualization** - Interactive graph view of pipeline dependencies and task flow
- **📝 Real-time Logs** - Live log streaming via Server-Sent Events (SSE)
- **⏰ Schedule Management** - View and manage automated pipeline schedules
- **🔍 Search & Filter** - Advanced filtering by state, project, pipeline, and date range

### 🎨 User Experience
- **⚡ Fast & Responsive** - Built with Vite for lightning-fast hot module replacement
- **📱 Mobile-Friendly** - Responsive design works on all screen sizes
- **🌙 Dark Theme** - Modern dark interface optimized for long sessions
- **♿ Accessible** - ARIA labels and keyboard navigation support
- **🔄 Auto-Refresh** - Automatic data polling for live updates

---

## 🛠️ Tech Stack

| Layer            | Technology                       | Version | Purpose                              |
|------------------|----------------------------------|---------|--------------------------------------|
| **Build Tool**   | Vite                             | 5.4     | Ultra-fast dev server & bundling     |
| **Framework**    | React                            | 18.3    | Component-based UI library           |
| **Routing**      | React Router                     | 6.26    | Client-side navigation               |
| **Data Fetching**| TanStack Query (React Query)     | 5.56    | Server state management & caching    |
| **Visualization**| @xyflow/react (React Flow)       | 12.3    | Interactive DAG/graph rendering      |
| **Styling**      | Tailwind CSS                     | 3.4     | Utility-first CSS framework          |
| **Icons**        | Lucide React                     | 0.446   | Beautiful & consistent icon set      |
| **Date Utils**   | date-fns                         | 4.1     | Modern date formatting & manipulation|
| **Dev Tools**    | React Query Devtools             | 5.56    | Query inspector & debugger           |

---

## 🚀 Quick Start

### Prerequisites
- **Node.js** >= 16.x
- **npm** >= 8.x (or **yarn** / **pnpm**)
- **Tauro API** running on `http://localhost:8000`

### Installation

```bash
# Navigate to UI directory
cd tauro/ui

# Install dependencies
npm install

# Start development server
npm run dev
```

The UI will be available at **http://localhost:3000**

API requests are automatically proxied to the FastAPI backend on port 8000 (configurable in `vite.config.js`).

### Development Commands

```bash
# Start dev server with hot reload
npm run dev

# Build for production
npm run build

# Preview production build locally
npm run preview

# Lint code
npm run lint

# Format code
npm run format
```

---

## 📁 Project Structure

```
tauro/ui/
├── src/
│   ├── api/              # API client & request handlers
│   │   ├── client.js     # Centralized API client with deduplication
│   │   └── endpoints.js  # API endpoint definitions
│   ├── components/       # Reusable UI components
│   │   ├── Layout.jsx    # Main app layout & navigation
│   │   ├── RunCard.jsx   # Run status card component
│   │   ├── DAGViewer.jsx # Pipeline DAG visualization
│   │   ├── LogsViewer.jsx # Real-time log streaming
│   │   ├── ErrorBoundary.jsx # Error handling wrapper
│   │   ├── PipelineEditor/ # Visual pipeline editor (WIP)
│   │   └── ui/           # Base UI components (buttons, cards, etc.)
│   ├── pages/            # Route-level page components
│   │   ├── DashboardPage.jsx     # Main dashboard
│   │   ├── PipelinesPage.jsx     # Pipeline listing
│   │   ├── PipelineDetailPage.jsx # Single pipeline view
│   │   ├── RunsPage.jsx          # Run history & monitoring
│   │   ├── RunDetailPage.jsx     # Single run details
│   │   └── SchedulesPage.jsx     # Schedule management
│   ├── hooks/            # Custom React hooks
│   │   ├── useRuns.js    # Fetch & manage runs
│   │   ├── usePipelines.js # Fetch & manage pipelines
│   │   └── useWebSocket.js # WebSocket connection (future)
│   ├── config/           # Configuration & constants
│   │   └── constants.js  # API config, feature flags
│   ├── utils/            # Utility functions
│   │   ├── format.js     # Date/number formatting
│   │   ├── errors.js     # Error handling utilities
│   │   └── validation.js # Input validation
│   ├── App.jsx           # Root application component
│   ├── main.jsx          # Application entry point
│   └── index.css         # Global styles & Tailwind imports
├── public/               # Static assets
├── index.html            # HTML template
├── vite.config.js        # Vite configuration
├── tailwind.config.js    # Tailwind CSS configuration
├── postcss.config.js     # PostCSS configuration
├── package.json          # Dependencies & scripts
└── README.md             # This file
```

---

## 🔌 API Client Usage

All network calls go through the centralized API client (`src/api/client.js`) which provides:

- ✅ **Request deduplication** - Cancels duplicate in-flight requests
- ✅ **Automatic cancellation** - Uses AbortController for cleanup
- ✅ **Unified error handling** - Consistent error format across all endpoints
- ✅ **Manual timeouts** - Configurable request timeout (default 30s)
- ✅ **Smart retries** - Only retries network/5xx errors (not 4xx)

### Example Usage

```javascript
import { apiClient } from './api/client'

// List all pipelines for a project
const pipelines = await apiClient.listPipelines('project-123')

// Get a single run with details
const run = await apiClient.getRun('run-456')

// Trigger a pipeline execution
const newRun = await apiClient.runPipeline('project-123', 'pipeline-789', {
  params: { start_date: '2025-01-01' },
  tags: { env: 'production' }
})

// Stream logs in real-time (SSE)
const eventSource = apiClient.streamLogs(
  'run-456',
  (log) => console.log(log),      // onLog callback
  (error) => console.error(error)  // onError callback
)

// Stop streaming
eventSource.close()
```

### API Response Format

List endpoints return a consistent structure:

```json
{
  "status": "success",
  "data": [
    { "id": "run-1", "state": "RUNNING", ... },
    { "id": "run-2", "state": "SUCCESS", ... }
  ],
  "pagination": {
    "total": 120,
    "limit": 50,
    "offset": 0,
    "has_next": true,
    "has_prev": false
  }
}
```

Error responses:

```json
{
  "status": "error",
  "error": {
    "code": "RUN_NOT_FOUND",
    "message": "Run with ID 'run-123' not found",
    "details": { ... }
  }
}
```

---

## 🎣 React Query Conventions

### Configuration

```javascript
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      staleTime: 5000,           // 5 seconds
      gcTime: 300000,            // 5 minutes (cache time)
      retry: (failureCount, error) => {
        // Don't retry 4xx errors, retry 5xx up to 3 times
        if (error?.status >= 400 && error?.status < 500) return false
        return failureCount < 3
      }
    },
    mutations: {
      retry: false  // Don't retry mutations
    }
  }
})
```

### Usage Patterns

```javascript
// Query with auto-refresh
const { data, isLoading, error, refetch } = useQuery({
  queryKey: ['runs', { state: 'RUNNING' }],
  queryFn: () => apiClient.listRuns({ state: 'RUNNING' }),
  refetchInterval: 5000  // Poll every 5 seconds
})

// Mutation with optimistic updates
const mutation = useMutation({
  mutationFn: (runId) => apiClient.cancelRun(runId),
  onSuccess: () => {
    queryClient.invalidateQueries(['runs'])
  }
})
```

---

## 🌍 Environment Variables

Configure via `.env` file in the UI directory:

| Variable | Description | Default | Example |
|----------|-------------|---------|---------|
| `VITE_API_BASE_URL` | Base path for API requests | `/api/v1` | `https://api.example.com/api/v1` |
| `VITE_API_TIMEOUT` | Request timeout (ms) | `30000` | `60000` |
| `VITE_DEFAULT_PAGE_SIZE` | Default pagination size | `50` | `25` |
| `VITE_MAX_PAGE_SIZE` | Maximum items per page | `100` | `200` |
| `VITE_ENABLE_LOGS_STREAMING` | Enable SSE log streaming | `true` | `false` |
| `VITE_ENABLE_DEVTOOLS` | Show React Query DevTools | `true` (dev only) | `false` |
| `VITE_POLLING_INTERVAL` | Auto-refresh interval (ms) | `30000` | `10000` |

### Example `.env` file

```bash
VITE_API_BASE_URL=/api/v1
VITE_API_TIMEOUT=30000
VITE_DEFAULT_PAGE_SIZE=50
VITE_ENABLE_LOGS_STREAMING=true
VITE_POLLING_INTERVAL=30000
```

---

## 🏗️ Production Build

### Build Process

```bash
# Create optimized production build
npm run build

# Output: dist/ directory
```

Build artifacts are written to `dist/`. The FastAPI backend automatically serves the UI at `/ui` if it finds the `dist/` folder.

### Build Optimization

- **Code Splitting** - Automatic route-based splitting
- **Tree Shaking** - Removes unused code
- **Minification** - Compresses JS/CSS
- **Asset Hashing** - Cache-busting for static files
- **Compression** - gzip/brotli ready

### Deployment

The built UI can be:

1. **Served by FastAPI** - Backend serves static files from `tauro/ui/dist/`
2. **Deployed separately** - CDN/static hosting with API proxy
3. **Containerized** - Included in Docker image with backend

#### Docker Integration

```dockerfile
# Multi-stage build
FROM node:18 AS ui-builder
WORKDIR /app/ui
COPY tauro/ui/package*.json ./
RUN npm ci
COPY tauro/ui/ ./
RUN npm run build

FROM python:3.10
WORKDIR /app
# Copy UI build
COPY --from=ui-builder /app/ui/dist /app/tauro/ui/dist
# Backend setup continues...
```

---

## 🎨 Component Library

### Base Components (`src/components/ui/`)

```javascript
import { Button, Card, Alert, Spinner, Badge, EmptyState } from './components/ui'

// Button variants
<Button variant="primary" size="lg" icon={PlayIcon}>
  Run Pipeline
</Button>

// Status badges
<Badge variant="success">SUCCESS</Badge>
<Badge variant="error">FAILED</Badge>
<Badge variant="warning">PENDING</Badge>

// Loading state
<Spinner size="lg" text="Loading pipelines..." />

// Empty state
<EmptyState 
  icon={InboxIcon}
  title="No runs found"
  description="Start a pipeline to see runs here"
/>
```

### Feature Components

- **`<DAGViewer />`** - Interactive pipeline DAG with zoom/pan
- **`<LogsViewer />`** - Real-time log streaming with filtering
- **`<RunCard />`** - Compact run status display
- **`<PipelineEditor />`** - Visual pipeline builder (WIP)

---

## 🔒 Security Features

### Current Implementation
- ✅ **CORS Protection** - Explicit origin allowlist (no wildcards)
- ✅ **XSS Prevention** - React auto-escaping + DOMPurify for HTML
- ✅ **CSRF Tokens** - Included in mutation requests
- ✅ **Secure Headers** - Content Security Policy (CSP)
- ✅ **Request Validation** - Input sanitization before API calls

### Planned Enhancements
- 🔄 **JWT Authentication** - Token-based auth with refresh
- 🔄 **RBAC Integration** - Role-based access control
- 🔄 **Audit Logging** - Track user actions
- 🔄 **Rate Limiting** - Client-side throttling

---

## 🐛 Error Handling

### Error Boundaries

All routes are wrapped in `<ErrorBoundary>` to catch React errors:

```javascript
<ErrorBoundary>
  <Routes>
    <Route path="/dashboard" element={<DashboardPage />} />
    {/* ... */}
  </Routes>
</ErrorBoundary>
```

### API Errors

All API errors are wrapped in a custom `APIError` class:

```javascript
class APIError extends Error {
  constructor(message, status, code, details) {
    super(message)
    this.status = status      // HTTP status code (e.g., 404)
    this.code = code          // Error code (e.g., 'RUN_NOT_FOUND')
    this.details = details    // Additional error context
  }
}
```

### Error Display

```javascript
import { Alert } from './components/ui'
import { getErrorMessage } from './utils/errors'

{error && (
  <Alert variant="error" title="Error loading runs">
    {getErrorMessage(error)}
  </Alert>
)}
```

---

## 🚀 Performance Optimization

### Current Optimizations
- ✅ **Request Deduplication** - Cancels duplicate in-flight requests
- ✅ **Query Caching** - 5-minute cache for static data
- ✅ **Lazy Loading** - Route-based code splitting (planned)
- ✅ **Memoization** - React.memo for expensive components
- ✅ **Virtual Scrolling** - For large log lists (planned)

### Performance Metrics
- **First Contentful Paint (FCP)** - < 1s
- **Time to Interactive (TTI)** - < 2s
- **Lighthouse Score** - 90+ (Performance)

### Monitoring
```javascript
// React Query DevTools (dev mode only)
import { ReactQueryDevtools } from '@tanstack/react-query-devtools'

{import.meta.env.DEV && <ReactQueryDevtools initialIsOpen={false} />}
```

---

## 🔍 Troubleshooting

### Common Issues

| Symptom | Possible Cause | Solution |
|---------|----------------|----------|
| **Blank page** | API unreachable | Verify backend on port 8000, check proxy config |
| **Logs not streaming** | SSE endpoint mismatch | Confirm `/logs/runs/{id}/stream-sse` exists |
| **CORS errors** | Origin not allowed | Add origin to backend `CORS_ORIGINS` |
| **Pagination broken** | API version mismatch | Ensure API returns `pagination.total` |
| **Requests timeout** | Slow backend | Increase `VITE_API_TIMEOUT` or optimize API |
| **Hot reload fails** | Port conflict | Change port in `vite.config.js` |
| **Build fails** | Dependencies outdated | Run `npm install` and `npm audit fix` |

### Debug Mode

Enable verbose logging:

```javascript
// In src/api/client.js
const DEBUG = true

if (DEBUG) {
  console.log('API Request:', method, url, options)
  console.log('API Response:', response)
}
```

### Network Inspection

Use browser DevTools:
1. Open **Network** tab
2. Filter by **Fetch/XHR**
3. Check request/response payloads
4. Verify CORS headers

---

## 📊 Feature Roadmap

### ✅ Released (v0.1.0)
- Dashboard with system overview
- Pipeline listing and detail views
- Run monitoring and history
- DAG visualization (React Flow)
- Real-time log streaming (SSE)
- Run filtering and search
- Responsive mobile layout

### 🚧 In Progress (v0.2.0)
- [ ] Parameterized pipeline triggers
- [ ] Visual pipeline editor
- [ ] Schedule management UI
- [ ] Advanced metrics dashboard
- [ ] Run comparison view

### 📅 Planned (v0.3.0+)
- [ ] WebSocket real-time updates
- [ ] Dark/light theme toggle
- [ ] Advanced search with tags
- [ ] Pipeline templates library
- [ ] Notification center
- [ ] User preferences & settings
- [ ] Export runs to CSV/JSON
- [ ] Pipeline version history
- [ ] Role-based access control (RBAC)
- [ ] Multi-project workspace

---

## 🧪 Testing

### Unit Tests (Planned)

```bash
# Run tests
npm test

# With coverage
npm test -- --coverage

# Watch mode
npm test -- --watch
```

### E2E Tests (Planned)

Using Playwright or Cypress:

```bash
# Run E2E tests
npm run test:e2e

# Interactive mode
npm run test:e2e:ui
```

### Testing Strategy
- **Unit Tests** - Component logic, utilities, hooks
- **Integration Tests** - API client, React Query interactions
- **E2E Tests** - Critical user flows (login, run pipeline, view logs)
- **Visual Regression** - Screenshot comparison (Percy/Chromatic)

---

## 🤝 Contributing

We welcome contributions! Please follow these guidelines:

### Code Style
- Use **2 spaces** for indentation
- Follow **Airbnb JavaScript Style Guide**
- Run `npm run format` before committing
- Use **semantic commit messages**

### Commit Convention
```bash
feat: add pipeline comparison view
fix: resolve log streaming connection issue
docs: update API client documentation
style: format code with Prettier
refactor: extract common button component
test: add unit tests for date formatting
chore: upgrade dependencies
```

### Pull Request Process
1. Fork the repository
2. Create a feature branch (`git checkout -b feat/amazing-feature`)
3. Make your changes
4. Run tests and linting
5. Commit with semantic messages
6. Push to your fork
7. Open a Pull Request with:
   - Clear description of changes
   - Screenshots for UI changes
   - Link to related issue (if any)

### Component Guidelines
- Keep components **small and focused** (< 200 lines)
- Extract reusable logic into **custom hooks**
- Use **TypeScript** for complex components (future)
- Add **PropTypes** or TypeScript types
- Include **JSDoc** comments for complex logic

---

## 📚 Additional Resources

### Documentation
- [React Documentation](https://react.dev/)
- [Vite Guide](https://vitejs.dev/guide/)
- [TanStack Query](https://tanstack.com/query/latest)
- [Tailwind CSS](https://tailwindcss.com/docs)
- [React Flow](https://reactflow.dev/learn)

### Tauro Project
- [Tauro API Documentation](../api/README.md)
- [Architecture Overview](../../docs/ARCHITECTURE.md)
- [Contributing Guide](../../CONTRIBUTING.md)
- [Changelog](../../CHANGELOG.md)

---

## 📄 License

MIT License - See [LICENSE](../../LICENSE) file for details.

---

## 👥 Team

**Maintainer:** Faustino Lopez Ramos ([@faustino125](https://github.com/faustino125))

**Contributors:**
- Your name could be here! See [Contributing](#-contributing)

---

## 💬 Support

- **Issues:** [GitHub Issues](https://github.com/faustino125/tauro/issues)
- **Discussions:** [GitHub Discussions](https://github.com/faustino125/tauro/discussions)
- **Email:** faustinolopezramos@gmail.com

---

<div align="center">

**Made with ❤️ using React & Vite**

⭐ Star us on GitHub if you find this useful!

</div>
