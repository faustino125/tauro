# Tauro UI

Simple, functional UI for Tauro pipeline orchestration.

## Stack

- **React 18** + **Vite** - Fast, modern build
- **React Flow** - DAG visualization
- **TanStack Query** - Server state management
- **Tailwind CSS** - Utility-first styling
- **Lucide React** - Clean iconography

## Setup

```bash
cd ui
npm install
npm run dev
```

## Build for Production

```bash
npm run build
```

Output goes to `dist/` which FastAPI serves as static files.

## Architecture

```
src/
  components/        # Reusable components
    DAGViewer/       # Pipeline DAG visualization
    RunsList/        # Executions table
    LogsViewer/      # Real-time log terminal
  pages/             # Route pages
  api/               # API client
  hooks/             # Custom React hooks
  App.jsx            # Main app component
```

## API Integration

All API calls go through `src/api/client.js`:

```javascript
import { apiClient } from './api/client';

// List pipelines
const pipelines = await apiClient.listPipelines(projectId);

// Get run status
const run = await apiClient.getRun(runId);

// Stream logs (SSE)
const eventSource = apiClient.streamLogs(runId, (log) => {
  console.log(log);
});
```

## Features

### v0.1.0 (MVP)
- ✅ Pipeline DAG visualization
- ✅ Run list with status filtering
- ✅ Real-time log streaming
- ✅ Run details and metrics

### Future
- Run triggers with params
- Pipeline editor (visual)
- Scheduling management
- Metrics dashboard
