#!/bin/bash
# Tauro Development Quick Start
# Inicia backend y frontend simultáneamente

set -e

echo "🚀 Starting Tauro Development Environment..."
echo ""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Trap para cleanup al salir
cleanup() {
    echo ""
    echo "🛑 Stopping Tauro services..."
    kill 0
}
trap cleanup EXIT

# Start Backend
echo "▶️  Starting Backend (port 8000)..."
cd "$SCRIPT_DIR"
python -m tauro.api.main &
BACKEND_PID=$!

# Wait for backend to start
sleep 3

# Check if UI directory exists
UI_PATH="$SCRIPT_DIR/tauro/ui"
if [ ! -d "$UI_PATH" ]; then
    echo "❌ UI directory not found at: $UI_PATH"
    exit 1
fi

cd "$UI_PATH"

# Install UI dependencies if needed
if [ ! -d "node_modules" ]; then
    echo "📦 Installing UI dependencies (first time only)..."
    npm install
fi

# Start Frontend
echo "▶️  Starting Frontend (port 3000)..."
npm run dev &
FRONTEND_PID=$!

echo ""
echo "✅ Tauro is running!"
echo ""
echo "📍 Access points:"
echo "   UI:   http://localhost:3000"
echo "   API:  http://localhost:8000"
echo "   Docs: http://localhost:8000/docs"
echo ""
echo "⌨️  Press Ctrl+C to stop all services"
echo ""

# Wait for both processes
wait
