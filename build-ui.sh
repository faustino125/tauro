#!/bin/bash
# Build script para Tauro UI

set -e  # Exit on error

echo "🏗️  Building Tauro UI..."

# Change to UI directory
cd "$(dirname "$0")/tauro/ui"

# Install dependencies if needed
if [ ! -d "node_modules" ]; then
    echo "📦 Installing dependencies..."
    npm install
fi

# Build
echo "⚙️  Running production build..."
npm run build

echo "✅ Build complete! Output in: tauro/ui/dist/"
echo ""
echo "To serve from FastAPI, visit: http://localhost:8000/ui"
