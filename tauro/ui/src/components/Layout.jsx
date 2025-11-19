import { Link, useLocation } from 'react-router-dom'
import { Activity, List, GitBranch } from 'lucide-react'
import { APP_INFO } from '../config/constants'

export default function Layout({ children }) {
  const location = useLocation()

  const isActive = (path) => {
    return location.pathname.startsWith(path)
  }

  const navItems = [
    { path: '/runs', label: 'Runs', icon: List },
    { path: '/pipelines', label: 'Pipelines', icon: GitBranch },
  ]

  return (
    <div className="min-h-screen bg-slate-900 text-slate-100">
      {/* Header */}
      <header className="bg-slate-800 border-b border-slate-700">
        <div className="container mx-auto px-4 py-4">
          <div className="flex items-center justify-between">
            <Link to="/" className="flex items-center gap-3 hover:opacity-80 transition">
              <Activity className="w-8 h-8 text-tauro-500" />
              <div>
                <h1 className="text-2xl font-bold">{APP_INFO.NAME}</h1>
                <span className="text-xs text-slate-400 font-mono">v{APP_INFO.VERSION}</span>
              </div>
            </Link>

            {/* Navigation */}
            <nav className="flex gap-2" role="navigation" aria-label="Main navigation">
              {navItems.map(({ path, label, icon: Icon }) => (
                <Link
                  key={path}
                  to={path}
                  className={`flex items-center gap-2 px-4 py-2 rounded transition ${
                    isActive(path)
                      ? 'bg-tauro-600 text-white'
                      : 'text-slate-300 hover:bg-slate-700'
                  }`}
                  aria-current={isActive(path) ? 'page' : undefined}
                >
                  <Icon className="w-4 h-4" />
                  {label}
                </Link>
              ))}
            </nav>
          </div>
        </div>
      </header>

      {/* Main content */}
      <main className="container mx-auto px-4 py-8">
        {children}
      </main>

      {/* Footer */}
      <footer className="border-t border-slate-800 mt-12">
        <div className="container mx-auto px-4 py-6 text-center text-sm text-slate-500">
          © {new Date().getFullYear()} {APP_INFO.NAME}. All rights reserved.
        </div>
      </footer>
    </div>
  )
}
