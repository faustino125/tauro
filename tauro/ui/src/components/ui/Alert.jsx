/**
 * Reusable Alert component
 */
import { AlertCircle, CheckCircle, Info, AlertTriangle } from 'lucide-react'

const variants = {
  error: {
    bg: 'bg-red-950/50',
    border: 'border-red-800',
    text: 'text-red-400',
    icon: AlertCircle,
  },
  success: {
    bg: 'bg-green-950/50',
    border: 'border-green-800',
    text: 'text-green-400',
    icon: CheckCircle,
  },
  warning: {
    bg: 'bg-orange-950/50',
    border: 'border-orange-800',
    text: 'text-orange-400',
    icon: AlertTriangle,
  },
  info: {
    bg: 'bg-blue-950/50',
    border: 'border-blue-800',
    text: 'text-blue-400',
    icon: Info,
  },
}

export default function Alert({ 
  variant = 'info',
  title,
  children,
  className = '',
}) {
  const config = variants[variant]
  const Icon = config.icon
  
  return (
    <div className={`${config.bg} border ${config.border} rounded-lg p-4 ${className}`}>
      <div className="flex gap-3">
        <Icon className={`w-5 h-5 ${config.text} flex-shrink-0 mt-0.5`} />
        <div className="flex-1">
          {title && (
            <h4 className={`font-semibold mb-1 ${config.text}`}>
              {title}
            </h4>
          )}
          <div className={`text-sm ${config.text}`}>
            {children}
          </div>
        </div>
      </div>
    </div>
  )
}
