/**
 * Reusable Button component
 */
import { Loader2 } from 'lucide-react'

const variants = {
  primary: 'bg-tauro-600 hover:bg-tauro-700 text-white',
  secondary: 'bg-slate-700 hover:bg-slate-600 text-slate-100',
  danger: 'bg-red-600 hover:bg-red-700 text-white',
  ghost: 'bg-transparent hover:bg-slate-800 text-slate-300',
  outline: 'border-2 border-slate-600 hover:border-slate-500 bg-transparent text-slate-300',
}

const sizes = {
  sm: 'px-3 py-1.5 text-sm',
  md: 'px-4 py-2 text-base',
  lg: 'px-6 py-3 text-lg',
}

export default function Button({
  children,
  variant = 'primary',
  size = 'md',
  loading = false,
  disabled = false,
  icon: Icon,
  className = '',
  ...props
}) {
  const baseStyles = 'inline-flex items-center justify-center gap-2 rounded font-medium transition-colors focus:outline-none focus:ring-2 focus:ring-tauro-500 focus:ring-offset-2 focus:ring-offset-slate-900 disabled:opacity-50 disabled:cursor-not-allowed'
  
  return (
    <button
      className={`${baseStyles} ${variants[variant]} ${sizes[size]} ${className}`}
      disabled={disabled || loading}
      {...props}
    >
      {loading ? (
        <Loader2 className="w-4 h-4 animate-spin" />
      ) : Icon ? (
        <Icon className="w-4 h-4" />
      ) : null}
      {children}
    </button>
  )
}
