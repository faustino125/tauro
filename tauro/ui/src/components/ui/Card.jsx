/**
 * Reusable Card component
 */

export default function Card({ 
  children, 
  className = '',
  padding = 'p-6',
  hover = false,
  onClick,
}) {
  const baseStyles = 'bg-slate-800 border border-slate-700 rounded-lg'
  const hoverStyles = hover ? 'hover:border-slate-600 hover:shadow-lg transition-all cursor-pointer' : ''
  
  return (
    <div 
      className={`${baseStyles} ${hoverStyles} ${padding} ${className}`}
      onClick={onClick}
      role={onClick ? 'button' : undefined}
      tabIndex={onClick ? 0 : undefined}
    >
      {children}
    </div>
  )
}

export function CardHeader({ children, className = '' }) {
  return (
    <div className={`mb-4 ${className}`}>
      {children}
    </div>
  )
}

export function CardTitle({ children, className = '' }) {
  return (
    <h3 className={`text-lg font-semibold ${className}`}>
      {children}
    </h3>
  )
}

export function CardContent({ children, className = '' }) {
  return (
    <div className={className}>
      {children}
    </div>
  )
}

export function CardFooter({ children, className = '' }) {
  return (
    <div className={`mt-4 pt-4 border-t border-slate-700 ${className}`}>
      {children}
    </div>
  )
}
