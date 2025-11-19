/**
 * Reusable Badge component
 */

const variants = {
  default: 'bg-slate-700 text-slate-200',
  primary: 'bg-tauro-700 text-tauro-100',
  success: 'bg-green-700 text-green-100',
  warning: 'bg-orange-700 text-orange-100',
  danger: 'bg-red-700 text-red-100',
  info: 'bg-blue-700 text-blue-100',
}

const sizes = {
  sm: 'text-xs px-2 py-0.5',
  md: 'text-sm px-2.5 py-1',
  lg: 'text-base px-3 py-1.5',
}

export default function Badge({ 
  children, 
  variant = 'default',
  size = 'sm',
  className = '',
}) {
  const baseStyles = 'inline-flex items-center font-semibold rounded-full'
  
  return (
    <span className={`${baseStyles} ${variants[variant]} ${sizes[size]} ${className}`}>
      {children}
    </span>
  )
}
