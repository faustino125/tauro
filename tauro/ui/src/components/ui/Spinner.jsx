/**
 * Reusable Spinner component
 */
import { Loader2 } from 'lucide-react'

export default function Spinner({ 
  size = 'md',
  className = '',
  text,
}) {
  const sizes = {
    sm: 'w-4 h-4',
    md: 'w-8 h-8',
    lg: 'w-12 h-12',
  }
  
  return (
    <div className={`flex flex-col items-center justify-center gap-2 ${className}`}>
      <Loader2 className={`${sizes[size]} animate-spin text-tauro-500`} />
      {text && (
        <p className="text-sm text-slate-400">{text}</p>
      )}
    </div>
  )
}
