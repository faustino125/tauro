/**
 * Reusable Empty State component
 */

export default function EmptyState({ 
  icon: Icon,
  title,
  description,
  action,
  className = '',
}) {
  return (
    <div className={`text-center py-12 ${className}`}>
      {Icon && (
        <Icon className="w-12 h-12 text-slate-600 mx-auto mb-4" />
      )}
      <h3 className="text-lg font-semibold text-slate-300 mb-2">
        {title}
      </h3>
      {description && (
        <p className="text-sm text-slate-400 mb-4 max-w-md mx-auto">
          {description}
        </p>
      )}
      {action && (
        <div className="mt-6">
          {action}
        </div>
      )}
    </div>
  )
}
