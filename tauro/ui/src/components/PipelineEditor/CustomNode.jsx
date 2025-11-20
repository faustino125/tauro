import React, { memo } from 'react';
import { Handle, Position } from '@xyflow/react';
import { Database, FileText, Settings, Play } from 'lucide-react';

const NodeIcon = ({ type }) => {
  switch (type) {
    case 'source': return <Database className="w-4 h-4" />;
    case 'sink': return <FileText className="w-4 h-4" />;
    case 'transform': return <Settings className="w-4 h-4" />;
    default: return <Play className="w-4 h-4" />;
  }
};

const CustomNode = ({ data, isConnectable }) => {
  const isSource = data.type === 'source';
  const isSink = data.type === 'sink';

  return (
    <div className="bg-slate-800 border-2 border-slate-600 rounded-lg min-w-[180px] shadow-lg hover:border-tauro-500 transition-colors">
      {/* Header */}
      <div className="bg-slate-900/50 p-2 border-b border-slate-700 rounded-t-lg flex items-center gap-2">
        <div className={`p-1 rounded ${
          isSource ? 'bg-blue-500/20 text-blue-400' :
          isSink ? 'bg-green-500/20 text-green-400' :
          'bg-purple-500/20 text-purple-400'
        }`}>
          <NodeIcon type={data.type} />
        </div>
        <div className="font-medium text-sm text-slate-200">{data.label}</div>
      </div>

      {/* Body */}
      <div className="p-3">
        <div className="text-xs text-slate-400 font-mono truncate">
          {data.implementation || 'No implementation'}
        </div>
      </div>

      {/* Handles */}
      {!isSource && (
        <Handle
          type="target"
          position={Position.Left}
          isConnectable={isConnectable}
          className="w-3 h-3 !bg-slate-400 !border-2 !border-slate-800"
        />
      )}
      {!isSink && (
        <Handle
          type="source"
          position={Position.Right}
          isConnectable={isConnectable}
          className="w-3 h-3 !bg-tauro-500 !border-2 !border-slate-800"
        />
      )}
    </div>
  );
};

export default memo(CustomNode);
