// 节点类型定义
export const NODE_TYPES = {
  SOURCE: 'source',
  TRANSFORM: 'transform',
  SINK: 'sink'
}

export const NODE_TYPE_DETAILS = [
  {
    type: NODE_TYPES.SOURCE,
    name: '数据源(Source)',
    icon: '📥',
    description: '数据输入组件'
  },
  {
    type: NODE_TYPES.TRANSFORM,
    name: '转换(Transform)',
    icon: '🔄',
    description: '数据处理转换组件'
  },
  {
    type: NODE_TYPES.SINK,
    name: '输出(Sink)',
    icon: '📤',
    description: '数据输出组件'
  }
]

export const DEFAULT_CONFIGS = {
  [NODE_TYPES.SOURCE]: {
    type: '',
    host: '',
    port: '',
    database: '',
    username: '',
    password: ''
  },
  [NODE_TYPES.TRANSFORM]: {
    operation: '',
    field: '',
    expression: '',
    condition: ''
  },
  [NODE_TYPES.SINK]: {
    type: '',
    path: '',
    format: '',
    host: '',
    port: ''
  }
}