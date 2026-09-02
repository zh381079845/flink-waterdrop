// 设计器相关API

/**
 * 保存流程配置
 * @param {Object} flowData - 流程配置数据
 * @returns {Promise}
 */
export function saveFlowConfiguration(flowData) {
  // 这里应该是实际的API调用
  return new Promise((resolve) => {
    console.log('保存流程配置:', flowData)
    setTimeout(() => {
      resolve({ success: true, message: '流程配置保存成功' })
    }, 500)
  })
}

/**
 * 导出流程配置为JSON
 * @param {Object} flowData - 流程配置数据
 * @returns {Promise}
 */
export function exportFlowConfiguration(flowData) {
  // 这里应该是实际的API调用
  return new Promise((resolve) => {
    console.log('导出流程配置:', flowData)
    setTimeout(() => {
      const jsonData = JSON.stringify(flowData, null, 2)
      resolve({ success: true, data: jsonData, message: '流程配置导出成功' })
    }, 500)
  })
}

/**
 * 获取节点类型列表
 * @returns {Promise}
 */
export function getNodeTypes() {
  // 这里应该是实际的API调用
  return new Promise((resolve) => {
    setTimeout(() => {
      resolve({
        success: true,
        data: [
          { type: 'source', name: '数据源(Source)', icon: '📥' },
          { type: 'transform', name: '转换(Transform)', icon: '🔄' },
          { type: 'sink', name: '输出(Sink)', icon: '📤' }
        ]
      })
    }, 300)
  })
}

/**
 * 获取默认配置
 * @param {string} nodeType - 节点类型
 * @returns {Promise}
 */
export function getDefaultConfig(nodeType) {
  // 这里应该是实际的API调用
  return new Promise((resolve) => {
    setTimeout(() => {
      let config = {}
      switch (nodeType) {
        case 'source':
          config = {
            type: '',
            host: '',
            port: '',
            database: '',
            username: '',
            password: ''
          }
          break
        case 'transform':
          config = {
            operation: '',
            field: '',
            expression: '',
            condition: ''
          }
          break
        case 'sink':
          config = {
            type: '',
            path: '',
            format: '',
            host: '',
            port: ''
          }
          break
        default:
          config = {}
      }
      resolve({ success: true, data: config })
    }, 300)
  })
}