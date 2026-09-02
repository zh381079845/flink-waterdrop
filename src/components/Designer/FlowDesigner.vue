<template>
  <div class="flow-designer">
    <div class="designer-container">
      <!-- 节点面板 -->
      <div class="node-panel">
        <h3>组件库</h3>
        <div 
          class="node-item" 
          draggable="true"
          v-for="node in nodeTypes" 
          :key="node.type"
          @dragstart="onDragStart($event, node)"
        >
          {{ node.name }}
        </div>
      </div>
      
      <!-- 画布区域 -->
      <div class="canvas-area" @drop="onDrop" @dragover="onDragOver">
        <h3>设计区域</h3>
        <div class="canvas" ref="canvas">
          <div 
            v-for="node in nodes" 
            :key="node.id"
            class="node-on-canvas"
            :style="{ left: node.x + 'px', top: node.y + 'px' }"
          >
            <div class="node-header">{{ node.name }}</div>
            <div class="node-content">
              <button @click="configureNode(node)">配置</button>
            </div>
          </div>
          
          <!-- 连接线 -->
          <svg class="connections">
            <line 
              v-for="connection in connections" 
              :key="connection.id"
              :x1="connection.sourceX" 
              :y1="connection.sourceY" 
              :x2="connection.targetX" 
              :y2="connection.targetY"
              class="connection-line"
            />
          </svg>
        </div>
      </div>
      
      <!-- 配置面板 -->
      <div class="config-panel">
        <h3>配置面板</h3>
        <div v-if="selectedNode">
          <h4>{{ selectedNode.name }} 配置</h4>
          <div class="form-group" v-for="(value, key) in selectedNode.config" :key="key">
            <label>{{ key }}:</label>
            <input v-model="selectedNode.config[key]" />
          </div>
          <button @click="saveConfiguration">保存配置</button>
        </div>
        <div v-else>
          <p>请选择一个节点进行配置</p>
        </div>
      </div>
    </div>
    
    <!-- 控制按钮 -->
    <div class="actions">
      <button @click="saveFlow">保存流程</button>
      <button @click="exportFlow">导出配置</button>
    </div>
    
    <!-- JSON预览 -->
    <div class="json-preview" v-if="showJsonPreview">
      <h3>流程配置(JSON)</h3>
      <pre>{{ flowJson }}</pre>
    </div>
  </div>
</template>

<script>
import { NODE_TYPES } from '@/utils/nodeTypes'

export default {
  name: 'FlowDesigner',
  data() {
    return {
      nodeTypes: [
        { type: 'source', name: '数据源(Source)', icon: '📥' },
        { type: 'transform', name: '转换(Transform)', icon: '🔄' },
        { type: 'sink', name: '输出(Sink)', icon: '📤' }
      ],
      nodes: [],
      connections: [],
      selectedNode: null,
      showJsonPreview: false,
      flowJson: '',
      nodeIdCounter: 1,
      connectionIdCounter: 1
    }
  },
  methods: {
    onDragStart(event, node) {
      event.dataTransfer.setData('nodeType', JSON.stringify(node))
    },
    
    onDragOver(event) {
      event.preventDefault()
    },
    
    onDrop(event) {
      event.preventDefault()
      const nodeType = JSON.parse(event.dataTransfer.getData('nodeType'))
      const rect = this.$refs.canvas.getBoundingClientRect()
      const x = event.clientX - rect.left
      const y = event.clientY - rect.top
      
      const newNode = {
        id: this.nodeIdCounter++,
        type: nodeType.type,
        name: nodeType.name,
        x: x,
        y: y,
        config: this.getDefaultConfig(nodeType.type)
      }
      
      this.nodes.push(newNode)
    },
    
    getDefaultConfig(type) {
      switch (type) {
        case 'source':
          return {
            'type': '',
            'host': '',
            'port': '',
            'database': ''
          }
        case 'transform':
          return {
            'operation': '',
            'field': '',
            'expression': ''
          }
        case 'sink':
          return {
            'type': '',
            'path': '',
            'format': ''
          }
        default:
          return {}
      }
    },
    
    configureNode(node) {
      this.selectedNode = node
    },
    
    saveConfiguration() {
      // 保存配置到节点
      alert('配置已保存')
    },
    
    saveFlow() {
      // 保存整个流程
      const flowData = {
        nodes: this.nodes,
        connections: this.connections
      }
      
      this.flowJson = JSON.stringify(flowData, null, 2)
      this.showJsonPreview = true
    },
    
    exportFlow() {
      // 导出流程配置
      this.saveFlow()
      alert('流程配置已导出')
    }
  }
}
</script>

<style scoped>
.flow-designer {
  width: 100%;
  height: 100%;
}

.designer-container {
  display: flex;
  height: calc(100vh - 100px);
}

.node-panel {
  width: 200px;
  border-right: 1px solid #ccc;
  padding: 10px;
  background-color: #f5f5f5;
}

.node-item {
  padding: 10px;
  margin: 5px 0;
  background-color: white;
  border: 1px solid #ddd;
  cursor: move;
  border-radius: 4px;
}

.node-item:hover {
  background-color: #e6f7ff;
  border-color: #1890ff;
}

.canvas-area {
  flex: 1;
  padding: 10px;
  position: relative;
}

.canvas {
  width: 100%;
  height: 100%;
  position: relative;
  background-image: 
    linear-gradient(#f0f0f0 1px, transparent 1px),
    linear-gradient(90deg, #f0f0f0 1px, transparent 1px);
  background-size: 20px 20px;
  border: 1px solid #ddd;
}

.node-on-canvas {
  position: absolute;
  width: 150px;
  background-color: white;
  border: 2px solid #1890ff;
  border-radius: 4px;
  box-shadow: 0 2px 8px rgba(0,0,0,0.15);
}

.node-header {
  padding: 5px;
  background-color: #1890ff;
  color: white;
  font-weight: bold;
  border-top-left-radius: 2px;
  border-top-right-radius: 2px;
}

.node-content {
  padding: 10px;
}

.config-panel {
  width: 300px;
  border-left: 1px solid #ccc;
  padding: 10px;
  background-color: #fafafa;
}

.form-group {
  margin-bottom: 10px;
}

.form-group label {
  display: block;
  margin-bottom: 5px;
  font-weight: bold;
}

.form-group input {
  width: 100%;
  padding: 5px;
  border: 1px solid #ddd;
  border-radius: 4px;
}

.actions {
  padding: 10px;
  text-align: center;
  border-top: 1px solid #ccc;
}

.actions button {
  margin: 0 5px;
  padding: 8px 16px;
  background-color: #1890ff;
  color: white;
  border: none;
  border-radius: 4px;
  cursor: pointer;
}

.actions button:hover {
  background-color: #40a9ff;
}

.json-preview {
  padding: 10px;
  background-color: #f5f5f5;
  border-top: 1px solid #ccc;
}

.json-preview pre {
  background-color: #fff;
  padding: 10px;
  border-radius: 4px;
  overflow: auto;
  max-height: 200px;
}

.connections {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  pointer-events: none;
}

.connection-line {
  stroke: #1890ff;
  stroke-width: 2;
}
</style>