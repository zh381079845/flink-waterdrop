<template>
  <div class="node-panel">
    <h3>组件库</h3>
    <div class="node-types">
      <div 
        class="node-type-item" 
        v-for="nodeType in nodeTypes" 
        :key="nodeType.type"
        draggable="true"
        @dragstart="onDragStart($event, nodeType)"
      >
        <span class="node-icon">{{ nodeType.icon }}</span>
        <span class="node-name">{{ nodeType.name }}</span>
      </div>
    </div>
  </div>
</template>

<script>
import { NODE_TYPE_DETAILS } from '@/utils/nodeTypes'

export default {
  name: 'NodePanel',
  data() {
    return {
      nodeTypes: NODE_TYPE_DETAILS
    }
  },
  methods: {
    onDragStart(event, nodeType) {
      event.dataTransfer.setData('nodeType', JSON.stringify(nodeType))
    }
  }
}
</script>

<style scoped>
.node-panel {
  width: 250px;
  background-color: #f5f5f5;
  border-right: 1px solid #ddd;
  padding: 15px;
  height: 100%;
}

.node-panel h3 {
  margin-top: 0;
  color: #333;
  border-bottom: 1px solid #ddd;
  padding-bottom: 10px;
}

.node-types {
  margin-top: 15px;
}

.node-type-item {
  display: flex;
  align-items: center;
  padding: 10px;
  margin-bottom: 10px;
  background-color: white;
  border: 1px solid #ddd;
  border-radius: 4px;
  cursor: move;
  transition: all 0.3s;
}

.node-type-item:hover {
  background-color: #e6f7ff;
  border-color: #1890ff;
  box-shadow: 0 2px 8px rgba(0,0,0,0.15);
}

.node-icon {
  font-size: 20px;
  margin-right: 10px;
}

.node-name {
  font-weight: 500;
  color: #333;
}
</style>