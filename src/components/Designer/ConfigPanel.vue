<template>
  <div class="config-panel">
    <h3>配置面板</h3>
    <div class="config-content" v-if="selectedNode">
      <h4>{{ selectedNode.name }} 配置</h4>
      <form @submit.prevent="saveConfig">
        <div class="form-group" v-for="(value, key) in selectedNode.config" :key="key">
          <label>{{ formatLabel(key) }}:</label>
          <input 
            v-model="selectedNode.config[key]" 
            :type="getFieldType(key)"
            class="form-control"
          />
        </div>
        <div class="form-actions">
          <button type="submit" class="btn-save">保存配置</button>
          <button type="button" class="btn-cancel" @click="cancelConfig">取消</button>
        </div>
      </form>
    </div>
    <div class="no-selection" v-else>
      <p>请在画布中选择一个节点进行配置</p>
    </div>
  </div>
</template>

<script>
export default {
  name: 'ConfigPanel',
  props: {
    selectedNode: {
      type: Object,
      default: null
    }
  },
  methods: {
    formatLabel(key) {
      // 将驼峰命名转换为可读的标签
      return key.replace(/([A-Z])/g, ' $1')
                .replace(/^./, str => str.toUpperCase())
    },
    
    getFieldType(key) {
      // 根据字段名确定输入类型
      if (key.includes('password')) {
        return 'password'
      } else if (key.includes('port')) {
        return 'number'
      } else {
        return 'text'
      }
    },
    
    saveConfig() {
      this.$emit('save-config', this.selectedNode)
    },
    
    cancelConfig() {
      this.$emit('cancel-config')
    }
  }
}
</script>

<style scoped>
.config-panel {
  width: 300px;
  background-color: #fafafa;
  border-left: 1px solid #ddd;
  padding: 15px;
  height: 100%;
}

.config-panel h3 {
  margin-top: 0;
  color: #333;
  border-bottom: 1px solid #ddd;
  padding-bottom: 10px;
}

.config-content h4 {
  color: #555;
  margin: 15px 0;
}

.form-group {
  margin-bottom: 15px;
}

.form-group label {
  display: block;
  margin-bottom: 5px;
  font-weight: 500;
  color: #555;
}

.form-control {
  width: 100%;
  padding: 8px;
  border: 1px solid #ddd;
  border-radius: 4px;
  box-sizing: border-box;
}

.form-actions {
  margin-top: 20px;
  text-align: center;
}

.btn-save, .btn-cancel {
  padding: 8px 16px;
  margin: 0 5px;
  border: none;
  border-radius: 4px;
  cursor: pointer;
}

.btn-save {
  background-color: #1890ff;
  color: white;
}

.btn-save:hover {
  background-color: #40a9ff;
}

.btn-cancel {
  background-color: #f5f5f5;
  color: #333;
  border: 1px solid #ddd;
}

.btn-cancel:hover {
  background-color: #e6e6e6;
}

.no-selection {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
}

.no-selection p {
  color: #999;
  text-align: center;
}
</style>