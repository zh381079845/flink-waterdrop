<template>
  <div class="designer-page">
    <h1>可视化流程设计器</h1>
    <p>拖拽组件构建数据处理流程，并配置相关参数</p>
    <FlowDesigner />
  </div>
</template>

<script>
import FlowDesigner from '@/components/Designer/FlowDesigner.vue'

export default {
  name: 'Designer',
  components: {
    FlowDesigner
  }
}
</script>

<style scoped>
.designer-page {
  height: 100%;
  padding: 20px;
}

.designer-page h1 {
  color: #333;
  text-align: center;
}

.designer-page p {
  text-align: center;
  color: #666;
  margin-bottom: 20px;
}
</style>