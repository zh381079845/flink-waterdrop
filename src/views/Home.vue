<template>
  <div class="home">
    <div class="hero">
      <h1>欢迎使用 Flink Waterdrop 可视化流程设计器</h1>
      <p>通过拖拽方式轻松构建和管理 Flink 数据处理流程</p>
      <div class="actions">
        <router-link to="/designer" class="btn btn-primary">开始设计</router-link>
        <router-link to="/jobs" class="btn btn-secondary">查看作业</router-link>
      </div>
    </div>
    
    <div class="features">
      <div class="feature">
        <div class="feature-icon">🎨</div>
        <h3>可视化设计</h3>
        <p>通过拖拽方式构建数据处理流程，无需编写复杂代码</p>
      </div>
      
      <div class="feature">
        <div class="feature-icon">⚡</div>
        <h3>实时处理</h3>
        <p>基于 Apache Flink 引擎，支持实时数据流处理</p>
      </div>
      
      <div class="feature">
        <div class="feature-icon">🔧</div>
        <h3>灵活配置</h3>
        <p>每个组件都可以配置相关参数，满足各种业务需求</p>
      </div>
    </div>
  </div>
</template>

<script>
export default {
  name: 'Home'
}
</script>

<style scoped>
.home {
  padding: 20px;
  max-width: 1200px;
  margin: 0 auto;
}

.hero {
  text-align: center;
  padding: 60px 20px;
  background: linear-gradient(135deg, #1890ff 0%, #0050b3 100%);
  color: white;
  border-radius: 8px;
  margin-bottom: 40px;
}

.hero h1 {
  font-size: 2.5em;
  margin-bottom: 20px;
}

.hero p {
  font-size: 1.2em;
  margin-bottom: 30px;
  opacity: 0.9;
}

.actions .btn {
  display: inline-block;
  padding: 12px 24px;
  margin: 0 10px;
  border-radius: 4px;
  text-decoration: none;
  font-weight: bold;
  transition: all 0.3s;
}

.btn-primary {
  background-color: white;
  color: #1890ff;
}

.btn-primary:hover {
  background-color: #f0f0f0;
  transform: translateY(-2px);
  box-shadow: 0 4px 12px rgba(0,0,0,0.15);
}

.btn-secondary {
  background-color: transparent;
  color: white;
  border: 2px solid white;
}

.btn-secondary:hover {
  background-color: rgba(255,255,255,0.1);
  transform: translateY(-2px);
}

.features {
  display: flex;
  justify-content: space-between;
  flex-wrap: wrap;
  gap: 20px;
}

.feature {
  flex: 1;
  min-width: 250px;
  text-align: center;
  padding: 30px;
  border-radius: 8px;
  box-shadow: 0 2px 8px rgba(0,0,0,0.1);
  transition: all 0.3s;
}

.feature:hover {
  transform: translateY(-5px);
  box-shadow: 0 6px 16px rgba(0,0,0,0.15);
}

.feature-icon {
  font-size: 3em;
  margin-bottom: 20px;
}

.feature h3 {
  margin-bottom: 15px;
  color: #333;
}

.feature p {
  color: #666;
  line-height: 1.6;
}
</style>