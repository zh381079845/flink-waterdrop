<template>
  <div id="app">
    <header class="app-header">
      <h1>Flink Waterdrop 可视化流程设计器</h1>
      <nav class="main-nav">
        <router-link to="/">首页</router-link>
        <router-link to="/designer">流程设计</router-link>
        <router-link to="/jobs">作业列表</router-link>
      </nav>
    </header>
    <main class="app-main">
      <router-view/>
    </main>
    <footer class="app-footer">
      <p>Flink Waterdrop 低代码平台 ©2025</p>
    </footer>
  </div>
</template>

<script>
export default {
  name: 'App'
}
</script>

<style>
* {
  margin: 0;
  padding: 0;
  box-sizing: border-box;
}

#app {
  font-family: 'Avenir', Helvetica, Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  color: #2c3e50;
  height: 100vh;
  display: flex;
  flex-direction: column;
}

.app-header {
  background-color: #1890ff;
  color: white;
  padding: 0 20px;
  box-shadow: 0 2px 8px rgba(0,0,0,0.15);
  z-index: 100;
}

.app-header h1 {
  font-size: 1.5em;
  display: inline-block;
  margin: 0;
  padding: 15px 0;
}

.main-nav {
  display: inline-block;
  margin-left: 30px;
}

.main-nav a {
  color: rgba(255,255,255,0.85);
  text-decoration: none;
  padding: 15px 10px;
  display: inline-block;
}

.main-nav a:hover {
  color: white;
}

.main-nav a.router-link-exact-active {
  color: white;
  font-weight: bold;
  border-bottom: 2px solid white;
}

.app-main {
  flex: 1;
  overflow: auto;
}

.app-footer {
  background-color: #f5f5f5;
  padding: 15px;
  text-align: center;
  border-top: 1px solid #ddd;
  color: #666;
}
</style>