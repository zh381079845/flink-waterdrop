import { createApp } from 'vue'
import App from './App.vue'
import router from './router'
import store from './store'

// 引入插件
import ElementPlusPlugin from './plugins/element-plus'
import X6Plugin from './plugins/x6'

// 创建应用实例
const app = createApp(App)

// 使用插件
ElementPlusPlugin.initElementPlus(app)
X6Plugin.initX6()

// 挂载应用
app.use(store).use(router).mount('#app')