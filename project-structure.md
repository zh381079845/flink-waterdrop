# Flink Waterdrop 低代码平台 - 前端项目结构

/src
  /assets            # 静态资源
  /components        # 公共组件
    /Designer        # 设计器相关组件
      FlowDesigner.vue       # 流程设计器主组件
      NodePanel.vue          # 节点面板组件
      ConfigPanel.vue        # 配置面板组件
      ConnectionLine.vue     # 连接线组件
    /DataPreview     # 数据预览相关组件
      PreviewPanel.vue       # 预览面板
      DataTable.vue          # 数据表格展示
    /Lineage         # 血缘分析相关组件
      LineageGraph.vue       # 血缘图组件
    /Debug           # 调试模式相关组件
      DebugPanel.vue         # 调试面板
      LogViewer.vue          # 日志查看器
    /VersionControl  # 版本管理相关组件
      VersionList.vue        # 版本列表
      VersionDiff.vue        # 版本对比
    /Templates       # 模板库相关组件
      TemplateList.vue       # 模板列表
      TemplateDetail.vue     # 模板详情
  /views             # 页面视图
    Home.vue                 # 首页
    Designer.vue             # 设计器页面
    JobList.vue              # 作业列表页面
    JobDetail.vue            # 作业详情页面
  /router            # 路由配置
    index.js
  /store             # 状态管理
    index.js
    modules/
      designer.js            # 设计器状态
      jobs.js                # 作业状态
      templates.js           # 模板状态
  /api               # API请求
    index.js
    job.js                   # 作业相关API
    designer.js              # 设计器相关API
    template.js              # 模板相关API
  /utils             # 工具函数
    validators.js            # 表单验证
    nodeTypes.js             # 节点类型定义
  /plugins           # 插件
    element-plus.js          # UI库配置
    x6.js                    # 图形库配置
  App.vue            # 应用主组件
  main.js            # 应用入口 