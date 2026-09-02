# Flink Waterdrop 低代码平台

这是一个基于 Vue.js 的低代码平台前端项目，用于可视化设计和管理 Flink 任务流。

## 功能特性

- 可视化流程设计：通过拖拽方式设计数据处理流程
- 组件丰富：支持多种数据源、转换和输出组件
- 多源多输出：支持多个数据源输入和多个目标输出
- 动态配置表单：根据不同组件类型生成对应的配置界面
- 数据预览：实时查看节点输出的数据样例
- 数据血缘分析：可视化展示数据来源和去向的完整链路
- 作业管理：支持作业的创建、保存、部署、启停
- 版本管理：支持作业配置的版本历史和回滚
- 调试模式：支持本地小数据集测试处理流程

## 技术栈

- Vue 3：前端核心框架
- Vuex：状态管理
- Vue Router：路由管理
- Element Plus：UI组件库
- X6/G6：图形绘制库
- ECharts：数据可视化
- Axios：网络请求

## 项目结构

```
/src
  /assets            # 静态资源
  /components        # 公共组件
    /Designer        # 设计器相关组件
    /DataPreview     # 数据预览相关组件
    /Lineage         # 血缘分析相关组件
    /Debug           # 调试模式相关组件
    /VersionControl  # 版本管理相关组件
    /Templates       # 模板库相关组件
  /views             # 页面视图
  /router            # 路由配置
  /store             # 状态管理
  /api               # API请求
  /utils             # 工具函数
  /plugins           # 插件
  App.vue            # 应用主组件
  main.js            # 应用入口
```

## 核心功能介绍

### 1. 可视化流程设计

使用X6图形库实现的流程设计器，支持节点拖拽、连线、删除等操作。节点分为三种类型：

- Source（数据源）：从外部系统读取数据
- Transform（转换）：处理和转换数据
- Sink（输出）：将处理后的数据输出到目标系统

### 2. 数据预览功能

允许用户在设计过程中查看各节点输出的数据样例，支持表格、JSON和图表三种查看模式。数据预览功能使用TTL机制管理预览数据，确保资源的合理使用。

### 3. 数据血缘分析

可视化展示数据的来源和去向，支持表级和列级两种视图模式。通过血缘图可以清晰地了解数据流向和转换过程。

### 4. 调试模式

支持在本地运行小数据集测试整个处理流程，实时展示各节点的处理性能、日志和状态信息，帮助用户快速发现和解决问题。

### 5. 作业管理和版本控制

支持作业的保存、部署、执行和监控。版本管理功能允许用户查看历史版本，比较差异并进行回滚。

## 安装和运行

### 环境要求

- Node.js >= 14.0.0
- npm >= 6.0.0

### 安装依赖

```bash
npm install
```

### 开发环境运行

```bash
npm run dev
```

### 生产环境构建

```bash
npm run build
```

## 配置说明

通过环境变量文件（`.env`、`.env.development`、`.env.production`）配置API地址等信息：

```
# API基础路径
VUE_APP_API_BASE_URL=/api

# 其他配置...
```

## 二次开发指南

### 添加新组件类型

1. 在 `src/utils/nodeTypes.js` 中添加新的组件定义
2. 在后端实现对应的组件处理逻辑
3. 添加新组件的配置模板

### 自定义主题

可通过修改 Element Plus 的主题变量来自定义UI风格：

``scss
// 在 src/assets/styles/main.scss 中修改
:root {
  --el-color-primary: #409eff;
  // 其他变量...
}
```

## 参与贡献

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/my-feature`)
3. 提交更改 (`git commit -m 'Add some feature'`)
4. 推送到分支 (`git push origin feature/my-feature`)
5. 创建 Pull Request

## 许可证

MIT



src/
├── assets/                  # 静态资源
│   ├── css/                 # 样式文件
│   │   ├── global.css       # 全局样式文件
│   │   └── variables.css    # 变量定义文件
│   ├── images/              # 图片资源
│   └── fonts/               # 字体资源
├── components/              # 公共组件
│   ├── Designer/            # 设计器相关组件
│   │   ├── FlowDesigner.vue     # 流程设计器主组件
│   │   ├── NodePanel.vue        # 节点面板组件
│   │   ├── ConfigPanel.vue      # 配置面板组件
│   │   └── ConnectionLine.vue   # 连接线组件
│   ├── DataPreview/         # 数据预览相关组件
│   │   ├── PreviewPanel.vue     # 预览面板
│   │   └── DataTable.vue        # 数据表格展示
│   ├── Lineage/             # 血缘分析相关组件
│   │   └── LineageGraph.vue     # 血缘图组件
│   ├── Debug/               # 调试模式相关组件
│   │   ├── DebugPanel.vue       # 调试面板
│   │   └── LogViewer.vue        # 日志查看器
│   ├── VersionControl/      # 版本管理相关组件
│   │   ├── VersionList.vue      # 版本列表
│   │   └── VersionDiff.vue      # 版本对比
│   └── Templates/           # 模板库相关组件
│       ├── TemplateList.vue     # 模板列表
│       └── TemplateDetail.vue   # 模板详情
├── views/                   # 页面视图
│   ├── Home.vue             # 首页
│   ├── Designer.vue         # 设计器页面
│   ├── JobList.vue          # 作业列表页面
│   └── JobDetail.vue        # 作业详情页面
├── router/                  # 路由配置
│   └── index.js             # 路由入口文件
├── store/                   # 状态管理
│   ├── index.js             # 状态管理入口
│   └── modules/             # 状态模块
│       ├── designer.js      # 设计器状态
│       ├── jobs.js          # 作业状态
│       └── templates.js     # 模板状态
├── api/                     # API请求
│   ├── index.js             # API入口
│   ├── job.js               # 作业相关API
│   ├── designer.js          # 设计器相关API
│   └── template.js          # 模板相关API
├── utils/                   # 工具函数
│   ├── validators.js        # 表单验证
│   └── nodeTypes.js         # 节点类型定义
├── plugins/                 # 插件
│   ├── element-plus.js      # UI库配置
│   └── x6.js                # 图形库配置
├── App.vue                  # 应用主组件
└── main.js                  # 应用入口
