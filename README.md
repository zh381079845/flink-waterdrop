# Flink Waterdrop Spatial

[![Spatial CI](https://img.shields.io/badge/CI-spatial-blue)](.github/workflows/spatial-ci.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Status](https://img.shields.io/badge/status-v0.1%20preview-orange)](docs/VERSIONING.md)

基于 **Apache Flink 1.13** 的空间计算插件（JTS + GeographicLib），以 Waterdrop 配置驱动方式运行 `st_*` UDF、物化网格与空间 JOIN。

> **v0.1 preview**：能讲清楚、能跑通、有护栏。  
> **不是** Apache Sedona / PostGIS 兼容实现（函数名 `st_*` 仅为 SQL 习惯）。

## 快速开始

```bash
# JDK 8 + Maven 3.6+
mvn -pl plugin-flink-transform-spatial -am test -Dskip.pmd.check=true
```

推荐配置流水线：

| 场景 | 配置 |
|------|------|
| 物化 + prepared_grid JOIN | [`config/flink.spatial.perf.prepared.join.conf`](config/flink.spatial.perf.prepared.join.conf) |
| 小维表广播 STRtree | [`config/flink.spatial.broadcast.join.conf`](config/flink.spatial.broadcast.join.conf) |
| 网格 JOIN | [`config/flink.spatial.grid.join.conf`](config/flink.spatial.grid.join.conf) |

微基准（千面 × 百万点）：

```bash
mvn -pl plugin-flink-transform-spatial -Dtest=SpatialBroadcastJoinMicroBenchmark test
```

## 文档

| 文档 | 说明 |
|------|------|
| [空间插件 README](plugin-flink-transform-spatial/README.md) | 模块能力、Transform、UDF |
| [性能指南](docs/spatial-performance-guide.md) | 物化、禁止裸 JOIN、调参 |
| [版本策略](docs/VERSIONING.md) | preview / 兼容承诺 |
| [兼容矩阵](docs/COMPATIBILITY.md) | Flink / JDK / CRS |
| [贡献指南](CONTRIBUTING.md) | 如何提 PR |

## 仓库结构（引擎相关）

```
plugin-flink-transform-spatial/   # 空间引擎（核心开源目标）
waterdrop-flink-api/              # Transform SPI
config/flink.spatial*.conf        # 示例作业
docs/                             # 设计与规范
```

前端低代码说明已移至 [`docs/frontend-lowcode-readme.md`](docs/frontend-lowcode-readme.md)（非本 preview 焦点）。

## 许可证

[Apache License 2.0](LICENSE)。第三方归属见 [NOTICE](NOTICE)。

本仓库部分代码命名空间源自 InterestingLab **Waterdrop**（后演进为 Apache **SeaTunnel**）血统；空间模块为本项目增量能力。与 ASF / SeaTunnel **无官方隶属关系**。
