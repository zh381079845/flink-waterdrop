# 版本策略（v0.1 preview）

| 标签 | 含义 |
|------|------|
| **v0.1.x preview** | 公开可编译、测试绿、文档齐；API / SQL 语义可能在小版本间调整 |
| **v0.2**（规划） | UDF 清单冻结草案 + 兼容矩阵 CI |
| **v1.0**（规划） | 语义冻结、变更走弃用周期；可称「生产可用」需附真实作业报告 |

## 兼容承诺（preview）

- **承诺**：`SpatialJoin` / `SpatialMaterialize` / `SpatialBroadcastJoin` 配置键尽量保持；破坏性变更在 CHANGELOG 标明。
- **不承诺**：与 Sedona / PostGIS 数值或函数全集一致；Flink 大版本升级路径。
- **CRS**：默认 WGS84 契约不变，除非升 major 并改文档。

## 发版检查清单

1. `mvn -pl plugin-flink-transform-spatial -am test -Dskip.pmd.check=true` 全绿  
2. 微基准可跑通（不强制写入发布说明数值）  
3. 更新 `docs/COMPATIBILITY.md` 若依赖版本变化  
4. Git tag：`spatial-v0.1.0`（建议）
