# 兼容矩阵

## 运行时（当前 preview）

| 组件 | 版本 | 说明 |
|------|------|------|
| JDK | 8 | 构建与测试基线 |
| Apache Flink | 1.13.2 | 父 POM 锁定 |
| Scala | 2.11.x | 与 Flink 发行配套 |
| JTS | GeoTools 24 传递 | 几何内核 |
| GeographicLib-Java | 1.52 | 椭球面积/距离 |
| GeoTools | 24.0 | GeoJSON 等；**LGPL** |

## 语义矩阵（摘要）

| API | 单位 / CRS | 备注 |
|-----|------------|------|
| `st_area` | 默认椭球 m² | `useSpheroid=false` 为平面 |
| `st_distance` / `st_dwithin` | 默认椭球米 | 最近点，非质心 |
| `st_buffer` | 米（经 3857） | 输入默认 4326 |
| Overlay | 平面 JTS | 面积请再 `st_area` |
| SpatialJoin | 配置模板 | 禁止 `mode=raw` |

完整对照见 [sedona-sql-compatibility.md](sedona-sql-compatibility.md)。

## 已知限制

- 无 Sedona 式分布式空间分区算子全集  
- `SpatialBroadcastJoin` 面向流式小维表；维表需能放入 Task 内存  
- 全国级数据建议先 `adcode` 等值分区再空间 JOIN  

## 升级意向（非承诺）

| 目标 | 状态 |
|------|------|
| Flink 1.17+ | 未验证 |
| 去 GeoTools / 仅 JTS | 可选重构 |
| Geometry 列贯穿（少 WKT） | P2 |
