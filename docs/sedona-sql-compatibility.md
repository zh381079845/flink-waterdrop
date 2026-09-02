# Waterdrop Spatial ↔ Apache Sedona SQL 对照表

> **兼容声明（P0，强制）：本仓库不是 Apache Sedona 兼容实现。**  
> 函数名采用 `st_*` 仅方便 SQL 书写；**不得**宣传为 Sedona / PostGIS 兼容。  
> 全集对齐 Sedona 的成本接近「升级 Flink 并接入真 Sedona」，见 §4 P2。

基线：`SpatialFunctionRegistry` 当前注册函数；Sedona 以 [Flink Geometry Functions](https://sedona.apache.org/latest/api/flink/Geometry-Functions/) 为准。

状态图例：✅ 已有 · ⚠️ 语义差 · ❌ 缺失 · ➕ 自有（非 Sedona API）

---

## 0. CRS / 单位契约（P0 护栏）

| 项 | Waterdrop 契约 |
|----|----------------|
| 输入 CRS | 默认 **WGS84 经纬度**（未做 `ST_Transform`） |
| `st_area` | 默认 **椭球 m²**；`st_area(g, false)` 平面 |
| `st_distance` / `st_dwithin` | 默认 **最近点 + 椭球米** |
| `st_length` / `st_perimeter` | 默认 **椭球米**；第二参 `false` 为平面 |
| `st_buffer` | 默认 4326→**EPSG:3857 按米**缓冲再转回 |
| Overlay | **平面 JTS**；面积请再套 `st_area` |
| 非法 WKT | 多数返回 **null**，作业不抛 |
| 金标测试 | `SpatialGoldenContractTest`（本引擎契约，非 Sedona CI） |

与 Sedona `Geometry` 默认（平面 CRS 单位）**数值不可直接混比**；Sedona `Geography` 更接近本侧面积/距离语义，但仍非同一实现。

---

## 1. 覆盖率速览

| 类别 | Waterdrop | Sedona Flink | 判断 |
|------|-----------|--------------|------|
| 谓词 | 14 + relate 三参 | 核心齐全 | 核心齐，细节 ⚠️ |
| 构造 / 访问 | GeomFromWKT、Point、Centroid、X/Y、AsText、AsGeoJSON | 30+ | P1 补了主干，仍缺 WKB/EWKT 等 |
| 量测 / Overlay | area/distance/buffer/length/perimeter + 交并差对称差 | 更多 | 主干可用 |
| 质检 | isvalid / makevalid | 有 | P1 已补 |
| Aggregate | 0 | 有 | ❌ |
| 网格辅助 | grid_* / geohash | 引擎分区 + GeoHash | ➕ 自有 |

相对 Sedona 常用全集：约 **20%～30%** 名字面；国土 ETL 刚需子集约 **70%～80%** 可拼。

---

## 2. 已注册函数逐项对照（摘要）

### 谓词
`st_intersects/contains/within/covers/coveredby/crosses/disjoint/overlaps/touches/equals/orderingequals/dwithin/relate/relatematch` → ✅/⚠️（见历史表；`st_relate(a,b,pattern)` 三参已支持）。

### 量测与缓冲
| 函数 | 状态 | 备注 |
|------|------|------|
| `st_area` | ⚠️ | 默认椭球 |
| `st_distance` | ⚠️ | 最近点椭球米 |
| `st_buffer` | ⚠️ | 3857 米 |
| `st_length` / `st_perimeter` | ⚠️ | **P1 已补**，默认椭球米 |
| `ST_Azimuth` 等 | ❌ | — |

### Overlay
| 函数 | 状态 |
|------|------|
| `st_intersection` / `st_difference` / `st_union` | ⚠️ 平面 + 多返回 WKT |
| `st_symdifference` | ⚠️ **P1 已补** |

### 构造 / 导出 / 访问 / 质检
| 函数 | 状态 |
|------|------|
| `st_geomfromwkt` / `st_astext` | ⚠️ |
| `st_point` / `st_centroid` / `st_x` / `st_y` | ⚠️ **P1 已补**（平面质心） |
| `st_asgeojson` | ⚠️ **P1 已补** |
| `st_isvalid` / `st_makevalid` | ✅/⚠️ **P1 已补**（IsValidOp；makevalid 用 buffer(0) 启发式） |
| `st_envelope` / `st_xmin`… | ⚠️ |
| WKB/EWKT/MakeEnvelope/… | ❌ |

### 网格（自有）
`st_geohash` ⚠️ · `st_grid_id` / `st_grid_ids` / `st_grid_cells` ➕ · `SpatialJoin` ➕

---

## 3. 关键语义差异（写业务必看）

| 主题 | Waterdrop | Sedona Geometry 常见 |
|------|-----------|----------------------|
| SQL 几何载体 | 多为 **WKT 字符串** | Geometry 列 |
| 面积/距离 | 椭球米 | 平面单位（Geography 另论） |
| 优化器 | UDF 黑盒；用 `SpatialJoin` 模板 | 空间 Join 可规划 |
| 兼容承诺 | **不兼容**；仅金标契约 | 官方 ST_ 文档 |

---

## 4. 分期路线

### P0 — 声明 + 金标（已做）
- [x] 本文兼容声明与 CRS 契约  
- [x] `SpatialGoldenContractTest`：intersects/contains/dwithin/area/distance/buffer/intersection/envelope/geohash  

### P1 — 高价值补齐（已做）
- [x] `st_length` / `st_perimeter`  
- [x] `st_centroid` / `st_point` / `st_x` / `st_y`  
- [x] `st_isvalid` / `st_makevalid`  
- [x] `st_symdifference`  
- [x] `st_asgeojson`  
- [x] `st_relate(a,b,pattern)` 三参（原已有，P1 测覆盖）  
- [x] `SpatialP1OperatorsTest`

### P2 — 真·兼容层（未做；成本≈Sedona）
- Geometry 类型一等公民、聚合、WKB/CRS、优化器感知 Join  
- **建议**：需要全集时升级 Flink 并接入 Apache Sedona，而不是在 1.13 UDF 上穷举  

---

## 5. 勿冒充 Sedona 的自有能力

`st_grid_*`、`LocalStrTreeIndex`、`GeoJsonSource`、`SpatialJoin`、默认椭球+3857 buffer 组合语义。

---

## 6. 一句话

| 问题 | 答案 |
|------|------|
| Sedona SQL 兼容？ | **否**（已声明） |
| P0/P1？ | **已完成**（声明 + 金标 + P1 算子） |
| 全集兼容？ | **不建议自研穷举**；成本接近接 Sedona |

---

*随 `SpatialFunctionRegistry` 更新。Sedona 文档请以 latest 为准。*
