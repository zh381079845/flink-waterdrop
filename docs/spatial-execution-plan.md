# 空间能力改进执行计划

按下面 5 步顺序做。每步做完再进下一步。不升 Flink、不接 Sedona、不做栅格/瓦片。

当前基线：Flink 1.13.2，空间函数已收到 `plugin-flink-transform-spatial`。`st_area` 默认 WGS84 椭球；WKT 用 `WktUtils`（ThreadLocal）。`UdfSql` 只保留 `str_len`。

**执行状态：5 步均已落地。** `plugin-flink-transform-spatial` 已编译通过。

---

## Step 1 — 把算错的改完并验证

目标：面积可信，WKT 并发安全，空间实现只在 spatial 插件。

已完成（本步只收尾）：

- [x] `st_area` 默认 `Spheroid.area()`（平方米）；`st_area(wkt, false)` 为平面面积
- [x] `WktUtils` ThreadLocal 读写 WKT
- [x] 删除 sql 插件里的空间类和 GeoTools 依赖
- [x] `UdfSql` 不再注册空间函数
- [x] 相交配置改为 `SpatialUdfRegister` + `sql`

本步还要做：

1. ~~把 `plugin-flink-transform-spatial` 加入 `waterdrop-core` 依赖~~ 已完成
2. ~~编译 spatial / sql~~ 已通过。core 的 Scala 编译在本机 JDK 上失败（Scala 2.11 找不到 `java.lang.Object`），与本次空间改动无关

后续增强（大规模相关，已部分落地）：

- [x] `st_distance` / `st_dwithin` 改为 **最近点** 测地线距离（不再用质心）
- [x] `st_intersection` / `st_difference` / `st_union`（含单参 UnaryUnion）

验收：`st_area(POLYGON((116 39, 116 40, 117 40, 117 39, 116 39)))` 得到约 10^10 量级平方米，而不是平面度数面积。

---

## Step 2 — 空间关系谓词（已全部接上）

全部放在 spatial 插件，WKT / Geometry 双入参，由 `SpatialUdfRegister` 一次注册。

| SQL 函数 | 行为 |
|----------|------|
| `st_astext(geom)` | Geometry 或 WKT → WKT |
| `st_contains` / `st_within` / `st_covers` / `st_coveredby` | 包含关系 |
| `st_intersects` / `st_disjoint` / `st_overlaps` / `st_touches` / `st_crosses` | 相交关系 |
| `st_equals` / `st_orderingequals` | 相等 / 顶点顺序相等 |
| `st_dwithin(a, b, meters[, useSpheroid])` | 默认椭球距离 ≤ meters |
| `st_distance(a, b)` | 椭球距离（米） |
| `st_relate` / `st_relatematch` | DE-9IM 关系矩阵 |

验收：`SpatialPredicatesTest` 覆盖上述谓词；非法 WKT 返回 null，作业不挂。

---

## Step 3 — 空间 JOIN 用包络预过滤（+ 网格两阶段）

补外包矩形函数，给 Flink 普通 JOIN 用：

| SQL 函数 | 行为 |
|----------|------|
| `st_envelope(wkt)` | 外包矩形 POLYGON WKT |
| `st_xmin` / `st_xmax` / `st_ymin` / `st_ymax` | 包络坐标（建议物化落表） |
| `st_geohash(geom, n)` | 点/质心 geohash |
| `st_grid_id(geom, cellSize°)` | 主格子 `ix_iy` |
| `st_grid_ids(geom, cellSize°)` | 包络覆盖格子 CSV |
| `st_grid_cells(geom, cellSize°)` | UDTF，LATERAL 展开多行 |

示例：

- 仅包络：`config/flink.spatial.envelope.join.conf`
- Sedona 风格网格+包络+精炼：`config/flink.spatial.grid.join.conf`

```sql
-- 两侧 LATERAL 展开后：
ON a.grid_id = b.grid_id
AND a.maxx >= b.minx AND a.minx <= b.maxx
AND a.maxy >= b.miny AND a.miny <= b.maxy
AND st_intersects(a.geom, b.geom)
```

局部广播索引工具类：`LocalStrTreeIndex`（单 Task 内 STRtree，适合小维表）。

产品化封装：`SpatialJoin` transform（配置两表即可，内部调用 `SpatialJoinSqlBuilder` 生成上述 SQL）。

示例配置：

- `config/flink.spatial.envelope.join.conf`（手写 SQL）
- `config/flink.spatial.grid.join.conf`（手写 SQL）
- `config/flink.spatial.join.transform.conf`（**推荐**：`SpatialJoin { ... }`）

不做：分布式 R-Tree / 升 Flink 接 Sedona / 改 Flink 优化器。---

## Step 4 — WKT 进、Geometry 算、WKT 出

1. `WktUtils.toGeometry(Object)`：String 或 Geometry
2. 现有 UDF 增加 Geometry 重载，内部只算一次 Geometry
3. `st_geomfromwkt` / `st_astext` 成对使用
4. `SpatialUdfRegister.prepare` 里给 Flink 注册 JTS Geometry 的 Kryo 类型，避免 Geometry 列进 DataStream 后序列化失败

兼容：原来的 `st_buffer(wkt, dist)` / `st_intersects(wkt, wkt)` 仍返回/接受字符串。

推荐 SQL：

```sql
SELECT st_astext(st_geomfromwkt(wkt)) AS geom_wkt,
       st_area(wkt) AS area_m2
FROM src
```

---

## Step 5 — 只加 GeoJSON Source

在 spatial 插件增加 `GeoJsonSource`（包名 `io.github.interestinglab.waterdrop.flink.source`，方便框架按插件名反射）。

- 读 FeatureCollection 或单个 Feature
- 输出列：`id`（STRING）、`geometry`（WKT STRING）、`properties`（JSON STRING）
- 流/批都支持
- 样例：`config/spatial_sample.geojson` + `config/flink.spatial.geojson.conf`
- 不做 Shapefile / 栅格 / 瓦片

---

## 不做

- 升级 Flink 1.13.2
- 接入 Apache Sedona
- 分布式空间索引
- 栅格、XYZ 瓦片、Shapefile
