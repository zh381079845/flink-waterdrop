# 空间引擎性能优化指南

面向当前 Waterdrop + Flink 1.13 + `st_*` UDF。**禁止**依赖裸 `JOIN ... ON st_intersects` 做大规模计算。

## 强制规范（P0）

1. 空间叠置必须走 `SpatialJoin`（`mode=grid|envelope|prepared|prepared_grid`），禁止 `mode=raw`。  
2. 热数据先 `SpatialMaterialize` 再 JOIN；已物化后设 `use_materialized_grid=true`（或 `mode=prepared_grid`）**跳过** `st_xmin` / `st_grid_cells`。  
3. `cell_size` 按密度调；`max_cells` 限制单要素展开（默认 10000，过大则退化为质心单格）。

### 已物化则跳过展开

| 配置 | 行为 |
|------|------|
| `use_materialized_bounds=true` / `mode=prepared` | 读 `minx/maxx/miny/maxy`，无 `st_xmin` |
| `use_materialized_grid=true` / `mode=prepared_grid` | 再读 `grid_id`，无 LATERAL |
| 可选列名 | `minx_col` / `maxx_col` / `miny_col` / `maxy_col` / `grid_id_col` |

### 推荐 `cell_size`（WGS84 度）

| 场景 | cell_size | 说明 |
|------|-----------|------|
| 城区地块 | 0.01～0.05 | ~1–5 km |
| 市域 | 0.05～0.2 | |
| 省域粗筛 | 0.2～1.0 | 再叠加 `adcode` 分区 |

## 流水线模板

### A. 物化 + prepared_grid（批/大表×大表）

见 `config/flink.spatial.perf.prepared.join.conf`。

### B. 广播 STRtree（小维表 × 大流）

见 `config/flink.spatial.broadcast.join.conf`（`SpatialBroadcastJoin`）。

微基准（千面 × 百万点，热路径 `LocalStrTreeIndex`）：

```bash
mvn -pl plugin-flink-transform-spatial -Dtest=SpatialBroadcastJoinMicroBenchmark test
# 可调规模：-Dspatial.bench.polys=1000 -Dspatial.bench.points=1000000
```

### C. 行政区先切

```hocon
SpatialJoin {
  mode = "grid"
  left_partition_key = "adcode"
  right_partition_key = "adcode"
  ...
}
```

### D. 粗几何筛 + 精几何判

物化时 `simplify_tolerance`，或 JOIN 配置 `left_coarse_geom` / `right_coarse_geom`（bounds/grid 用粗列，谓词仍用原 `geometry`）。

## 谓词短路

`SpatialJoinSqlBuilder` 已保证：`st_intersection` / `st_area` **只在 SELECT**，ON 中仅 grid + envelope + 便宜谓词。

## WKT 解析

- UDF 内 `WktUtils` 为 ThreadLocal Reader（勿每调用 new）。  
- 理想路径：解析一次后 Geometry 算到底（P2 增强）；当前热 JOIN 优先减少调用次数（物化 bounds）。

## Flink 调参（P2）

- `env.execution.parallelism` 按核数提高  
- 广播维表控制在内存可承受范围  
- 监控反压：格过小导致 explode 行膨胀是常见原因  

## 能力对照

| 优化项 | 落地 |
|--------|------|
| 物化 minx/maxx/miny/maxy/grid_id | `SpatialMaterialize` |
| cell_size + max_cells | 配置 + `st_grid_cells(..., maxCells)` |
| 禁止无模板空间 JOIN | `SpatialJoin.checkConfig` 拒绝 raw |
| 广播 + STRtree | `SpatialBroadcastJoin` |
| 谓词短路 | SQL Builder |
| 分区键 | `left/right_partition_key` |
| simplify | `st_simplify` + materialize/join 粗列 |

版本与兼容矩阵见 [VERSIONING.md](VERSIONING.md)、[COMPATIBILITY.md](COMPATIBILITY.md)。
