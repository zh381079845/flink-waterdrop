# plugin-flink-transform-spatial

Flink 1.13 上的空间计算插件。几何用 JTS，面积/距离默认按 WGS84 椭球米算（GeographicLib）。

不要拿裸 SQL `JOIN ... ON st_intersects(...)` 跑大表，会炸。大表叠置走 `SpatialMaterialize` + `SpatialJoin`，小面 × 海量点走 `SpatialBroadcastJoin`。

## 怎么跑测试

```bash
mvn -pl plugin-flink-transform-spatial -am test -Dskip.pmd.check=true
```

千面 × 百万点的本地微基准：

```bash
mvn -pl plugin-flink-transform-spatial -Dtest=SpatialBroadcastJoinMicroBenchmark test
```

## Transform

| 名字 | 干什么 |
|------|--------|
| `SpatialUdfRegister` | 把下面这些 `st_*` 注册进 Table 环境 |
| `SpatialMaterialize` | 把 `minx/maxx/miny/maxy`，以及可选的 `grid_id` 算出来落成普通列 |
| `SpatialJoin` | 网格 / 包络 JOIN。表上已经有物化列时设 `use_materialized_grid=true`，不要再现场 `st_xmin` / `LATERAL` |
| `SpatialBroadcastJoin` | 小表建 STRtree 广播出去，大表逐条探测 |

推荐作业配置：

- `config/flink.spatial.perf.prepared.join.conf` — 先物化再 JOIN
- `config/flink.spatial.broadcast.join.conf` — 广播探测

`SpatialJoin` 片段：

```hocon
SpatialJoin {
  left_table = "eco_prep"
  right_table = "farm_prep"
  use_materialized_grid = true
  predicate = "intersects"
  result_table_name = "conflicts"
}
```

`cell_size` 按数据密度调；太大候选多，太小行膨胀。单要素展开上限用 `max_cells`。全国数据最好先按 `adcode` 等值（`left_partition_key` / `right_partition_key`），再做空间条件。

## SQL 函数

注册入口是 `SpatialFunctionRegistry`。常用的：

- 构造 / 输出：`st_geomfromwkt`、`st_astext`、`st_asgeojson`、`st_point`、`st_centroid`、`st_x`、`st_y`
- 量测：`st_area`、`st_distance`、`st_length`、`st_perimeter`、`st_buffer`、`st_dwithin`
- 谓词：`st_intersects`、`st_contains`、`st_within`、`st_covers`、`st_coveredby`、`st_crosses`、`st_overlaps`、`st_touches`、`st_equals`、`st_disjoint`、`st_relate`
- 叠置：`st_intersection`、`st_difference`、`st_union`、`st_symdifference`
- 包络：`st_envelope`、`st_xmin`、`st_xmax`、`st_ymin`、`st_ymax`
- 网格：`st_geohash`、`st_grid_id`、`st_grid_ids`、`st_grid_cells`
- 其它：`st_isvalid`、`st_makevalid`、`st_simplify`

默认输入是经纬度。`st_buffer` 按米缓冲（内部走 3857）。overlay 是平面 JTS，要面积再套一层 `st_area`。非法 WKT 多数返回 null，作业不会直接挂。

## 依赖

- Flink `1.13.2`（跟父 POM）
- GeoTools 24（LGPL，打包分发前自己看一下许可证）
