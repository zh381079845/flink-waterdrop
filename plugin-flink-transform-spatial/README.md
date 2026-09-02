# plugin-flink-transform-spatial

Waterdrop / Flink 空间转换插件（**v0.1 preview**）。

## 能力概览

- **UDF**：`st_intersects` / `st_area` / `st_distance` / `st_buffer` / overlay / `st_simplify` / grid 等（见 `SpatialFunctionRegistry`）
- **Transform**
  - `SpatialUdfRegister` — 注册全部 `st_*`
  - `SpatialMaterialize` — 物化 `minx/maxx/miny/maxy[/grid_id]`
  - `SpatialJoin` — 强制模板 JOIN；已物化用 `use_materialized_grid=true` 跳过展开
  - `SpatialBroadcastJoin` — 小表 `LocalStrTreeIndex` 广播探测大表
- **契约**：默认 WGS84；面积/距离默认椭球米（详见 `docs/sedona-sql-compatibility.md`）

## 构建与测试

```bash
mvn -pl plugin-flink-transform-spatial -am test -Dskip.pmd.check=true
```

## 配置片段

已物化后跳过现场展开：

```hocon
SpatialJoin {
  left_table = "eco_prep"
  right_table = "farm_prep"
  use_materialized_grid = true
  predicate = "intersects"
  result_table_name = "conflicts"
}
```

## 依赖注意

- GeoTools（LGPL）通过 Maven 引入，再分发请自行评估合规。
- Flink 版本锁定见父 POM：`1.13.2`。
