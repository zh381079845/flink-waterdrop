# 空间业务需求验收报告

对照文档：`docs/spatial-business-requirements.md`  
样例数据：`plugin-flink-transform-spatial/src/test/resources/business/*.geojson`  
验收测试：

- `BusinessRequirementsAcceptanceTest`（UDF / 业务规则）
- `BusinessRequirementsFlinkSqlTest`（Flink SQL 作业链路）

**测试执行结果（2026-09-02）：45 / 45 通过。**

```bash
mvn -pl plugin-flink-transform-spatial -am test -Dskip.pmd.check=true \
  -Dtest=BusinessRequirementsAcceptanceTest,BusinessRequirementsFlinkSqlTest \
  -Dsurefire.failIfNoSpecifiedTests=false
```

---

## 1. 结论摘要

| 结论 | 说明 |
|------|------|
| **能完成** | 需求文档中标为 **P0** 的主干空间业务：冲突筛查、落区/落格、缓冲覆盖、椭球面积、包络 JOIN、GeoJSON 入库规范化、多数拓扑谓词 |
| **部分完成** | 需要「相交面积 / 净用地 / 缝隙」等结果时，当前只能做**相交判定**，不能直接算交并差几何 |
| **不能完成（当前引擎）** | P1/P2：`st_intersection`/`difference`/`union`、`isValid`、线面**最近点**距离、路网服务区、栅格/三维、分布式空间索引 |
| **语义注意** | `st_distance` / `st_dwithin` 对非 Point 几何使用**质心**测地线距离；管线安全距等场景请优先用 `st_buffer` + `st_intersects` |

当前 Flink Waterdrop **可以支撑**文档第 22 节列出的 5 个对外业务包；**不能**宣称覆盖全部 180+ 条需求。

---

## 2. 样例数据清单

| 文件 | 要素 | 业务用途 |
|------|------|----------|
| `eco_redline.geojson` | ECO-001/002 | 生态红线 |
| `farmland.geojson` | FARM-001/002 | 永久基本农田 |
| `projects.geojson` | PRJ-CONFLICT/OK/OUTSIDE/POINT | 拟建项目与选址点 |
| `development_boundary.geojson` | DEV-001 | 城镇开发边界 |
| `facilities.geojson` | 医院/学校/消防/门店/危化 | 覆盖、防护、竞品距 |
| `roads_pipelines.geojson` | 道路/燃气管 | 穿红线、安全距 |
| `parcels_houses.geojson` | 宗地/房屋/小区/建筑 | 权属、违建、淹没 |
| `zones_grids.geojson` | 水源/文物/网格/事件/淹没/带孔/多部件 | 分区、落格、面积 |

坐标系：WGS84（经纬度）。同一 FeatureCollection 内属性字段已对齐（GeoTools 异构 schema 会报错）。

---

## 3. 已用测试覆盖并通过的需求 ID

### 3.1 UDF 验收（39）

| 需求 ID | 结果 | 验证要点 |
|---------|------|----------|
| L01 | PASS | 冲突项目∩红线；合规项目不相交 |
| L02 | PASS | 冲突项目∩农田 |
| L04 | PASS | 边界内/外判定 |
| L05 | PASS | 同时命中红线+农田 |
| L06 | PASS | 椭球面积 >1e7 m² |
| L07 | PASS | 带孔面积 < 外环 |
| L08 | PASS | MultiPolygon 面积可算 |
| L14 | PASS | 多约束一票否决组合 |
| R01 | PASS | 宗地 overlaps |
| R04 | PASS | 房屋 within 宗地 |
| R07 | PASS | 多部件质心距离过大→飞地 |
| R09 | PASS | 抵押范围⊆权属（covers/contains） |
| R10 | PASS | WKT↔Geometry roundtrip |
| U03 | PASS | 建筑面积/用地面积比 |
| U07 | PASS | 文物控建带关系 |
| U08 | PASS | 医院 5km buffer 覆盖小区 |
| M01 | PASS | 道路 crosses/intersects 红线 |
| M02 | PASS | **buffer∩建筑**（非质心距离） |
| M04 | PASS | 道路拓宽走廊∩约束 |
| M06 | PASS | 小半径覆盖盲区 |
| E01 | PASS | 水源保护区叠置关系 |
| E06 | PASS | 敏感目标 dwithin |
| S01–S03/S05 | PASS | 危化距、疏散圈、消防到达、淹没叠置 |
| C01/C02 | PASS | 事件落格、越界建筑 |
| B01/B02 | PASS | 门店覆盖、竞品过近 |
| D01/D05/D07/D09/D13/D14 | PASS | 脏 WKT、引用完整、包络过滤、GeoJSON、DE-9IM、null-safe |
| L-END-01 / S-END-01 / B-END-01 | PASS | 组合链路（UDF） |

### 3.2 Flink SQL 验收（6）

| 场景 | 对应需求 | 结果 |
|------|----------|------|
| 项目×约束包络 JOIN + intersects + area | L-END-01 / L01 / L02 / D07 | PASS |
| 医院 buffer 覆盖居住区 | U08 | PASS |
| 事件 within 网格派单 | C-END-01 / C01 | PASS |
| 危化 buffer 影响资产 + distance | S-END-01 | PASS |
| 道路∩红线 | M01 | PASS |
| 宗地 overlaps 两两检测 | R01 | PASS |

---

## 4. 业务包级判定（文档 §22）

| 业务包 | 判定 | 证据 |
|--------|------|------|
| 1. 空间合规冲突检测 | **可完成** | L01/L02/L05 + SQL JOIN |
| 2. 缓冲覆盖与防护距离 | **可完成**（线面距用 buffer） | U08/M02/S02 + SQL |
| 3. 面积量算与汇总 | **可完成** | L06/L07/L08；交叉分类汇总可用 GROUP BY + area，**相交斑块面积需 P1** |
| 4. GeoJSON 交换与质检入库 | **可完成** | D01/D09；同文件属性 schema 需一致 |
| 5. 包络加速大表叠置 | **可完成** | D07 + SQL envelope 条件 |

---

## 5. 明确未覆盖 / 阻塞项（抽样）

| 需求 ID | 原因 | 需要什么 |
|---------|------|----------|
| L01 占用**面积**精确值 | 无 `st_intersection` | P1 交并差 |
| L09 净用地 | 无 difference | P1 |
| R02 缝隙检测 | 无差集/边界长度 | P1 |
| R06 共享边界长度 | 无 length(intersection) | P1 |
| U01 退线精确 | 线到面最近点 | 改进 distance 或 buffer 规则 |
| M07 / S 路网疏散 | 无网络分析 | P2 |
| D02 isValid | 未暴露 | P1 |
| 全量 180+ 条目 | 未逐条造数 | 可持续按 ID 增补用例 |

---

## 6. 已知实现语义（验收时必须知晓）

1. **`st_distance` / 非点 `st_dwithin`**：对 Line/Polygon 取 **Centroid** 再算 WGS84 测地线距离。  
2. **Flink SQL 数字字面量**：`st_buffer(g, 5000)` 可能绑成 Integer，需 `CAST(5000 AS DOUBLE)` 或 `5000.0`。  
3. **GeoJSON**：同一 FeatureCollection 属性键集合需一致，否则 GeoTools 可能抛 `No such attribute`。  
4. **冲突面积**：当前输出「是否冲突 + 项目自身面积」，不是「交集面积」。

---

## 7. 复现命令

```bash
# 仅业务验收
mvn -pl plugin-flink-transform-spatial -am test -Dskip.pmd.check=true \
  -Dtest=BusinessRequirementsAcceptanceTest,BusinessRequirementsFlinkSqlTest \
  -Dsurefire.failIfNoSpecifiedTests=false

# 全 spatial 模块
mvn -pl plugin-flink-transform-spatial -am test -Dskip.pmd.check=true
```

---

*本报告随测试套件更新；新增需求请在测试方法名中保留需求 ID 前缀以便追溯。*
