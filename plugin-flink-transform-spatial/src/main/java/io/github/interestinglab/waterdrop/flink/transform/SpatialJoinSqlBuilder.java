package io.github.interestinglab.waterdrop.flink.transform;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * 生成性能友好的空间 JOIN SQL。
 * <ul>
 *   <li>grid：现场 LATERAL 展开（可带 max_cells）</li>
 *   <li>envelope：仅包络（现场 st_xmin…）</li>
 *   <li>prepared：已物化 minx/maxx/miny/maxy → 跳过 st_xmin/st_xmax/…</li>
 *   <li>prepared_grid：已物化 bounds + grid_id → 跳过 st_xmin 与 st_grid_cells 展开</li>
 * </ul>
 * 谓词短路：ON 中只放便宜谓词；st_intersection/area 仅出现在 SELECT。
 */
public final class SpatialJoinSqlBuilder {

    private static final Map<String, String> PREDICATES = new HashMap<String, String>();

    static {
        PREDICATES.put("intersects", "st_intersects(%s, %s)");
        PREDICATES.put("contains", "st_contains(%s, %s)");
        PREDICATES.put("within", "st_within(%s, %s)");
        PREDICATES.put("covers", "st_covers(%s, %s)");
        PREDICATES.put("coveredby", "st_coveredby(%s, %s)");
        PREDICATES.put("crosses", "st_crosses(%s, %s)");
        PREDICATES.put("overlaps", "st_overlaps(%s, %s)");
        PREDICATES.put("touches", "st_touches(%s, %s)");
        PREDICATES.put("equals", "st_equals(%s, %s)");
        PREDICATES.put("disjoint", "st_disjoint(%s, %s)");
    }

    public enum Mode {
        GRID,
        ENVELOPE,
        /** 已物化 bounds：JOIN 直接读列，不再现场 st_xmin */
        PREPARED,
        /** 已物化 bounds + grid_id：不再 st_xmin / LATERAL st_grid_cells */
        PREPARED_GRID
    }

    public static final class Spec {
        public String leftTable;
        public String rightTable;
        public String leftGeom = "geometry";
        public String rightGeom = "geometry";
        public String leftKey = "id";
        public String rightKey = "id";
        public double cellSize = 0.05;
        public int maxCells = SpatialGrid.DEFAULT_MAX_CELLS;
        public String predicate = "intersects";
        public Mode mode = Mode.GRID;
        public boolean intersectionArea = false;
        public boolean dedup = true;
        /** 业务分区键（如 adcode），先等值再空间 */
        public String leftPartitionKey;
        public String rightPartitionKey;
        /** prepared / prepared_grid 物化列名（默认与 SpatialMaterialize 一致） */
        public String minxCol = "minx";
        public String maxxCol = "maxx";
        public String minyCol = "miny";
        public String maxyCol = "maxy";
        public String gridIdCol = "grid_id";
        /** 粗筛用抽稀几何列（可选）；精炼仍用 leftGeom/rightGeom */
        public String leftCoarseGeom;
        public String rightCoarseGeom;

        /** grid 物化优先于仅 bounds；用于配置 use_materialized_* */
        public void applyMaterializedFlags(boolean useMaterializedBounds, boolean useMaterializedGrid) {
            if (useMaterializedGrid) {
                this.mode = Mode.PREPARED_GRID;
            } else if (useMaterializedBounds) {
                this.mode = Mode.PREPARED;
            }
        }

        /** 是否跳过现场 st_xmin / LATERAL 展开 */
        public boolean skipsLiveExpand() {
            return mode == Mode.PREPARED || mode == Mode.PREPARED_GRID;
        }
    }

    private SpatialJoinSqlBuilder() {
    }

    public static String build(Spec spec) {
        validate(spec);
        String predTemplate = PREDICATES.get(spec.predicate.toLowerCase(Locale.ROOT));
        String predCall = String.format(predTemplate, "l._geom", "r._geom");

        String leftSub = expandSubquery("lsrc", spec.leftTable, spec.leftKey, spec.leftGeom,
                spec.leftCoarseGeom, spec.leftPartitionKey, spec);
        String rightSub = expandSubquery("rsrc", spec.rightTable, spec.rightKey, spec.rightGeom,
                spec.rightCoarseGeom, spec.rightPartitionKey, spec);

        StringBuilder select = new StringBuilder();
        select.append("SELECT l._key AS left_id, r._key AS right_id");
        if (spec.intersectionArea) {
            // 谓词短路：贵算子只在 SELECT，且仅命中行执行
            if (spec.dedup) {
                select.append(", MAX(st_area(st_intersection(l._geom, r._geom))) AS occupy_m2");
            } else {
                select.append(", st_area(st_intersection(l._geom, r._geom)) AS occupy_m2");
            }
        }
        select.append(" FROM (").append(leftSub).append(") l ")
                .append("JOIN (").append(rightSub).append(") r ON ");

        if (spec.leftPartitionKey != null && spec.rightPartitionKey != null) {
            select.append("l._part = r._part AND ");
        }
        if (spec.mode == Mode.GRID || spec.mode == Mode.PREPARED_GRID) {
            select.append("l.grid_id = r.grid_id AND ");
        }
        select.append("l.maxx >= r.minx AND l.minx <= r.maxx ")
                .append("AND l.maxy >= r.miny AND l.miny <= r.maxy ")
                .append("AND ").append(predCall);

        if (spec.dedup) {
            select.append(" GROUP BY l._key, r._key");
        }
        return select.toString();
    }

    private static String expandSubquery(
            String alias,
            String table,
            String keyCol,
            String geomCol,
            String coarseGeomCol,
            String partitionKey,
            Spec spec) {
        String qTable = quote(table);
        String qKey = quote(keyCol);
        String qGeom = quote(geomCol);
        String boundsGeom = coarseGeomCol != null ? quote(coarseGeomCol) : qGeom;
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ")
                .append(alias).append('.').append(qKey).append(" AS _key, ")
                .append(alias).append('.').append(qGeom).append(" AS _geom");

        if (partitionKey != null) {
            sb.append(", ").append(alias).append('.').append(quote(partitionKey)).append(" AS _part");
        }

        if (spec.mode == Mode.PREPARED || spec.mode == Mode.PREPARED_GRID) {
            // 已物化：直接投影列，跳过 st_xmin / LATERAL
            sb.append(", ").append(alias).append('.').append(quote(spec.minxCol)).append(" AS minx, ")
                    .append(alias).append('.').append(quote(spec.maxxCol)).append(" AS maxx, ")
                    .append(alias).append('.').append(quote(spec.minyCol)).append(" AS miny, ")
                    .append(alias).append('.').append(quote(spec.maxyCol)).append(" AS maxy");
            if (spec.mode == Mode.PREPARED_GRID) {
                sb.append(", ").append(alias).append('.').append(quote(spec.gridIdCol)).append(" AS grid_id");
            }
            sb.append(" FROM ").append(qTable).append(' ').append(alias);
            return sb.toString();
        }

        // 现场计算 bounds（可用 coarse 几何）
        sb.append(", st_xmin(").append(alias).append('.').append(boundsGeom).append(") AS minx, ")
                .append("st_xmax(").append(alias).append('.').append(boundsGeom).append(") AS maxx, ")
                .append("st_ymin(").append(alias).append('.').append(boundsGeom).append(") AS miny, ")
                .append("st_ymax(").append(alias).append('.').append(boundsGeom).append(") AS maxy");

        if (spec.mode == Mode.GRID) {
            sb.append(", g.grid_id AS grid_id FROM ")
                    .append(qTable).append(' ').append(alias)
                    .append(", LATERAL TABLE(st_grid_cells(")
                    .append(alias).append('.').append(boundsGeom)
                    .append(", CAST(").append(spec.cellSize).append(" AS DOUBLE)")
                    .append(", CAST(").append(spec.maxCells).append(" AS INT)")
                    .append(")) AS g(grid_id)");
        } else {
            sb.append(" FROM ").append(qTable).append(' ').append(alias);
        }
        return sb.toString();
    }

    private static void validate(Spec spec) {
        if (spec.leftTable == null || spec.leftTable.isEmpty()
                || spec.rightTable == null || spec.rightTable.isEmpty()) {
            throw new IllegalArgumentException("left_table and right_table are required");
        }
        if ((spec.mode == Mode.GRID) && spec.cellSize <= 0) {
            throw new IllegalArgumentException("cell_size must be > 0 for grid mode");
        }
        if (spec.maxCells <= 0) {
            throw new IllegalArgumentException("max_cells must be > 0");
        }
        if ((spec.leftPartitionKey == null) != (spec.rightPartitionKey == null)) {
            throw new IllegalArgumentException("left_partition_key and right_partition_key must both be set or both omitted");
        }
        String p = spec.predicate == null ? "" : spec.predicate.toLowerCase(Locale.ROOT);
        if (!PREDICATES.containsKey(p)) {
            throw new IllegalArgumentException(
                    "unsupported predicate: " + spec.predicate + ", allowed: " + PREDICATES.keySet());
        }
        spec.predicate = p;
    }

    static String quote(String ident) {
        if (ident == null) {
            throw new IllegalArgumentException("identifier is null");
        }
        if (!ident.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            throw new IllegalArgumentException("illegal identifier: " + ident);
        }
        return "`" + ident + "`";
    }
}
