package io.github.interestinglab.waterdrop.flink.transform;

/**
 * 入库/预处理物化 SQL：bounds + 主格 / 或展开多格。
 */
public final class SpatialMaterializeSqlBuilder {

    public enum Mode {
        /** 一行一要素：minx/maxx/miny/maxy + grid_id(主格) */
        BOUNDS,
        /** 一行一格：LATERAL 展开 grid_id + bounds */
        EXPLODE_GRID
    }

    public static final class Spec {
        public String sourceTable;
        public String geomCol = "geometry";
        public String keyCol = "id";
        public double cellSize = 0.05;
        public int maxCells = SpatialGrid.DEFAULT_MAX_CELLS;
        public Mode mode = Mode.BOUNDS;
        /** 可选：抽稀后再算 bounds/grid，原 geometry 保留 */
        public Double simplifyTolerance;
        /** 额外透传列（如 adcode），逗号分隔简单标识符 */
        public String extraColumns;
    }

    private SpatialMaterializeSqlBuilder() {
    }

    public static String build(Spec spec) {
        if (spec.sourceTable == null || spec.sourceTable.isEmpty()) {
            throw new IllegalArgumentException("source_table required");
        }
        String t = SpatialJoinSqlBuilder.quote(spec.sourceTable);
        String g = SpatialJoinSqlBuilder.quote(spec.geomCol);
        String k = SpatialJoinSqlBuilder.quote(spec.keyCol);
        String geomExpr = "s." + g;
        if (spec.simplifyTolerance != null && spec.simplifyTolerance >= 0) {
            geomExpr = "st_simplify(s." + g + ", CAST(" + spec.simplifyTolerance + " AS DOUBLE))";
        }

        StringBuilder extras = new StringBuilder();
        if (spec.extraColumns != null && !spec.extraColumns.trim().isEmpty()) {
            for (String col : spec.extraColumns.split(",")) {
                String c = col.trim();
                if (!c.isEmpty()) {
                    extras.append(", s.").append(SpatialJoinSqlBuilder.quote(c));
                }
            }
        }

        if (spec.mode == Mode.BOUNDS) {
            return "SELECT s." + k + " AS id, s." + g + " AS geometry"
                    + extras
                    + ", st_xmin(" + geomExpr + ") AS minx"
                    + ", st_xmax(" + geomExpr + ") AS maxx"
                    + ", st_ymin(" + geomExpr + ") AS miny"
                    + ", st_ymax(" + geomExpr + ") AS maxy"
                    + ", st_grid_id(" + geomExpr + ", CAST(" + spec.cellSize + " AS DOUBLE)) AS grid_id"
                    + " FROM " + t + " s";
        }

        return "SELECT s." + k + " AS id, s." + g + " AS geometry"
                + extras
                + ", st_xmin(" + geomExpr + ") AS minx"
                + ", st_xmax(" + geomExpr + ") AS maxx"
                + ", st_ymin(" + geomExpr + ") AS miny"
                + ", st_ymax(" + geomExpr + ") AS maxy"
                + ", g.grid_id AS grid_id"
                + " FROM " + t + " s, LATERAL TABLE(st_grid_cells("
                + geomExpr + ", CAST(" + spec.cellSize + " AS DOUBLE), CAST(" + spec.maxCells + " AS INT)"
                + ")) AS g(grid_id)";
    }
}
