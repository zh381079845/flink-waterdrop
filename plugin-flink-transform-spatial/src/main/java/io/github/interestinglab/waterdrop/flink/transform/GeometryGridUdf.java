package io.github.interestinglab.waterdrop.flink.transform;

import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;

import java.util.List;

/**
 * Sedona 风格空间哈希：geohash / 单格 / 多格展开（UDTF）。
 */
public final class GeometryGridUdf {

    private GeometryGridUdf() {
    }

    /**
     * st_geohash(geom, precision)：点或质心的 geohash。
     */
    public static class GeoHashFn extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public String eval(String wkt, Integer precision) {
            return hash(WktUtils.read(wkt), precision);
        }

        public String eval(Geometry geometry, Integer precision) {
            return hash(geometry, precision);
        }

        private String hash(Geometry geometry, Integer precision) {
            if (geometry == null || geometry.isEmpty() || precision == null || precision <= 0) {
                return null;
            }
            Coordinate c = "Point".equals(geometry.getGeometryType())
                    ? geometry.getCoordinate()
                    : geometry.getCentroid().getCoordinate();
            if (c == null) {
                return null;
            }
            return GeoHash.encode(c.x, c.y, precision);
        }
    }

    /**
     * st_grid_id(geom, cellSizeDegrees)：质心/点所在主格子，格式 ix_iy。
     */
    public static class GridId extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public String eval(String wkt, Double cellSizeDegrees) {
            return id(WktUtils.read(wkt), cellSizeDegrees);
        }

        public String eval(Geometry geometry, Double cellSizeDegrees) {
            return id(geometry, cellSizeDegrees);
        }

        private String id(Geometry geometry, Double cellSizeDegrees) {
            if (cellSizeDegrees == null) {
                return null;
            }
            return SpatialGrid.primaryCellId(geometry, cellSizeDegrees);
        }
    }

    /**
     * st_grid_ids(geom, cellSizeDegrees)：包络覆盖格子，逗号分隔（便于落表）。
     */
    public static class GridIds extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public String eval(String wkt, Double cellSizeDegrees) {
            return ids(WktUtils.read(wkt), cellSizeDegrees, SpatialGrid.DEFAULT_MAX_CELLS);
        }

        public String eval(String wkt, Double cellSizeDegrees, Integer maxCells) {
            int cap = maxCells == null ? SpatialGrid.DEFAULT_MAX_CELLS : maxCells;
            return ids(WktUtils.read(wkt), cellSizeDegrees, cap);
        }

        public String eval(Geometry geometry, Double cellSizeDegrees) {
            return ids(geometry, cellSizeDegrees, SpatialGrid.DEFAULT_MAX_CELLS);
        }

        private String ids(Geometry geometry, Double cellSizeDegrees, int maxCells) {
            if (cellSizeDegrees == null) {
                return null;
            }
            return SpatialGrid.cellsCoveringCsv(geometry, cellSizeDegrees, maxCells);
        }
    }

    /**
     * st_grid_cells(geom, cellSize[, maxCells])：UDTF 展开。
     */
    public static class GridCells extends TableFunction<String> {
        private static final long serialVersionUID = 1L;

        public void eval(String wkt, Double cellSizeDegrees) {
            emit(wkt, cellSizeDegrees, SpatialGrid.DEFAULT_MAX_CELLS);
        }

        public void eval(String wkt, Double cellSizeDegrees, Integer maxCells) {
            int cap = maxCells == null ? SpatialGrid.DEFAULT_MAX_CELLS : maxCells;
            emit(wkt, cellSizeDegrees, cap);
        }

        private void emit(String wkt, Double cellSizeDegrees, int maxCells) {
            if (cellSizeDegrees == null) {
                return;
            }
            List<String> cells = SpatialGrid.cellsCovering(WktUtils.read(wkt), cellSizeDegrees, maxCells);
            for (String cell : cells) {
                collect(cell);
            }
        }
    }
}
