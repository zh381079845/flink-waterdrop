package io.github.interestinglab.waterdrop.flink.transform;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 规则经纬度网格（Sedona 风格 spatial hash 的简化版）。
 * cellSize 单位与几何坐标一致（WGS84 下为度）。
 */
public final class SpatialGrid {

    /** 单个几何最多展开的格子数，防止超大包络拖垮 JOIN。 */
    public static final int DEFAULT_MAX_CELLS = 10_000;

    private SpatialGrid() {
    }

    public static String cellId(double x, double y, double cellSize) {
        long ix = (long) Math.floor(x / cellSize);
        long iy = (long) Math.floor(y / cellSize);
        return ix + "_" + iy;
    }

    /**
     * 点或质心所在的主格子。
     */
    public static String primaryCellId(Geometry geometry, double cellSize) {
        if (geometry == null || geometry.isEmpty() || cellSize <= 0) {
            return null;
        }
        org.locationtech.jts.geom.Coordinate c =
                "Point".equals(geometry.getGeometryType())
                        ? geometry.getCoordinate()
                        : geometry.getCentroid().getCoordinate();
        if (c == null) {
            return null;
        }
        return cellId(c.x, c.y, cellSize);
    }

    /**
     * 包络覆盖到的全部格子 ID（闭区间）。
     */
    public static List<String> cellsCovering(Geometry geometry, double cellSize) {
        return cellsCovering(geometry, cellSize, DEFAULT_MAX_CELLS);
    }

    public static List<String> cellsCovering(Geometry geometry, double cellSize, int maxCells) {
        if (geometry == null || geometry.isEmpty() || cellSize <= 0) {
            return Collections.emptyList();
        }
        Envelope env = geometry.getEnvelopeInternal();
        long minIx = (long) Math.floor(env.getMinX() / cellSize);
        long maxIx = (long) Math.floor(env.getMaxX() / cellSize);
        long minIy = (long) Math.floor(env.getMinY() / cellSize);
        long maxIy = (long) Math.floor(env.getMaxY() / cellSize);

        long width = maxIx - minIx + 1;
        long height = maxIy - minIy + 1;
        if (width <= 0 || height <= 0) {
            return Collections.emptyList();
        }
        if (width > Integer.MAX_VALUE || height > Integer.MAX_VALUE
                || width * height > maxCells) {
            // 过大则退化为质心单格，避免爆炸；调用方应换更大 cellSize
            String primary = primaryCellId(geometry, cellSize);
            return primary == null ? Collections.<String>emptyList() : Collections.singletonList(primary);
        }

        List<String> cells = new ArrayList<String>((int) (width * height));
        for (long ix = minIx; ix <= maxIx; ix++) {
            for (long iy = minIy; iy <= maxIy; iy++) {
                cells.add(ix + "_" + iy);
            }
        }
        return cells;
    }

    public static String cellsCoveringCsv(Geometry geometry, double cellSize) {
        return cellsCoveringCsv(geometry, cellSize, DEFAULT_MAX_CELLS);
    }

    public static String cellsCoveringCsv(Geometry geometry, double cellSize, int maxCells) {
        List<String> cells = cellsCovering(geometry, cellSize, maxCells);
        if (cells.isEmpty()) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < cells.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(cells.get(i));
        }
        return sb.toString();
    }
}
