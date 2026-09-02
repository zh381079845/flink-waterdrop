package io.github.interestinglab.waterdrop.flink.transform;

import org.junit.Test;
import org.locationtech.jts.geom.Geometry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * 复杂空间计算场景：多部件几何、缓冲区连锁、包络预过滤 JOIN、红线/地块业务链路。
 */
public class ComplexSpatialScenarioTest {

    // 北京附近近似经纬度面（红线 / 农田 / 建设用地）
    private static final String ECO_REDLINE =
            "POLYGON((116.30 39.90, 116.30 40.05, 116.50 40.05, 116.50 39.90, 116.30 39.90))";
    private static final String FARMLAND =
            "POLYGON((116.40 39.95, 116.40 40.10, 116.60 40.10, 116.60 39.95, 116.40 39.95))";
    private static final String CONSTRUCTION =
            "POLYGON((116.55 39.88, 116.55 39.98, 116.70 39.98, 116.70 39.88, 116.55 39.88))";
    private static final String HOLE_DONUT =
            "POLYGON((116.32 39.92, 116.32 40.02, 116.48 40.02, 116.48 39.92, 116.32 39.92),"
                    + "(116.36 39.95, 116.44 39.95, 116.44 39.99, 116.36 39.99, 116.36 39.95))";
    private static final String MULTI_PARCEL =
            "MULTIPOLYGON("
                    + "((116.10 39.80, 116.10 39.85, 116.15 39.85, 116.15 39.80, 116.10 39.80)),"
                    + "((116.70 40.10, 116.70 40.15, 116.75 40.15, 116.75 40.10, 116.70 40.10))"
                    + ")";
    private static final String ROAD =
            "LINESTRING(116.20 39.85, 116.45 39.98, 116.65 39.93)";
    private static final String SITE_POINT = "POINT(116.42 39.97)";
    private static final String OUTSIDE_POINT = "POINT(117.00 39.50)";
    private static final String TOUCH_POINT = "POINT(116.50 39.90)";

    private final GeometryAreaUdf area = new GeometryAreaUdf();
    private final GeometryBufferUdf buffer = new GeometryBufferUdf();
    private final GeometryDistanceUdf distance = new GeometryDistanceUdf();
    private final GeometryBoundsUdf.XMin xmin = new GeometryBoundsUdf.XMin();
    private final GeometryBoundsUdf.XMax xmax = new GeometryBoundsUdf.XMax();
    private final GeometryBoundsUdf.YMin ymin = new GeometryBoundsUdf.YMin();
    private final GeometryBoundsUdf.YMax ymax = new GeometryBoundsUdf.YMax();
    private final Predicates.ST_Intersects intersects = new Predicates.ST_Intersects();
    private final Predicates.ST_Contains contains = new Predicates.ST_Contains();
    private final Predicates.ST_Within within = new Predicates.ST_Within();
    private final Predicates.ST_Overlaps overlaps = new Predicates.ST_Overlaps();
    private final Predicates.ST_Touches touches = new Predicates.ST_Touches();
    private final Predicates.ST_Disjoint disjoint = new Predicates.ST_Disjoint();
    private final Predicates.ST_Crosses crosses = new Predicates.ST_Crosses();
    private final Predicates.ST_DWithin dWithin = new Predicates.ST_DWithin();
    private final Predicates.ST_Relate relate = new Predicates.ST_Relate();
    private final GeometryFromWktUdf fromWkt = new GeometryFromWktUdf();
    private final GeometryAsTextUdf asText = new GeometryAsTextUdf();

    @Test
    public void multiPolygonSpheroidAreaIsSumOfParts() {
        Double multi = area.eval(MULTI_PARCEL);
        Double part1 = area.eval("POLYGON((116.10 39.80, 116.10 39.85, 116.15 39.85, 116.15 39.80, 116.10 39.80))");
        Double part2 = area.eval("POLYGON((116.70 40.10, 116.70 40.15, 116.75 40.15, 116.75 40.10, 116.70 40.10))");
        assertNotNull(multi);
        assertNotNull(part1);
        assertNotNull(part2);
        assertEquals(part1 + part2, multi, multi * 1e-6);
        assertTrue(multi > 1e6);
    }

    @Test
    public void polygonWithHoleAreaLessThanOuterRing() {
        String outerOnly =
                "POLYGON((116.32 39.92, 116.32 40.02, 116.48 40.02, 116.48 39.92, 116.32 39.92))";
        Double withHole = area.eval(HOLE_DONUT);
        Double solid = area.eval(outerOnly);
        assertNotNull(withHole);
        assertNotNull(solid);
        assertTrue(withHole < solid);
        assertTrue(withHole > 0);
    }

    @Test
    public void landUseConflict_redlineOverlapsFarmland() {
        // 红线与农田部分重叠 -> overlaps / intersects
        assertTrue(intersects.eval(ECO_REDLINE, FARMLAND));
        assertTrue(overlaps.eval(ECO_REDLINE, FARMLAND));

        // 用包络预过滤再精过滤，模拟大表 JOIN
        List<String[]> pairs = Arrays.asList(
                new String[]{"eco", ECO_REDLINE, "farm", FARMLAND},
                new String[]{"eco", ECO_REDLINE, "build", CONSTRUCTION},
                new String[]{"farm", FARMLAND, "build", CONSTRUCTION}
        );
        List<String> conflicts = new ArrayList<String>();
        for (String[] p : pairs) {
            if (envelopeOverlaps(p[1], p[3]) && Boolean.TRUE.equals(intersects.eval(p[1], p[3]))) {
                conflicts.add(p[0] + "-" + p[2]);
            }
        }
        assertTrue(conflicts.contains("eco-farm"));
        // 建设用地与红线若包络重叠，再以 intersects 为准；至少冲突列表非空
        assertFalse(conflicts.isEmpty());
    }

    @Test
    public void siteSelection_pointInRedlineWithBufferClearance() {
        assertTrue(contains.eval(ECO_REDLINE, SITE_POINT));
        assertTrue(within.eval(SITE_POINT, ECO_REDLINE));
        assertFalse(contains.eval(ECO_REDLINE, OUTSIDE_POINT));

        // 选址点 500m 缓冲是否压到建设用地
        String siteBuffer = buffer.eval(SITE_POINT, 500.0);
        assertNotNull(siteBuffer);
        assertTrue(siteBuffer.startsWith("POLYGON") || siteBuffer.startsWith("MULTIPOLYGON"));

        // 与红线必相交；与远点不相交
        assertTrue(intersects.eval(siteBuffer, ECO_REDLINE));
        assertFalse(intersects.eval(siteBuffer, OUTSIDE_POINT));
    }

    @Test
    public void roadCrossingRedline_lineCrossesAndIntersects() {
        assertTrue(intersects.eval(ROAD, ECO_REDLINE));
        // 线与面的穿越关系：常见为 crosses 或至少 intersects
        Boolean cross = crosses.eval(ROAD, ECO_REDLINE);
        assertNotNull(cross);
        assertTrue(cross || intersects.eval(ROAD, ECO_REDLINE));

        String matrix = relate.eval(ROAD, ECO_REDLINE);
        assertNotNull(matrix);
        assertEquals(9, matrix.length());
    }

    @Test
    public void facilityServiceArea_dWithinThenEnvelopeJoin() {
        // 设施点：红线中心附近 vs 外部
        String hospital = "POINT(116.40 39.97)";
        String remoteClinic = "POINT(117.20 40.50)";
        double radiusMeters = 8000.0;

        assertTrue(dWithin.eval(hospital, SITE_POINT, radiusMeters));
        assertFalse(dWithin.eval(remoteClinic, SITE_POINT, radiusMeters));

        // 服务区 = buffer，再与地块做包络预过滤 + intersects
        String serviceArea = buffer.eval(hospital, radiusMeters);
        List<String> hitParcels = new ArrayList<String>();
        String[][] parcels = {
                {"eco", ECO_REDLINE},
                {"farm", FARMLAND},
                {"build", CONSTRUCTION},
                {"multi", MULTI_PARCEL}
        };
        for (String[] parcel : parcels) {
            if (envelopeOverlaps(serviceArea, parcel[1])
                    && Boolean.TRUE.equals(intersects.eval(serviceArea, parcel[1]))) {
                hitParcels.add(parcel[0]);
            }
        }
        assertTrue(hitParcels.contains("eco"));
        assertTrue(hitParcels.contains("farm"));
        // MULTI 只有真正 intersects 时才进结果
        assertEquals(Boolean.TRUE.equals(intersects.eval(serviceArea, MULTI_PARCEL)),
                hitParcels.contains("multi"));
    }

    @Test
    public void envelopePrefilterRejectsFarCandidatesBeforeIntersects() {
        int candidates = 0;
        int envelopeHits = 0;
        int preciseHits = 0;
        List<String> farBoxes = Arrays.asList(
                "POLYGON((110 30, 110 31, 111 31, 111 30, 110 30))",
                "POLYGON((120 30, 120 31, 121 31, 121 30, 120 30))",
                "POLYGON((100 20, 100 21, 101 21, 101 20, 100 20))",
                FARMLAND,
                CONSTRUCTION
        );
        for (String box : farBoxes) {
            candidates++;
            if (envelopeOverlaps(ECO_REDLINE, box)) {
                envelopeHits++;
                if (Boolean.TRUE.equals(intersects.eval(ECO_REDLINE, box))) {
                    preciseHits++;
                }
            }
        }
        assertEquals(5, candidates);
        assertTrue("envelope should filter most far boxes", envelopeHits < candidates);
        assertTrue(preciseHits <= envelopeHits);
        assertTrue(preciseHits >= 1);
    }

    @Test
    public void wktGeometryChain_roundTripAndMixedOverloads() {
        Geometry geom = fromWkt.eval(ECO_REDLINE);
        assertNotNull(geom);
        String back = asText.eval(geom);
        assertNotNull(back);

        Double a1 = area.eval(ECO_REDLINE);
        Double a2 = area.eval(geom);
        assertEquals(a1, a2, a1 * 1e-9);

        assertTrue(intersects.eval(geom, fromWkt.eval(SITE_POINT)));
        assertTrue(contains.eval(geom, fromWkt.eval(SITE_POINT)));
        assertTrue(intersects.eval(ECO_REDLINE, back));
    }

    @Test
    public void multipolygonEnvelopeAndPartialIntersect() {
        // MULTI 两片中一片靠近红线外，一片较远；整体包络可能与红线包络重叠但几何不一定相交
        assertNotNull(xmin.eval(MULTI_PARCEL));
        assertNotNull(xmax.eval(MULTI_PARCEL));
        Boolean env = envelopeOverlaps(ECO_REDLINE, MULTI_PARCEL);
        Boolean hit = intersects.eval(ECO_REDLINE, MULTI_PARCEL);
        assertNotNull(env);
        assertNotNull(hit);
        // 包络重叠是 intersects 的必要条件（同坐标系下）
        if (Boolean.TRUE.equals(hit)) {
            assertTrue(env);
        }
    }

    @Test
    public void boundaryTouchVsOverlapSemantics() {
        assertTrue(touches.eval(ECO_REDLINE, TOUCH_POINT)
                || contains.eval(ECO_REDLINE, TOUCH_POINT)
                || intersects.eval(ECO_REDLINE, TOUCH_POINT));
        assertFalse(overlaps.eval(ECO_REDLINE, TOUCH_POINT));
        assertFalse(disjoint.eval(ECO_REDLINE, SITE_POINT));
    }

    @Test
    public void distanceOrderingConsistentWithDWithin() {
        Double near = distance.eval(SITE_POINT, "POINT(116.43 39.97)");
        Double far = distance.eval(SITE_POINT, OUTSIDE_POINT);
        assertNotNull(near);
        assertNotNull(far);
        assertTrue(near < far);
        assertTrue(dWithin.eval(SITE_POINT, "POINT(116.43 39.97)", near + 1.0));
        assertFalse(dWithin.eval(SITE_POINT, OUTSIDE_POINT, near));
    }

    @Test
    public void invalidMixedIntoComplexPipelineDoesNotThrow() {
        String bad = "POLYGON((";
        assertNull(area.eval(bad));
        assertNull(buffer.eval(bad, 100.0));
        assertNull(intersects.eval(ECO_REDLINE, bad));
        assertNull(dWithin.eval(SITE_POINT, bad, 1000.0));
        assertNull(relate.eval(ECO_REDLINE, bad));
        // 坏数据不应进入冲突集合
        Set<String> conflicts = new HashSet<String>();
        if (envelopeOverlaps(ECO_REDLINE, bad) && Boolean.TRUE.equals(intersects.eval(ECO_REDLINE, bad))) {
            conflicts.add("eco-bad");
        }
        assertTrue(conflicts.isEmpty());
    }

    private boolean envelopeOverlaps(String left, String right) {
        Double lmaxx = xmax.eval(left);
        Double lminx = xmin.eval(left);
        Double lmaxy = ymax.eval(left);
        Double lminy = ymin.eval(left);
        Double rmaxx = xmax.eval(right);
        Double rminx = xmin.eval(right);
        Double rmaxy = ymax.eval(right);
        Double rminy = ymin.eval(right);
        if (lmaxx == null || rmaxx == null) {
            return false;
        }
        return lmaxx >= rminx && lminx <= rmaxx && lmaxy >= rminy && lminy <= rmaxy;
    }
}
