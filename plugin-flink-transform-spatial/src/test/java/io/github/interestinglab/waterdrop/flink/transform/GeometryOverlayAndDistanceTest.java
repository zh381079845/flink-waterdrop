package io.github.interestinglab.waterdrop.flink.transform;

import org.junit.Test;
import org.locationtech.jts.geom.Geometry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * st_distance 最近点语义 + st_intersection / difference / union。
 */
public class GeometryOverlayAndDistanceTest {

    private static final String PIPE =
            "LINESTRING(116.50 39.88, 116.55 39.90, 116.60 39.91)";
    private static final String BUILDING_NEAR_PIPE =
            "POLYGON((116.549 39.899, 116.549 39.901, 116.551 39.901, 116.551 39.899, 116.549 39.899))";
    private static final String POINT_ON_PIPE = "POINT(116.55 39.90)";
    private static final String LEFT =
            "POLYGON((116.40 39.90, 116.40 40.00, 116.50 40.00, 116.50 39.90, 116.40 39.90))";
    private static final String RIGHT =
            "POLYGON((116.45 39.95, 116.45 40.05, 116.55 40.05, 116.55 39.95, 116.45 39.95))";
    private static final String FAR =
            "POLYGON((120.00 31.00, 120.00 31.10, 120.10 31.10, 120.10 31.00, 120.00 31.00))";

    private final GeometryDistanceUdf distance = new GeometryDistanceUdf();
    private final GeometryAreaUdf area = new GeometryAreaUdf();
    private final GeometryOverlayUdf.Intersection intersection = new GeometryOverlayUdf.Intersection();
    private final GeometryOverlayUdf.Difference difference = new GeometryOverlayUdf.Difference();
    private final GeometryOverlayUdf.Union union = new GeometryOverlayUdf.Union();
    private final Predicates.ST_Intersects intersects = new Predicates.ST_Intersects();
    private final Predicates.ST_DWithin dWithin = new Predicates.ST_DWithin();

    @Test
    public void stDistanceUsesNearestPointsNotCentroid() {
        Double near = distance.eval(PIPE, BUILDING_NEAR_PIPE);
        assertNotNull(near);
        // 旧质心语义约 300m+；最近点应远小于 50m
        assertTrue("nearest distance too large: " + near, near < 50.0);

        Double onLine = distance.eval(PIPE, POINT_ON_PIPE);
        assertNotNull(onLine);
        assertEquals(0.0, onLine, 1e-3);

        assertTrue(dWithin.eval(PIPE, BUILDING_NEAR_PIPE, 50.0));
        assertFalse(dWithin.eval(PIPE, FAR, 1000.0));
    }

    @Test
    public void stIntersectionOccupyArea() {
        String hit = intersection.eval(LEFT, RIGHT);
        assertNotNull(hit);
        assertTrue(intersects.eval(LEFT, RIGHT));
        Double occupy = area.eval(hit);
        assertNotNull(occupy);
        assertTrue(occupy > 1e5);
        assertTrue(occupy < area.eval(LEFT));
        assertTrue(occupy < area.eval(RIGHT));

        String empty = intersection.eval(LEFT, FAR);
        assertNotNull(empty);
        Geometry g = WktUtils.read(empty);
        assertNotNull(g);
        assertTrue(g.isEmpty());
        assertEquals(0.0, area.eval(empty), 1e-9);
    }

    @Test
    public void stDifferenceNetLand() {
        String net = difference.eval(LEFT, RIGHT);
        assertNotNull(net);
        Double leftArea = area.eval(LEFT);
        Double netArea = area.eval(net);
        Double occupy = area.eval(intersection.eval(LEFT, RIGHT));
        assertNotNull(netArea);
        assertNotNull(occupy);
        // difference ≈ left - intersection（椭球有数值误差）
        assertEquals(leftArea - occupy, netArea, leftArea * 1e-3);
        assertTrue(netArea < leftArea);
    }

    @Test
    public void stUnionMergesOverlap() {
        Double leftA = area.eval(LEFT);
        Double rightA = area.eval(RIGHT);
        Double occupy = area.eval(intersection.eval(LEFT, RIGHT));
        String merged = union.eval(LEFT, RIGHT);
        Double unionA = area.eval(merged);
        assertNotNull(unionA);
        assertEquals(leftA + rightA - occupy, unionA, unionA * 1e-3);
        assertTrue(unionA > leftA);
        assertTrue(unionA > rightA);
    }

    @Test
    public void stUnionUnaryMultiPolygon() {
        String multi =
                "MULTIPOLYGON("
                        + "((116.10 39.80, 116.10 39.85, 116.15 39.85, 116.15 39.80, 116.10 39.80)),"
                        + "((116.70 40.10, 116.70 40.15, 116.75 40.15, 116.75 40.10, 116.70 40.10))"
                        + ")";
        String out = union.eval(multi);
        assertNotNull(out);
        assertEquals(area.eval(multi), area.eval(out), area.eval(multi) * 1e-6);
    }

    @Test
    public void overlayNullSafe() {
        assertNull(intersection.eval(null, LEFT));
        assertNull(difference.eval(LEFT, "BAD"));
        assertNull(union.eval("BAD", LEFT));
        assertNull(distance.eval(PIPE, "BAD"));
    }

    @Test
    public void geometryOverloadsWork() {
        Geometry a = WktUtils.read(LEFT);
        Geometry b = WktUtils.read(RIGHT);
        Geometry hit = intersection.eval(a, b);
        assertNotNull(hit);
        assertFalse(hit.isEmpty());
        assertEquals(area.eval(LEFT), area.eval(a), 1.0);
        assertTrue(distance.eval(a, b) >= 0);
    }
}
