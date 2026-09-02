package io.github.interestinglab.waterdrop.flink.transform;

import org.junit.Test;
import org.locationtech.jts.geom.Geometry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * P0 金标契约测试：固定 WKT → 本引擎承诺输出（非 Sedona CI 对跑）。
 * <p>
 * 数值金标按 Waterdrop 语义标定：面积/距离默认椭球米；buffer 默认 3857 米。
 * 与 Sedona {@code Geometry} 平面默认值不可直接混比——见 docs/sedona-sql-compatibility.md。
 */
public class SpatialGoldenContractTest {

    private static final String BEIJING = "POINT(116.397428 39.90923)";
    private static final String SHANGHAI = "POINT(121.473667 31.230525)";
    private static final String DEGREE_SQUARE =
            "POLYGON((116 39, 116 40, 117 40, 117 39, 116 39))";
    private static final String INNER =
            "POLYGON((116.2 39.2, 116.2 39.4, 116.4 39.4, 116.4 39.2, 116.2 39.2))";
    private static final String PIPE =
            "LINESTRING(116.50 39.88, 116.55 39.90, 116.60 39.91)";
    private static final String ON_PIPE = "POINT(116.55 39.90)";

    private final GeometryAreaUdf area = new GeometryAreaUdf();
    private final GeometryDistanceUdf distance = new GeometryDistanceUdf();
    private final GeometryBufferUdf buffer = new GeometryBufferUdf();
    private final GeometryOverlayUdf.Intersection intersection = new GeometryOverlayUdf.Intersection();
    private final GeometryBoundsUdf.EnvelopeWkt envelope = new GeometryBoundsUdf.EnvelopeWkt();
    private final GeometryGridUdf.GeoHashFn geohash = new GeometryGridUdf.GeoHashFn();
    private final Predicates.ST_Intersects intersects = new Predicates.ST_Intersects();
    private final Predicates.ST_Contains contains = new Predicates.ST_Contains();
    private final Predicates.ST_DWithin dWithin = new Predicates.ST_DWithin();

    @Test
    public void golden_beijingShanghaiDistanceSpheroidMeters() {
        // GeographicLib WGS84；约 1067km（非 Sedona 平面度距离）
        Double meters = distance.eval(BEIJING, SHANGHAI);
        assertNotNull(meters);
        assertEquals(1_067_800.0, meters, 5_000.0);
        assertFalse(dWithin.eval(BEIJING, SHANGHAI, 1000.0));
        assertTrue(dWithin.eval(BEIJING, SHANGHAI, 2_000_000.0));
    }

    @Test
    public void golden_degreeSquareAreaSpheroidNotPlanar() {
        Double spheroid = area.eval(DEGREE_SQUARE);
        Double planar = area.eval(DEGREE_SQUARE, false);
        assertEquals(1.0, planar, 1e-9);
        // ~1.02e10 m² 量级（北京纬度 1°×1°）
        assertTrue(spheroid > 9.5e9 && spheroid < 1.1e10);
    }

    @Test
    public void golden_predicatesSquareContainsInner() {
        assertTrue(intersects.eval(DEGREE_SQUARE, INNER));
        assertTrue(contains.eval(DEGREE_SQUARE, INNER));
        assertFalse(contains.eval(INNER, DEGREE_SQUARE));
    }

    @Test
    public void golden_pointOnLineDistanceZero() {
        assertEquals(0.0, distance.eval(PIPE, ON_PIPE), 1e-3);
        assertTrue(dWithin.eval(PIPE, ON_PIPE, 1.0));
    }

    @Test
    public void golden_bufferMetersProducesPolygonAroundPoint() {
        String buf = buffer.eval(BEIJING, 1000.0);
        assertNotNull(buf);
        assertTrue(buf.startsWith("POLYGON"));
        Double xmin = new GeometryBoundsUdf.XMin().eval(buf);
        Double xmax = new GeometryBoundsUdf.XMax().eval(buf);
        assertTrue(xmin < 116.397428 && xmax > 116.397428);
        // ~1000m 在北京纬度约 0.009–0.012°
        assertTrue((xmax - xmin) > 0.01 && (xmax - xmin) < 0.03);
    }

    @Test
    public void golden_intersectionNonEmptyAndEnvelope() {
        String hit = intersection.eval(DEGREE_SQUARE, INNER);
        assertNotNull(hit);
        assertFalse(WktUtils.read(hit).isEmpty());
        assertTrue(area.eval(hit) > 1e8);

        String env = envelope.eval(INNER);
        assertNotNull(env);
        assertEquals(116.2, new GeometryBoundsUdf.XMin().eval(env), 1e-8);
        assertEquals(116.4, new GeometryBoundsUdf.XMax().eval(env), 1e-8);
    }

    @Test
    public void golden_geohashBeijingPrecision6() {
        String h = geohash.eval(BEIJING, 6);
        assertNotNull(h);
        assertEquals(6, h.length());
        // 固定金标：同一点同一精度必须稳定
        assertEquals(h, geohash.eval(BEIJING, 6));
        assertTrue(h.matches("[0-9bcdefghjkmnpqrstuvwxyz]+"));
    }

    @Test
    public void golden_invalidWktNullSafe() {
        assertTrue(intersects.eval("BAD", DEGREE_SQUARE) == null
                || Boolean.FALSE.equals(intersects.eval("BAD", DEGREE_SQUARE))
                || intersects.eval("BAD", DEGREE_SQUARE) == null);
        assertTrue(area.eval("NOT_WKT") == null);
        assertTrue(distance.eval(BEIJING, "XX") == null);
    }
}
