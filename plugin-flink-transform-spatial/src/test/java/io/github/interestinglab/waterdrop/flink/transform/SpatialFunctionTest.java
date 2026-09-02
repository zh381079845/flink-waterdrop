package io.github.interestinglab.waterdrop.flink.transform;

import org.junit.Test;
import org.locationtech.jts.geom.Geometry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SpatialFunctionTest {

    private static final String DEGREE_SQUARE =
            "POLYGON((116 39, 116 40, 117 40, 117 39, 116 39))";
    private static final String INNER =
            "POLYGON((116.2 39.2, 116.2 39.4, 116.4 39.4, 116.4 39.2, 116.2 39.2))";
    private static final String FAR =
            "POLYGON((120 31, 120 32, 121 32, 121 31, 120 31))";
    private static final String BEIJING = "POINT(116.397428 39.90923)";
    private static final String SHANGHAI = "POINT(121.473667 31.230525)";

    @Test
    public void stAreaUsesSpheroidByDefault() {
        GeometryAreaUdf area = new GeometryAreaUdf();
        Double spheroid = area.eval(DEGREE_SQUARE);
        Double planar = area.eval(DEGREE_SQUARE, false);
        assertNotNull(spheroid);
        assertNotNull(planar);
        assertEquals(1.0, planar, 1e-6);
        assertTrue("spheroid area should be m2, got " + spheroid, spheroid > 1e9);
        assertTrue(spheroid > planar * 1e6);
    }

    @Test
    public void stAreaGeometryOverloadMatchesWkt() {
        GeometryAreaUdf area = new GeometryAreaUdf();
        Geometry geom = WktUtils.read(DEGREE_SQUARE);
        assertEquals(area.eval(DEGREE_SQUARE), area.eval(geom), 1.0);
    }

    @Test
    public void stIntersectsAndDisjoint() {
        GeometryIntersectsUdf intersects = new GeometryIntersectsUdf();
        assertTrue(intersects.eval(DEGREE_SQUARE, INNER));
        assertFalse(intersects.eval(DEGREE_SQUARE, FAR));
        assertNull(intersects.eval(DEGREE_SQUARE, "NOT-WKT"));
    }

    @Test
    public void stContainsAndWithin() {
        GeometryContainsUdf contains = new GeometryContainsUdf();
        GeometryWithinUdf within = new GeometryWithinUdf();
        assertTrue(contains.eval(DEGREE_SQUARE, INNER));
        assertFalse(contains.eval(INNER, DEGREE_SQUARE));
        assertTrue(within.eval(INNER, DEGREE_SQUARE));
        assertFalse(within.eval(DEGREE_SQUARE, INNER));
    }

    @Test
    public void stDistanceAndDWithin() {
        GeometryDistanceUdf distance = new GeometryDistanceUdf();
        GeometryDWithinUdf dwithin = new GeometryDWithinUdf();
        Double meters = distance.eval(BEIJING, SHANGHAI);
        assertNotNull(meters);
        assertTrue("Beijing-Shanghai should be ~1000km, got " + meters, meters > 900_000 && meters < 1_200_000);
        assertFalse(dwithin.eval(BEIJING, SHANGHAI, 1000.0));
        assertTrue(dwithin.eval(BEIJING, SHANGHAI, 2_000_000.0));
        assertTrue(dwithin.eval(BEIJING, BEIJING, 1.0));
    }

    @Test
    public void stGeomFromWktAndAsTextRoundTrip() {
        GeometryFromWktUdf fromWkt = new GeometryFromWktUdf();
        GeometryAsTextUdf asText = new GeometryAsTextUdf();
        Geometry geom = fromWkt.eval(BEIJING);
        assertNotNull(geom);
        String wkt = asText.eval(geom);
        assertNotNull(wkt);
        assertTrue(wkt.contains("116.397428"));
        assertEquals(wkt, asText.eval(wkt));
    }

    @Test
    public void stBufferReturnsPolygon() {
        GeometryBufferUdf buffer = new GeometryBufferUdf();
        String simple = buffer.eval(BEIJING, 0.01, true);
        assertNotNull(simple);
        assertTrue(simple.startsWith("POLYGON"));
        String meters = buffer.eval(BEIJING, 1000.0);
        assertNotNull(meters);
        assertTrue(meters.startsWith("POLYGON"));
        Double xmin = new GeometryBoundsUdf.XMin().eval(meters);
        Double xmax = new GeometryBoundsUdf.XMax().eval(meters);
        assertNotNull(xmin);
        assertNotNull(xmax);
        assertTrue(xmin < 116.397428 && xmax > 116.397428);
    }

    @Test
    public void stEnvelopeAndBounds() {
        GeometryBoundsUdf.EnvelopeWkt envelope = new GeometryBoundsUdf.EnvelopeWkt();
        GeometryBoundsUdf.XMin xmin = new GeometryBoundsUdf.XMin();
        GeometryBoundsUdf.XMax xmax = new GeometryBoundsUdf.XMax();
        GeometryBoundsUdf.YMin ymin = new GeometryBoundsUdf.YMin();
        GeometryBoundsUdf.YMax ymax = new GeometryBoundsUdf.YMax();

        String env = envelope.eval(INNER);
        assertNotNull(env);
        assertTrue(env.contains("POLYGON"));
        assertEquals(116.2, xmin.eval(INNER), 1e-8);
        assertEquals(116.4, xmax.eval(INNER), 1e-8);
        assertEquals(39.2, ymin.eval(INNER), 1e-8);
        assertEquals(39.4, ymax.eval(INNER), 1e-8);
    }

    @Test
    public void envelopePrefilterThenIntersects() {
        GeometryBoundsUdf.XMin xmin = new GeometryBoundsUdf.XMin();
        GeometryBoundsUdf.XMax xmax = new GeometryBoundsUdf.XMax();
        GeometryBoundsUdf.YMin ymin = new GeometryBoundsUdf.YMin();
        GeometryBoundsUdf.YMax ymax = new GeometryBoundsUdf.YMax();
        GeometryIntersectsUdf intersects = new GeometryIntersectsUdf();

        assertTrue(envelopeOverlaps(DEGREE_SQUARE, INNER, xmin, xmax, ymin, ymax));
        assertTrue(intersects.eval(DEGREE_SQUARE, INNER));

        assertFalse(envelopeOverlaps(DEGREE_SQUARE, FAR, xmin, xmax, ymin, ymax));
        assertFalse(intersects.eval(DEGREE_SQUARE, FAR));
    }

    @Test
    public void invalidWktReturnsNull() {
        assertNull(WktUtils.read("NOT A GEOMETRY"));
        assertNull(new GeometryAreaUdf().eval("BAD"));
        assertNull(new GeometryFromWktUdf().eval(null));
        assertNull(WktUtils.toGeometry(123));
        Geometry geom = WktUtils.read(BEIJING);
        assertEquals(geom, WktUtils.toGeometry(geom));
        assertNotNull(WktUtils.toGeometry(BEIJING));
    }

    @Test
    public void wktReaderIsThreadSafe() throws Exception {
        int threads = 8;
        int loops = 200;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        List<Throwable> errors = Collections.synchronizedList(new ArrayList<Throwable>());
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    for (int j = 0; j < loops; j++) {
                        Geometry g = WktUtils.read(DEGREE_SQUARE);
                        if (g == null || g.isEmpty()) {
                            throw new IllegalStateException("parse failed");
                        }
                        WktUtils.write(g);
                    }
                } catch (Throwable t) {
                    errors.add(t);
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS));
        pool.shutdownNow();
        assertTrue("concurrent WKT parse failed: " + errors, errors.isEmpty());
    }

    private boolean envelopeOverlaps(
            String left,
            String right,
            GeometryBoundsUdf.XMin xmin,
            GeometryBoundsUdf.XMax xmax,
            GeometryBoundsUdf.YMin ymin,
            GeometryBoundsUdf.YMax ymax) {
        return xmax.eval(left) >= xmin.eval(right)
                && xmin.eval(left) <= xmax.eval(right)
                && ymax.eval(left) >= ymin.eval(right)
                && ymin.eval(left) <= ymax.eval(right);
    }
}
