package io.github.interestinglab.waterdrop.flink.transform;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * 剩余 ST_* 谓词：WKT 入参、注册语义、非法 WKT 返回 null。
 */
public class SpatialPredicatesTest {

    private static final String OUTER =
            "POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))";
    private static final String INNER =
            "POLYGON((2 2, 2 4, 4 4, 4 2, 2 2))";
    private static final String OVERLAP =
            "POLYGON((8 8, 8 12, 12 12, 12 8, 8 8))";
    private static final String DISJOINT =
            "POLYGON((20 20, 20 22, 22 22, 22 20, 20 20))";
    private static final String TOUCH_EDGE =
            "POLYGON((10 0, 10 5, 15 5, 15 0, 10 0))";
    private static final String LINE_CROSS =
            "LINESTRING(1 1, 9 9)";
    private static final String LINE_CROSS_OTHER =
            "LINESTRING(1 9, 9 1)";
    private static final String SAME_AS_OUTER =
            "POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))";
    private static final String ORDER_DIFF =
            "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))";

    @Test
    public void stCoversAndCoveredBy() {
        Predicates.ST_Covers covers = new Predicates.ST_Covers();
        Predicates.ST_CoveredBy coveredBy = new Predicates.ST_CoveredBy();
        assertTrue(covers.eval(OUTER, INNER));
        assertTrue(coveredBy.eval(INNER, OUTER));
        assertFalse(covers.eval(INNER, OUTER));
        assertNull(covers.eval(OUTER, "BAD"));
    }

    @Test
    public void stCrosses() {
        Predicates.ST_Crosses crosses = new Predicates.ST_Crosses();
        assertTrue(crosses.eval(LINE_CROSS, LINE_CROSS_OTHER));
        assertFalse(crosses.eval(OUTER, INNER));
        assertNull(crosses.eval(null, LINE_CROSS));
    }

    @Test
    public void stDisjoint() {
        Predicates.ST_Disjoint disjoint = new Predicates.ST_Disjoint();
        assertTrue(disjoint.eval(OUTER, DISJOINT));
        assertFalse(disjoint.eval(OUTER, INNER));
        assertNull(disjoint.eval("BAD", OUTER));
    }

    @Test
    public void stEqualsAndOrderingEquals() {
        Predicates.ST_Equals equals = new Predicates.ST_Equals();
        Predicates.ST_OrderingEquals orderingEquals = new Predicates.ST_OrderingEquals();
        assertTrue(equals.eval(OUTER, SAME_AS_OUTER));
        assertTrue(equals.eval(OUTER, ORDER_DIFF));
        assertTrue(orderingEquals.eval(OUTER, SAME_AS_OUTER));
        assertFalse(orderingEquals.eval(OUTER, ORDER_DIFF));
        assertFalse(equals.eval(OUTER, INNER));
    }

    @Test
    public void stOverlaps() {
        Predicates.ST_Overlaps overlaps = new Predicates.ST_Overlaps();
        assertTrue(overlaps.eval(OUTER, OVERLAP));
        assertFalse(overlaps.eval(OUTER, INNER));
        assertFalse(overlaps.eval(OUTER, DISJOINT));
    }

    @Test
    public void stTouches() {
        Predicates.ST_Touches touches = new Predicates.ST_Touches();
        assertTrue(touches.eval(OUTER, TOUCH_EDGE));
        assertFalse(touches.eval(OUTER, INNER));
        assertFalse(touches.eval(OUTER, DISJOINT));
    }

    @Test
    public void stRelateAndRelateMatch() {
        Predicates.ST_Relate relate = new Predicates.ST_Relate();
        Predicates.ST_RelateMatch relateMatch = new Predicates.ST_RelateMatch();

        String matrix = relate.eval(OUTER, INNER);
        assertNotNull(matrix);
        assertEquals(9, matrix.length());

        assertTrue(relate.eval(OUTER, INNER, matrix));
        assertFalse(relate.eval(OUTER, DISJOINT, matrix));

        assertTrue(relateMatch.eval(matrix, matrix));
        assertNull(relate.eval(OUTER, "BAD"));
        assertNull(relateMatch.eval(null, matrix));
    }

    @Test
    public void stDWithinDefaultUsesSpheroid() {
        Predicates.ST_DWithin dWithin = new Predicates.ST_DWithin();
        String beijing = "POINT(116.397428 39.90923)";
        String shanghai = "POINT(121.473667 31.230525)";
        assertFalse(dWithin.eval(beijing, shanghai, 1000.0));
        assertTrue(dWithin.eval(beijing, shanghai, 2_000_000.0));
        // 显式平面距离：经纬度度数，两点相距很大，1e-3 应 false
        assertFalse(dWithin.eval(beijing, shanghai, 0.001, false));
        assertNull(dWithin.eval(beijing, "BAD", 100.0));
    }

    @Test
    public void stIntersectsContainsWithinStillWorkViaPredicates() {
        assertTrue(new Predicates.ST_Intersects().eval(OUTER, INNER));
        assertTrue(new Predicates.ST_Contains().eval(OUTER, INNER));
        assertTrue(new Predicates.ST_Within().eval(INNER, OUTER));
        assertFalse(new Predicates.ST_Intersects().eval(OUTER, DISJOINT));
    }
}
