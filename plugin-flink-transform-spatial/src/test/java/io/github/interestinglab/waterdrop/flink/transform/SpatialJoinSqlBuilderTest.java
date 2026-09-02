package io.github.interestinglab.waterdrop.flink.transform;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SpatialJoinSqlBuilderTest {

    @Test
    public void buildsGridJoinWithOccupyArea() {
        SpatialJoinSqlBuilder.Spec spec = new SpatialJoinSqlBuilder.Spec();
        spec.leftTable = "eco_raw";
        spec.rightTable = "farm_raw";
        spec.cellSize = 0.05;
        spec.intersectionArea = true;
        spec.dedup = true;
        spec.mode = SpatialJoinSqlBuilder.Mode.GRID;

        String sql = SpatialJoinSqlBuilder.build(spec);
        assertTrue(sql.contains("LATERAL TABLE(st_grid_cells"));
        assertTrue(sql.contains("CAST(0.05 AS DOUBLE)"));
        assertTrue(sql.contains("CAST(" + SpatialGrid.DEFAULT_MAX_CELLS + " AS INT)")
                || sql.contains("AS INT)"));
        assertTrue(sql.contains("l.grid_id = r.grid_id"));
        assertTrue(sql.contains("st_intersects(l._geom, r._geom)"));
        assertTrue(sql.contains("st_area(st_intersection"));
        assertTrue(sql.contains("GROUP BY l._key, r._key"));
        assertTrue(sql.contains("`eco_raw`"));
        assertTrue(sql.contains("`farm_raw`"));
    }

    @Test
    public void buildsEnvelopeOnlyJoin() {
        SpatialJoinSqlBuilder.Spec spec = new SpatialJoinSqlBuilder.Spec();
        spec.leftTable = "a";
        spec.rightTable = "b";
        spec.mode = SpatialJoinSqlBuilder.Mode.ENVELOPE;
        spec.predicate = "contains";
        spec.dedup = false;

        String sql = SpatialJoinSqlBuilder.build(spec);
        assertFalse(sql.contains("st_grid_cells"));
        assertFalse(sql.contains("grid_id"));
        assertTrue(sql.contains("st_contains(l._geom, r._geom)"));
        assertTrue(sql.contains("l.maxx >= r.minx"));
        assertFalse(sql.contains("GROUP BY"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsBadPredicate() {
        SpatialJoinSqlBuilder.Spec spec = new SpatialJoinSqlBuilder.Spec();
        spec.leftTable = "a";
        spec.rightTable = "b";
        spec.predicate = "nearby";
        SpatialJoinSqlBuilder.build(spec);
    }
}
