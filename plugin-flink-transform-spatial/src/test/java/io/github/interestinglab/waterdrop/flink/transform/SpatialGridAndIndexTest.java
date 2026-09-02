package io.github.interestinglab.waterdrop.flink.transform;

import org.junit.Test;
import org.locationtech.jts.geom.Geometry;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SpatialGridAndIndexTest {

    private static final String ECO =
            "POLYGON((116.30 39.90, 116.30 40.05, 116.50 40.05, 116.50 39.90, 116.30 39.90))";
    private static final String FARM =
            "POLYGON((116.40 39.95, 116.40 40.10, 116.60 40.10, 116.60 39.95, 116.40 39.95))";
    private static final String FAR =
            "POLYGON((120.00 31.00, 120.00 31.10, 120.10 31.10, 120.10 31.00, 120.00 31.00))";
    private static final String SITE = "POINT(116.42 39.97)";

    private final GeometryGridUdf.GeoHashFn geohash = new GeometryGridUdf.GeoHashFn();
    private final GeometryGridUdf.GridId gridId = new GeometryGridUdf.GridId();
    private final GeometryGridUdf.GridIds gridIds = new GeometryGridUdf.GridIds();

    @Test
    public void geohashStableForPoint() {
        String h = geohash.eval(SITE, 6);
        assertNotNull(h);
        assertEquals(6, h.length());
        assertEquals(h, geohash.eval(WktUtils.read(SITE), 6));
    }

    @Test
    public void gridIdAndIdsCoverOverlap() {
        double cell = 0.05;
        String ecoIds = gridIds.eval(ECO, cell);
        String farmIds = gridIds.eval(FARM, cell);
        assertNotNull(ecoIds);
        assertNotNull(farmIds);

        Set<String> eco = toSet(ecoIds);
        Set<String> farm = toSet(farmIds);
        eco.retainAll(farm);
        assertFalse("overlapping polygons should share grid cells", eco.isEmpty());

        Set<String> far = toSet(gridIds.eval(FAR, cell));
        Set<String> eco2 = toSet(ecoIds);
        eco2.retainAll(far);
        assertTrue(eco2.isEmpty());

        assertNotNull(gridId.eval(SITE, cell));
        assertTrue(toSet(ecoIds).contains(gridId.eval(SITE, cell)));
    }

    @Test
    public void primaryCellInsideCoveringCells() {
        Geometry eco = WktUtils.read(ECO);
        String primary = SpatialGrid.primaryCellId(eco, 0.1);
        assertTrue(SpatialGrid.cellsCovering(eco, 0.1).contains(primary));
    }

    @Test
    public void nullSafeGridUdf() {
        assertNull(geohash.eval("BAD", 5));
        assertNull(gridId.eval((String) null, 0.1));
        assertNull(gridIds.eval(ECO, -1.0));
    }

    @Test
    public void localStrTreeTwoPhaseQuery() {
        LocalStrTreeIndex index = new LocalStrTreeIndex();
        index.insert("eco", WktUtils.read(ECO));
        index.insert("far", WktUtils.read(FAR));
        index.build();

        List<LocalStrTreeIndex.Entry> envelopeHits = index.queryEnvelope(WktUtils.read(FARM));
        assertEquals(1, envelopeHits.size());
        assertEquals("eco", envelopeHits.get(0).id);

        List<LocalStrTreeIndex.Entry> precise = index.queryIntersects(WktUtils.read(FARM));
        assertEquals(1, precise.size());

        assertTrue(index.queryIntersects(WktUtils.read(FAR)).isEmpty()
                || index.queryIntersects(WktUtils.read("POINT(110 30)")).isEmpty());
        assertTrue(index.queryIntersects(WktUtils.read("POINT(110 30)")).isEmpty());
    }

    private Set<String> toSet(String csv) {
        Set<String> set = new HashSet<String>();
        for (String p : csv.split(",")) {
            if (!p.isEmpty()) {
                set.add(p);
            }
        }
        return set;
    }
}
