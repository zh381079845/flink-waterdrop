package io.github.interestinglab.waterdrop.flink.transform;

import org.junit.Test;
import org.locationtech.jts.geom.Geometry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * P1 新增算子：length/centroid/point/xy/isvalid/makevalid/symdifference/asgeojson + relate 三参。
 */
public class SpatialP1OperatorsTest {

    private static final String LINE = "LINESTRING(116.40 39.90, 116.50 39.90)";
    private static final String POLY =
            "POLYGON((116.40 39.90, 116.40 40.00, 116.50 40.00, 116.50 39.90, 116.40 39.90))";
    private static final String OTHER =
            "POLYGON((116.45 39.95, 116.45 40.05, 116.55 40.05, 116.55 39.95, 116.45 39.95))";
    private static final String INVALID_BOW =
            "POLYGON((0 0, 2 2, 0 2, 2 0, 0 0))";

    @Test
    public void lengthAndPerimeterSpheroid() {
        GeometryLengthUdf.Length length = new GeometryLengthUdf.Length();
        GeometryLengthUdf.Perimeter perimeter = new GeometryLengthUdf.Perimeter();
        Double lineM = length.eval(LINE);
        assertNotNull(lineM);
        // ~0.1° 经度在 40°N ≈ 8.5km
        assertTrue(lineM > 7000 && lineM < 12000);
        Double peri = perimeter.eval(POLY);
        assertNotNull(peri);
        assertTrue(peri > lineM);
        assertEquals(0.1, length.eval(LINE, false), 1e-9);
    }

    @Test
    public void pointCentroidXY() {
        GeometryConstructUdf.PointFromXY point = new GeometryConstructUdf.PointFromXY();
        GeometryConstructUdf.Centroid centroid = new GeometryConstructUdf.Centroid();
        GeometryConstructUdf.X x = new GeometryConstructUdf.X();
        GeometryConstructUdf.Y y = new GeometryConstructUdf.Y();

        String pt = point.eval(116.4, 39.9);
        assertEquals("POINT (116.4 39.9)", pt);
        assertEquals(116.4, x.eval(pt), 1e-9);
        assertEquals(39.9, y.eval(pt), 1e-9);

        String c = centroid.eval(POLY);
        assertNotNull(c);
        assertTrue(x.eval(c) > 116.4 && x.eval(c) < 116.5);
    }

    @Test
    public void isValidAndMakeValid() {
        GeometryValidUdf.IsValid isValid = new GeometryValidUdf.IsValid();
        GeometryValidUdf.MakeValid makeValid = new GeometryValidUdf.MakeValid();
        assertTrue(isValid.eval(POLY));
        assertFalse(isValid.eval(INVALID_BOW));
        String fixed = makeValid.eval(INVALID_BOW);
        assertNotNull(fixed);
        Geometry g = WktUtils.read(fixed);
        assertNotNull(g);
        assertTrue(isValid.eval(fixed));
    }

    @Test
    public void symDifferenceAndAsGeoJson() {
        GeometryOverlayUdf.SymDifference sym = new GeometryOverlayUdf.SymDifference();
        GeometryAsGeoJsonUdf asJson = new GeometryAsGeoJsonUdf();
        String out = sym.eval(POLY, OTHER);
        assertNotNull(out);
        assertFalse(WktUtils.read(out).isEmpty());

        String json = asJson.eval("POINT(116.4 39.9)");
        assertNotNull(json);
        assertTrue(json.contains("Point") || json.contains("\"type\""));
        assertTrue(json.contains("116.4"));
    }

    @Test
    public void relateThreeArgPattern() {
        Predicates.ST_Relate relate = new Predicates.ST_Relate();
        String matrix = relate.eval(POLY, OTHER);
        assertNotNull(matrix);
        assertEquals(9, matrix.length());
        assertTrue(relate.eval(POLY, OTHER, matrix));
        assertTrue(relate.eval(POLY, OTHER, "T********"));
        assertFalse(relate.eval(POLY, OTHER, "FF*FF****"));
    }
}
