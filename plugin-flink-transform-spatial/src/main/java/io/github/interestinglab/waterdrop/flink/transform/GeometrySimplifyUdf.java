package io.github.interestinglab.waterdrop.flink.transform;

import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier;

/**
 * st_simplify(geom, tolerance)：Douglas-Peucker 抽稀，供粗筛几何；精炼仍用原 geom。
 */
public class GeometrySimplifyUdf extends ScalarFunction {

    private static final long serialVersionUID = 1L;

    public String eval(String wkt, Double tolerance) {
        return WktUtils.write(simplify(WktUtils.read(wkt), tolerance));
    }

    public Geometry eval(Geometry geometry, Double tolerance) {
        return simplify(geometry, tolerance);
    }

    private Geometry simplify(Geometry geometry, Double tolerance) {
        if (geometry == null || tolerance == null || tolerance < 0) {
            return geometry;
        }
        return DouglasPeuckerSimplifier.simplify(geometry, tolerance);
    }
}
