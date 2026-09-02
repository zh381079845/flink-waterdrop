package io.github.interestinglab.waterdrop.flink.transform;

import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.Geometry;

/**
 * 空间相交判断 UDF，接受 WKT 或 Geometry。
 */
public class GeometryIntersectsUdf extends ScalarFunction {

    private static final long serialVersionUID = 1L;

    public Boolean eval(String wkt1, String wkt2) {
        return intersects(WktUtils.read(wkt1), WktUtils.read(wkt2));
    }

    public Boolean eval(Geometry geom1, Geometry geom2) {
        return intersects(geom1, geom2);
    }

    private Boolean intersects(Geometry geom1, Geometry geom2) {
        if (geom1 == null || geom2 == null) {
            return null;
        }
        return geom1.intersects(geom2);
    }
}
