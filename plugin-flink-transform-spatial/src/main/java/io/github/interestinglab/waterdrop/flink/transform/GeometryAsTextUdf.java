package io.github.interestinglab.waterdrop.flink.transform;

import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.Geometry;

/**
 * st_astext：Geometry 或 WKT → WKT 字符串。
 */
public class GeometryAsTextUdf extends ScalarFunction {

    private static final long serialVersionUID = 1L;

    public String eval(String wkt) {
        return WktUtils.write(WktUtils.read(wkt));
    }

    public String eval(Geometry geometry) {
        return WktUtils.write(geometry);
    }
}
