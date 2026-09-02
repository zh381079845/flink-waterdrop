package io.github.interestinglab.waterdrop.flink.transform;

import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.Geometry;

/**
 * 几何面积 UDF。默认按 WGS84 椭球计算（平方米），对齐 PostGIS ST_Area(geog)。
 * st_area(geom) 或 st_area(geom, true) 使用椭球；st_area(geom, false) 使用平面坐标。
 */
public class GeometryAreaUdf extends ScalarFunction {

    private static final long serialVersionUID = 1L;

    public Double eval(String wkt) {
        return area(WktUtils.read(wkt), Boolean.TRUE);
    }

    public Double eval(String wkt, Boolean useSpheroid) {
        return area(WktUtils.read(wkt), useSpheroid);
    }

    public Double eval(Geometry geometry) {
        return area(geometry, Boolean.TRUE);
    }

    public Double eval(Geometry geometry, Boolean useSpheroid) {
        return area(geometry, useSpheroid);
    }

    private Double area(Geometry geometry, Boolean useSpheroid) {
        if (geometry == null) {
            return null;
        }
        boolean spheroid = useSpheroid == null || useSpheroid;
        return spheroid ? Spheroid.area(geometry) : geometry.getArea();
    }
}
