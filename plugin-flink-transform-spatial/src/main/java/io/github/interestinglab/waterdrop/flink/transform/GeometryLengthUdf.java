package io.github.interestinglab.waterdrop.flink.transform;

import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.Geometry;

/**
 * st_length / st_perimeter：默认 WGS84 椭球长度（米）；useSpheroid=false 为平面长度。
 */
public final class GeometryLengthUdf {

    private GeometryLengthUdf() {
    }

    public static class Length extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public Double eval(String wkt) {
            return length(WktUtils.read(wkt), true);
        }

        public Double eval(String wkt, Boolean useSpheroid) {
            return length(WktUtils.read(wkt), useSpheroid);
        }

        public Double eval(Geometry geometry) {
            return length(geometry, true);
        }

        public Double eval(Geometry geometry, Boolean useSpheroid) {
            return length(geometry, useSpheroid);
        }
    }

    /** 对面：外环+内环周长之和（与 PostGIS ST_Perimeter 近似）。 */
    public static class Perimeter extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public Double eval(String wkt) {
            return length(WktUtils.read(wkt), true);
        }

        public Double eval(String wkt, Boolean useSpheroid) {
            return length(WktUtils.read(wkt), useSpheroid);
        }

        public Double eval(Geometry geometry) {
            return length(geometry, true);
        }

        public Double eval(Geometry geometry, Boolean useSpheroid) {
            return length(geometry, useSpheroid);
        }
    }

    static Double length(Geometry geometry, Boolean useSpheroid) {
        if (geometry == null || geometry.isEmpty()) {
            return null;
        }
        boolean spheroid = useSpheroid == null || useSpheroid;
        if (spheroid) {
            return Spheroid.length(geometry);
        }
        return geometry.getLength();
    }
}
