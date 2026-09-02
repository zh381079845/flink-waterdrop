package io.github.interestinglab.waterdrop.flink.transform;

import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

/**
 * st_point / st_centroid / st_x / st_y。
 */
public final class GeometryConstructUdf {

    private static final GeometryFactory FACTORY = new GeometryFactory();

    private GeometryConstructUdf() {
    }

    public static class PointFromXY extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public String eval(Double x, Double y) {
            if (x == null || y == null) {
                return null;
            }
            return WktUtils.write(FACTORY.createPoint(new Coordinate(x, y)));
        }
    }

    public static class Centroid extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public String eval(String wkt) {
            return WktUtils.write(centroid(WktUtils.read(wkt)));
        }

        public Geometry eval(Geometry geometry) {
            return centroid(geometry);
        }

        private Geometry centroid(Geometry geometry) {
            if (geometry == null || geometry.isEmpty()) {
                return null;
            }
            return geometry.getCentroid();
        }
    }

    public static class X extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public Double eval(String wkt) {
            return x(WktUtils.read(wkt));
        }

        public Double eval(Geometry geometry) {
            return x(geometry);
        }

        private Double x(Geometry geometry) {
            if (geometry == null || geometry.isEmpty()) {
                return null;
            }
            Point p = geometry instanceof Point ? (Point) geometry : geometry.getCentroid();
            return p.getX();
        }
    }

    public static class Y extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public Double eval(String wkt) {
            return y(WktUtils.read(wkt));
        }

        public Double eval(Geometry geometry) {
            return y(geometry);
        }

        private Double y(Geometry geometry) {
            if (geometry == null || geometry.isEmpty()) {
                return null;
            }
            Point p = geometry instanceof Point ? (Point) geometry : geometry.getCentroid();
            return p.getY();
        }
    }
}
