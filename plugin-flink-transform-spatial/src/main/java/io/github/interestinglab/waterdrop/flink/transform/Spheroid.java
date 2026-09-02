package io.github.interestinglab.waterdrop.flink.transform;

import net.sf.geographiclib.Geodesic;
import net.sf.geographiclib.GeodesicData;
import net.sf.geographiclib.PolygonArea;
import net.sf.geographiclib.PolygonResult;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.operation.distance.DistanceOp;

import static java.lang.Math.abs;

public class Spheroid {
    // Standard EPSG Codes
    public static final int EPSG_WORLD_MERCATOR = 3395;
    public static final int EPSG_NORTH_UTM_START = 32601;
    public static final int EPSG_NORTH_UTM_END = 32660;
    public static final int EPSG_NORTH_LAMBERT = 3574;
    public static final int EPSG_NORTH_STEREO = 3995;
    public static final int EPSG_SOUTH_UTM_START = 32701;
    public static final int EPSG_SOUTH_UTM_END = 32760;
    public static final int EPSG_SOUTH_LAMBERT = 3409;
    public static final int EPSG_SOUTH_STEREO = 3031;

    /**
     * WGS84 椭球最短距离（米）。
     * <p>
     * 先用 JTS {@link DistanceOp#nearestPoints} 在输入坐标系下找一对最近点，
     * 再对这两点做 GeographicLib 测地线距离。对 Point/Line/Polygon 均为「最近点」语义，
     * 不再使用质心（旧行为会系统性高估线/面距离）。
     * <p>
     * 说明：最近点在经纬度平面上近似求解，远距离/跨带场景存在小误差；近距管线/红线类业务足够用。
     */
    public static double distance(Geometry geom1, Geometry geom2) {
        if (geom1 == null || geom2 == null || geom1.isEmpty() || geom2.isEmpty()) {
            return Double.NaN;
        }
        Coordinate[] nearest = DistanceOp.nearestPoints(geom1, geom2);
        double lon1 = nearest[0].getX();
        double lat1 = nearest[0].getY();
        double lon2 = nearest[1].getX();
        double lat2 = nearest[1].getY();
        GeodesicData g = Geodesic.WGS84.Inverse(lat1, lon1, lat2, lon2);
        return g.s12;
    }

    /**
     * 两点测地线距离（米）。
     */
    public static double distanceMeters(double lon1, double lat1, double lon2, double lat2) {
        return Geodesic.WGS84.Inverse(lat1, lon1, lat2, lon2).s12;
    }

    /**
     * Calculate the length of a geometry using the Spheroid formula. Equivalent to PostGIS
     * ST_LengthSpheroid and PostGIS ST_Length(useSpheroid=true) WGS84 ellipsoid is used.
     *
     * @param geom
     * @return
     */
    public static double length(Geometry geom) {
        String geomType = geom.getGeometryType();
        if (geomType.equals("Polygon") || geomType.equals("LineString")) {
            PolygonArea p = new PolygonArea(Geodesic.WGS84, true);
            Coordinate[] coordinates = geom.getCoordinates();
            for (int i = 0; i < coordinates.length; i++) {
                double lon = coordinates[i].getX();
                double lat = coordinates[i].getY();
                p.AddPoint(lat, lon);
            }
            PolygonResult compute = p.Compute();
            return compute.perimeter;
        } else if (geomType.equals("MultiPolygon")
                || geomType.equals("MultiLineString")
                || geomType.equals("GeometryCollection")) {
            double length = 0.0;
            for (int i = 0; i < geom.getNumGeometries(); i++) {
                length += length(geom.getGeometryN(i));
            }
            return length;
        } else {
            return 0.0;
        }
    }

    /**
     * Calculate the area of a geometry using the Spheroid formula. Equivalent to PostGIS
     * ST_Area(useSpheroid=true) WGS84 ellipsoid is used.
     *
     * @param geom
     * @return
     */
    public static double area(Geometry geom) {
        String geomType = geom.getGeometryType();
        if (geomType.equals("Polygon")) {
            PolygonArea p = new PolygonArea(Geodesic.WGS84, false);
            Coordinate[] coordinates = geom.getCoordinates();
            for (int i = 0; i < coordinates.length; i++) {
                double lon = coordinates[i].getX();
                double lat = coordinates[i].getY();
                p.AddPoint(lat, lon);
            }
            PolygonResult compute = p.Compute();
            // The area is negative if the polygon is oriented clockwise
            // We make sure that all area are positive
            return abs(compute.area);
        } else if (geomType.equals("MultiPolygon") || geomType.equals("GeometryCollection")) {
            double area = 0.0;
            for (int i = 0; i < geom.getNumGeometries(); i++) {
                area += area(geom.getGeometryN(i));
            }
            return area;
        } else {
            return 0.0;
        }
    }

    public static Double angularWidth(Envelope envelope) {
        double lon1 = envelope.getMinX();
        double lon2 = envelope.getMaxX();
        double lat =
                (envelope.getMinY() + envelope.getMaxY()) / 2; // Mid-latitude for width calculation

        // Compute geodesic distance
        GeodesicData g = Geodesic.WGS84.Inverse(lat, lon1, lat, lon2);
        double distance = g.s12; // Distance in meters

        // Convert distance to angular width in degrees
        Double angularWidth =
                Math.toDegrees(distance / (Geodesic.WGS84.MajorRadius() * Math.PI / 180));

        return angularWidth;
    }

    public static Double angularHeight(Envelope envelope) {
        double lat1 = envelope.getMinY();
        double lat2 = envelope.getMaxY();
        double lon =
                (envelope.getMinX() + envelope.getMaxX()) / 2; // Mid-longitude for height calculation

        // Compute geodesic distance
        GeodesicData g = Geodesic.WGS84.Inverse(lat1, lon, lat2, lon);
        double distance = g.s12; // Distance in meters

        // Convert distance to angular height in degrees
        Double angularHeight =
                Math.toDegrees(distance / (Geodesic.WGS84.MajorRadius() * Math.PI / 180));

        return angularHeight;
    }
}

