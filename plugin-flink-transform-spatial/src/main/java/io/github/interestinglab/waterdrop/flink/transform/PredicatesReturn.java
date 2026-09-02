package io.github.interestinglab.waterdrop.flink.transform;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.IntersectionMatrix;
import org.locationtech.jts.operation.relate.RelateOp;

public class PredicatesReturn {

    public static boolean contains(Geometry leftGeometry, Geometry rightGeometry) {
        return leftGeometry.contains(rightGeometry);
    }

    public static boolean intersects(Geometry leftGeometry, Geometry rightGeometry) {
        return leftGeometry.intersects(rightGeometry);
    }

    public static boolean within(Geometry leftGeometry, Geometry rightGeometry) {
        return leftGeometry.within(rightGeometry);
    }

    public static boolean covers(Geometry leftGeometry, Geometry rightGeometry) {
        return leftGeometry.covers(rightGeometry);
    }

    public static boolean coveredBy(Geometry leftGeometry, Geometry rightGeometry) {
        return leftGeometry.coveredBy(rightGeometry);
    }

    public static boolean crosses(Geometry leftGeometry, Geometry rightGeometry) {
        return leftGeometry.crosses(rightGeometry);
    }

    public static boolean overlaps(Geometry leftGeometry, Geometry rightGeometry) {
        return leftGeometry.overlaps(rightGeometry);
    }

    public static boolean touches(Geometry leftGeometry, Geometry rightGeometry) {
        return leftGeometry.touches(rightGeometry);
    }

    public static boolean equals(Geometry leftGeometry, Geometry rightGeometry) {
        return leftGeometry.symDifference(rightGeometry).isEmpty();
    }

    public static boolean disjoint(Geometry leftGeometry, Geometry rightGeometry) {
        return leftGeometry.disjoint(rightGeometry);
    }

    public static boolean orderingEquals(Geometry leftGeometry, Geometry rightGeometry) {
        return leftGeometry.equalsExact(rightGeometry);
    }

    public static boolean dWithin(Geometry leftGeometry, Geometry rightGeometry, double distance) {
        return dWithin(leftGeometry, rightGeometry, distance, false);
    }

    public static boolean dWithin(
            Geometry leftGeometry, Geometry rightGeometry, double distance, boolean useSpheroid) {
        if (useSpheroid) {
            double distanceSpheroid = Spheroid.distance(leftGeometry, rightGeometry);
            if (Double.isNaN(distanceSpheroid)) {
                return false;
            }
            return distanceSpheroid <= distance;
        } else {
            return leftGeometry.isWithinDistance(rightGeometry, distance);
        }
    }

    public static String relate(Geometry leftGeometry, Geometry rightGeometry) {
        return RelateOp.relate(leftGeometry, rightGeometry).toString();
    }

    public static boolean relate(
            Geometry leftGeometry, Geometry rightGeometry, String intersectionMatrix) {
        String matrixFromGeom = relate(leftGeometry, rightGeometry);
        return IntersectionMatrix.matches(matrixFromGeom, intersectionMatrix);
    }

    public static boolean relateMatch(String matrix1, String matrix2) {
        return IntersectionMatrix.matches(matrix1, matrix2);
    }
}
