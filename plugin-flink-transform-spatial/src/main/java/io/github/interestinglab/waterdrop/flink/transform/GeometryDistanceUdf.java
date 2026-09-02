package io.github.interestinglab.waterdrop.flink.transform;

import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.Geometry;

/**
 * st_distance(a, b)：WGS84 椭球最短距离（米），最近点语义（非质心）。
 */
public class GeometryDistanceUdf extends ScalarFunction {

    private static final long serialVersionUID = 1L;

    public Double eval(String left, String right) {
        return distance(WktUtils.read(left), WktUtils.read(right));
    }

    public Double eval(Geometry left, Geometry right) {
        return distance(left, right);
    }

    private Double distance(Geometry left, Geometry right) {
        if (left == null || right == null || left.isEmpty() || right.isEmpty()) {
            return null;
        }
        double meters = Spheroid.distance(left, right);
        return Double.isNaN(meters) ? null : meters;
    }
}
