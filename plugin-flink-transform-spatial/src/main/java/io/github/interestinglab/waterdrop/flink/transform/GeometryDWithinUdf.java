package io.github.interestinglab.waterdrop.flink.transform;

import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.Geometry;

/**
 * st_dwithin(a, b, meters)：椭球距离是否不超过给定米数。
 */
public class GeometryDWithinUdf extends ScalarFunction {

    private static final long serialVersionUID = 1L;

    public Boolean eval(String left, String right, Double meters) {
        return dWithin(WktUtils.read(left), WktUtils.read(right), meters);
    }

    public Boolean eval(Geometry left, Geometry right, Double meters) {
        return dWithin(left, right, meters);
    }

    private Boolean dWithin(Geometry left, Geometry right, Double meters) {
        if (left == null || right == null || meters == null) {
            return null;
        }
        double d = Spheroid.distance(left, right);
        if (Double.isNaN(d)) {
            return null;
        }
        return d <= meters;
    }
}
