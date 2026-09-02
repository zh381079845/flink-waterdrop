package io.github.interestinglab.waterdrop.flink.transform;

import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.Geometry;

/**
 * st_contains(a, b)：a 包含 b。
 */
public class GeometryContainsUdf extends ScalarFunction {

    private static final long serialVersionUID = 1L;

    public Boolean eval(String left, String right) {
        return contains(WktUtils.read(left), WktUtils.read(right));
    }

    public Boolean eval(Geometry left, Geometry right) {
        return contains(left, right);
    }

    private Boolean contains(Geometry left, Geometry right) {
        if (left == null || right == null) {
            return null;
        }
        return left.contains(right);
    }
}
