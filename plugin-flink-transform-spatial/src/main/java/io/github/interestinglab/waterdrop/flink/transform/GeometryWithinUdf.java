package io.github.interestinglab.waterdrop.flink.transform;

import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.Geometry;

/**
 * st_within(a, b)：a 在 b 内部。
 */
public class GeometryWithinUdf extends ScalarFunction {

    private static final long serialVersionUID = 1L;

    public Boolean eval(String left, String right) {
        return within(WktUtils.read(left), WktUtils.read(right));
    }

    public Boolean eval(Geometry left, Geometry right) {
        return within(left, right);
    }

    private Boolean within(Geometry left, Geometry right) {
        if (left == null || right == null) {
            return null;
        }
        return left.within(right);
    }
}
