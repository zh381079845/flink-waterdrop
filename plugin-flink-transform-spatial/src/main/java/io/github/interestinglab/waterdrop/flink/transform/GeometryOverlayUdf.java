package io.github.interestinglab.waterdrop.flink.transform;

import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.operation.union.UnaryUnionOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 几何交 / 差 / 并。SQL 侧默认返回 WKT；Geometry 重载返回 Geometry 便于链式计算。
 * 非法或拓扑失败返回 null；空结果返回 EMPTY 几何的 WKT（面积为 0）。
 */
public final class GeometryOverlayUdf {

    private static final Logger LOG = LoggerFactory.getLogger(GeometryOverlayUdf.class);

    private GeometryOverlayUdf() {
    }

    public static class Intersection extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public String eval(String left, String right) {
            return WktUtils.write(intersection(WktUtils.read(left), WktUtils.read(right)));
        }

        public Geometry eval(Geometry left, Geometry right) {
            return intersection(left, right);
        }
    }

    public static class Difference extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public String eval(String left, String right) {
            return WktUtils.write(difference(WktUtils.read(left), WktUtils.read(right)));
        }

        public Geometry eval(Geometry left, Geometry right) {
            return difference(left, right);
        }
    }

    public static class Union extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public String eval(String left, String right) {
            return WktUtils.write(union(WktUtils.read(left), WktUtils.read(right)));
        }

        public Geometry eval(Geometry left, Geometry right) {
            return union(left, right);
        }

        /** 单几何 UnaryUnion（合并 Multi 部件）。 */
        public String eval(String wkt) {
            return WktUtils.write(unaryUnion(WktUtils.read(wkt)));
        }

        public Geometry eval(Geometry geometry) {
            return unaryUnion(geometry);
        }
    }

    public static class SymDifference extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public String eval(String left, String right) {
            return WktUtils.write(symDifference(WktUtils.read(left), WktUtils.read(right)));
        }

        public Geometry eval(Geometry left, Geometry right) {
            return symDifference(left, right);
        }
    }

    static Geometry intersection(Geometry left, Geometry right) {
        return overlay(left, right, Overlay.INTERSECTION);
    }

    static Geometry difference(Geometry left, Geometry right) {
        return overlay(left, right, Overlay.DIFFERENCE);
    }

    static Geometry union(Geometry left, Geometry right) {
        return overlay(left, right, Overlay.UNION);
    }

    static Geometry symDifference(Geometry left, Geometry right) {
        return overlay(left, right, Overlay.SYM_DIFFERENCE);
    }

    static Geometry unaryUnion(Geometry geometry) {
        if (geometry == null) {
            return null;
        }
        try {
            return UnaryUnionOp.union(geometry);
        } catch (Exception e) {
            LOG.warn("st_union failed: {}", e.toString());
            return null;
        }
    }

    private enum Overlay {
        INTERSECTION,
        DIFFERENCE,
        UNION,
        SYM_DIFFERENCE
    }

    private static Geometry overlay(Geometry left, Geometry right, Overlay op) {
        if (left == null || right == null) {
            return null;
        }
        try {
            switch (op) {
                case INTERSECTION:
                    return left.intersection(right);
                case DIFFERENCE:
                    return left.difference(right);
                case UNION:
                    return left.union(right);
                case SYM_DIFFERENCE:
                    return left.symDifference(right);
                default:
                    return null;
            }
        } catch (Exception e) {
            LOG.warn("geometry overlay {} failed: {}", op, e.toString());
            return null;
        }
    }
}
