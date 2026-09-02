package io.github.interestinglab.waterdrop.flink.transform;

import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.Geometry;

/**
 * 空间关系谓词。每个函数都支持 WKT 字符串和 JTS Geometry。
 * 非法 WKT / null 返回 null，不抛异常打断作业。
 */
public final class Predicates {

    private Predicates() {
    }

    private static Boolean bool(Geometry left, Geometry right, BoolOp op) {
        if (left == null || right == null) {
            return null;
        }
        return op.apply(left, right);
    }

    private static String text(Geometry left, Geometry right, TextOp op) {
        if (left == null || right == null) {
            return null;
        }
        return op.apply(left, right);
    }

    private interface BoolOp {
        boolean apply(Geometry left, Geometry right);
    }

    private interface TextOp {
        String apply(Geometry left, Geometry right);
    }

    public static class ST_Intersects extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public Boolean eval(String left, String right) {
            return bool(WktUtils.read(left), WktUtils.read(right), PredicatesReturn::intersects);
        }

        public Boolean eval(Geometry left, Geometry right) {
            return bool(left, right, PredicatesReturn::intersects);
        }
    }

    public static class ST_Contains extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public Boolean eval(String left, String right) {
            return bool(WktUtils.read(left), WktUtils.read(right), PredicatesReturn::contains);
        }

        public Boolean eval(Geometry left, Geometry right) {
            return bool(left, right, PredicatesReturn::contains);
        }
    }

    public static class ST_Within extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public Boolean eval(String left, String right) {
            return bool(WktUtils.read(left), WktUtils.read(right), PredicatesReturn::within);
        }

        public Boolean eval(Geometry left, Geometry right) {
            return bool(left, right, PredicatesReturn::within);
        }
    }

    public static class ST_Covers extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public Boolean eval(String left, String right) {
            return bool(WktUtils.read(left), WktUtils.read(right), PredicatesReturn::covers);
        }

        public Boolean eval(Geometry left, Geometry right) {
            return bool(left, right, PredicatesReturn::covers);
        }
    }

    public static class ST_CoveredBy extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public Boolean eval(String left, String right) {
            return bool(WktUtils.read(left), WktUtils.read(right), PredicatesReturn::coveredBy);
        }

        public Boolean eval(Geometry left, Geometry right) {
            return bool(left, right, PredicatesReturn::coveredBy);
        }
    }

    public static class ST_Crosses extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public Boolean eval(String left, String right) {
            return bool(WktUtils.read(left), WktUtils.read(right), PredicatesReturn::crosses);
        }

        public Boolean eval(Geometry left, Geometry right) {
            return bool(left, right, PredicatesReturn::crosses);
        }
    }

    public static class ST_Disjoint extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public Boolean eval(String left, String right) {
            return bool(WktUtils.read(left), WktUtils.read(right), PredicatesReturn::disjoint);
        }

        public Boolean eval(Geometry left, Geometry right) {
            return bool(left, right, PredicatesReturn::disjoint);
        }
    }

    public static class ST_Equals extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public Boolean eval(String left, String right) {
            return bool(WktUtils.read(left), WktUtils.read(right), PredicatesReturn::equals);
        }

        public Boolean eval(Geometry left, Geometry right) {
            return bool(left, right, PredicatesReturn::equals);
        }
    }

    public static class ST_OrderingEquals extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public Boolean eval(String left, String right) {
            return bool(WktUtils.read(left), WktUtils.read(right), PredicatesReturn::orderingEquals);
        }

        public Boolean eval(Geometry left, Geometry right) {
            return bool(left, right, PredicatesReturn::orderingEquals);
        }
    }

    public static class ST_Overlaps extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public Boolean eval(String left, String right) {
            return bool(WktUtils.read(left), WktUtils.read(right), PredicatesReturn::overlaps);
        }

        public Boolean eval(Geometry left, Geometry right) {
            return bool(left, right, PredicatesReturn::overlaps);
        }
    }

    public static class ST_Touches extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public Boolean eval(String left, String right) {
            return bool(WktUtils.read(left), WktUtils.read(right), PredicatesReturn::touches);
        }

        public Boolean eval(Geometry left, Geometry right) {
            return bool(left, right, PredicatesReturn::touches);
        }
    }

    public static class ST_Relate extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public String eval(String left, String right) {
            return text(WktUtils.read(left), WktUtils.read(right), PredicatesReturn::relate);
        }

        public String eval(Geometry left, Geometry right) {
            return text(left, right, PredicatesReturn::relate);
        }

        public Boolean eval(String left, String right, String matrix) {
            Geometry g1 = WktUtils.read(left);
            Geometry g2 = WktUtils.read(right);
            if (g1 == null || g2 == null || matrix == null) {
                return null;
            }
            return PredicatesReturn.relate(g1, g2, matrix);
        }

        public Boolean eval(Geometry left, Geometry right, String matrix) {
            if (left == null || right == null || matrix == null) {
                return null;
            }
            return PredicatesReturn.relate(left, right, matrix);
        }
    }

    public static class ST_RelateMatch extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public Boolean eval(String matrix1, String matrix2) {
            if (matrix1 == null || matrix2 == null) {
                return null;
            }
            return PredicatesReturn.relateMatch(matrix1, matrix2);
        }
    }

    public static class ST_DWithin extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public Boolean eval(String left, String right, Double distance) {
            return dWithin(WktUtils.read(left), WktUtils.read(right), distance, true);
        }

        public Boolean eval(Geometry left, Geometry right, Double distance) {
            return dWithin(left, right, distance, true);
        }

        public Boolean eval(String left, String right, Double distance, Boolean useSpheroid) {
            return dWithin(WktUtils.read(left), WktUtils.read(right), distance, useSpheroid);
        }

        public Boolean eval(Geometry left, Geometry right, Double distance, Boolean useSpheroid) {
            return dWithin(left, right, distance, useSpheroid);
        }

        private Boolean dWithin(Geometry left, Geometry right, Double distance, Boolean useSpheroid) {
            if (left == null || right == null || distance == null) {
                return null;
            }
            boolean spheroid = useSpheroid == null || useSpheroid;
            return PredicatesReturn.dWithin(left, right, distance, spheroid);
        }
    }
}
