package io.github.interestinglab.waterdrop.flink.transform;

import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.Geometry;

/**
 * 外包矩形及相关坐标，供空间 JOIN 包络预过滤使用。
 */
public class GeometryBoundsUdf {

    public static class EnvelopeWkt extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public String eval(String wkt) {
            return writeEnvelope(WktUtils.read(wkt));
        }

        public String eval(Geometry geometry) {
            return writeEnvelope(geometry);
        }

        private String writeEnvelope(Geometry geometry) {
            if (geometry == null) {
                return null;
            }
            return WktUtils.write(geometry.getEnvelope());
        }
    }

    public static class XMin extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public Double eval(String wkt) {
            return minX(WktUtils.read(wkt));
        }

        public Double eval(Geometry geometry) {
            return minX(geometry);
        }

        private Double minX(Geometry geometry) {
            if (geometry == null) {
                return null;
            }
            return geometry.getEnvelopeInternal().getMinX();
        }
    }

    public static class XMax extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public Double eval(String wkt) {
            return maxX(WktUtils.read(wkt));
        }

        public Double eval(Geometry geometry) {
            return maxX(geometry);
        }

        private Double maxX(Geometry geometry) {
            if (geometry == null) {
                return null;
            }
            return geometry.getEnvelopeInternal().getMaxX();
        }
    }

    public static class YMin extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public Double eval(String wkt) {
            return minY(WktUtils.read(wkt));
        }

        public Double eval(Geometry geometry) {
            return minY(geometry);
        }

        private Double minY(Geometry geometry) {
            if (geometry == null) {
                return null;
            }
            return geometry.getEnvelopeInternal().getMinY();
        }
    }

    public static class YMax extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public Double eval(String wkt) {
            return maxY(WktUtils.read(wkt));
        }

        public Double eval(Geometry geometry) {
            return maxY(geometry);
        }

        private Double maxY(Geometry geometry) {
            if (geometry == null) {
                return null;
            }
            return geometry.getEnvelopeInternal().getMaxY();
        }
    }
}
