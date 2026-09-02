package io.github.interestinglab.waterdrop.flink.transform;

import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.operation.valid.IsValidOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * st_isvalid / st_makevalid。
 * makevalid：旧版 JTS 无 GeometryFixer，用 buffer(0) 启发式修复自相交面。
 */
public final class GeometryValidUdf {

    private static final Logger LOG = LoggerFactory.getLogger(GeometryValidUdf.class);

    private GeometryValidUdf() {
    }

    public static class IsValid extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public Boolean eval(String wkt) {
            return valid(WktUtils.read(wkt));
        }

        public Boolean eval(Geometry geometry) {
            return valid(geometry);
        }

        private Boolean valid(Geometry geometry) {
            if (geometry == null) {
                return null;
            }
            return new IsValidOp(geometry).isValid();
        }
    }

    public static class MakeValid extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public String eval(String wkt) {
            return WktUtils.write(fix(WktUtils.read(wkt)));
        }

        public Geometry eval(Geometry geometry) {
            return fix(geometry);
        }

        private Geometry fix(Geometry geometry) {
            if (geometry == null) {
                return null;
            }
            try {
                if (new IsValidOp(geometry).isValid()) {
                    return geometry;
                }
                Geometry fixed = geometry.buffer(0.0);
                return fixed == null || fixed.isEmpty() ? geometry : fixed;
            } catch (Exception e) {
                LOG.warn("st_makevalid failed: {}", e.toString());
                return null;
            }
        }
    }
}
