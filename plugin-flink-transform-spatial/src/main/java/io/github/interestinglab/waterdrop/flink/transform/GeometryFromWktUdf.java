package io.github.interestinglab.waterdrop.flink.transform;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.Geometry;

/**
 * 将 WKT 字符串解析为 JTS Geometry，对应 SQL 函数 st_geomfromwkt。
 */
public class GeometryFromWktUdf extends ScalarFunction {

    private static final long serialVersionUID = 1L;

    public Geometry eval(String wkt) {
        return WktUtils.read(wkt);
    }

    @Override
    public TypeInformation<?> getResultType(Class<?>[] signature) {
        return TypeInformation.of(Geometry.class);
    }
}
