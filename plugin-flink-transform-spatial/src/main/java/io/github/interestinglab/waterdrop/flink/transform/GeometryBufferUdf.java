package io.github.interestinglab.waterdrop.flink.transform;

import org.apache.flink.table.functions.ScalarFunction;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Geometry;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

/**
 * 空间缓冲区 UDF。默认把 EPSG:4326 转到 Web Mercator 后再按米缓冲，结果仍输出 WKT。
 */
public class GeometryBufferUdf extends ScalarFunction {

    private static final long serialVersionUID = 1L;

    private static final String DEFAULT_CRS = "EPSG:4326";
    private static final String DEFAULT_BUFFER_CRS = "EPSG:3857";

    public String eval(String wkt, Double distance) {
        return WktUtils.write(bufferMeters(WktUtils.read(wkt), distance, false));
    }

    public String eval(String wkt, Double distance, Boolean simpleBuffer) {
        return WktUtils.write(bufferMeters(WktUtils.read(wkt), distance, simpleBuffer));
    }

    public String eval(Geometry geometry, Double distance) {
        return WktUtils.write(bufferMeters(geometry, distance, false));
    }

    public String eval(Geometry geometry, Double distance, Boolean simpleBuffer) {
        return WktUtils.write(bufferMeters(geometry, distance, simpleBuffer));
    }

    private Geometry bufferMeters(Geometry geometry, Double distance, Boolean simpleBuffer) {
        if (geometry == null || distance == null) {
            return null;
        }
        if (simpleBuffer != null && simpleBuffer) {
            return geometry.buffer(distance);
        }
        try {
            CoordinateReferenceSystem sourceCRS = CRS.decode(DEFAULT_CRS, true);
            CoordinateReferenceSystem targetCRS = CRS.decode(DEFAULT_BUFFER_CRS);
            MathTransform transform = CRS.findMathTransform(sourceCRS, targetCRS, true);
            Geometry projectedGeometry = JTS.transform(geometry, transform);
            Geometry bufferedGeometry = projectedGeometry.buffer(distance);
            MathTransform inverseTransform = transform.inverse();
            return JTS.transform(bufferedGeometry, inverseTransform);
        } catch (FactoryException | TransformException e) {
            throw new RuntimeException("Failed to compute buffer for geometry", e);
        }
    }
}
