package io.github.interestinglab.waterdrop.flink.transform;

import org.apache.flink.table.functions.ScalarFunction;
import org.geotools.geojson.geom.GeometryJSON;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;

/**
 * st_asgeojson：Geometry/WKT → GeoJSON geometry 对象字符串。
 */
public class GeometryAsGeoJsonUdf extends ScalarFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(GeometryAsGeoJsonUdf.class);
    private static final ThreadLocal<GeometryJSON> JSON =
            ThreadLocal.withInitial(() -> new GeometryJSON(8));

    public String eval(String wkt) {
        return toGeoJson(WktUtils.read(wkt));
    }

    public String eval(Geometry geometry) {
        return toGeoJson(geometry);
    }

    private String toGeoJson(Geometry geometry) {
        if (geometry == null) {
            return null;
        }
        try {
            StringWriter writer = new StringWriter();
            JSON.get().write(geometry, writer);
            return writer.toString();
        } catch (IOException e) {
            LOG.warn("st_asgeojson failed: {}", e.toString());
            return null;
        }
    }
}
