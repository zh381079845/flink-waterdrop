package io.github.interestinglab.waterdrop.flink.transform.business;

import io.github.interestinglab.waterdrop.flink.source.GeoJsonSource;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 业务样例图层加载器：读取 test/resources/business/*.geojson。
 */
public final class BusinessSampleData {

    public static final String ECO = "business/eco_redline.geojson";
    public static final String FARM = "business/farmland.geojson";
    public static final String PROJECT = "business/projects.geojson";
    public static final String DEV = "business/development_boundary.geojson";
    public static final String FACILITY = "business/facilities.geojson";
    public static final String ROAD = "business/roads_pipelines.geojson";
    public static final String PARCEL = "business/parcels_houses.geojson";
    public static final String ZONE = "business/zones_grids.geojson";

    private BusinessSampleData() {
    }

    public static Map<String, Feature> load(String resourcePath) throws Exception {
        URL url = BusinessSampleData.class.getClassLoader().getResource(resourcePath);
        if (url == null) {
            throw new IllegalStateException("missing resource: " + resourcePath);
        }
        ListCollector collector = new ListCollector();
        new GeoJsonSource.GeoJsonFlatMap().flatMap(url.getPath(), collector);
        Map<String, Feature> map = new LinkedHashMap<String, Feature>();
        for (Row row : collector.rows) {
            Feature f = new Feature(
                    (String) row.getField(0),
                    (String) row.getField(1),
                    (String) row.getField(2));
            map.put(f.id, f);
        }
        return map;
    }

    public static Feature require(Map<String, Feature> layer, String id) {
        Feature f = layer.get(id);
        if (f == null) {
            throw new IllegalStateException("feature not found: " + id);
        }
        return f;
    }

    public static final class Feature {
        public final String id;
        public final String wkt;
        public final String properties;

        public Feature(String id, String wkt, String properties) {
            this.id = id;
            this.wkt = wkt;
            this.properties = properties;
        }
    }

    private static class ListCollector implements Collector<Row> {
        private final List<Row> rows = new ArrayList<Row>();

        @Override
        public void collect(Row record) {
            rows.add(record);
        }

        @Override
        public void close() {
        }
    }
}
