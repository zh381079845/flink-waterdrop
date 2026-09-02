package io.github.interestinglab.waterdrop.flink.source;

import io.github.interestinglab.waterdrop.flink.transform.GeometryIntersectsUdf;
import io.github.interestinglab.waterdrop.flink.transform.WktUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class GeoJsonSourceTest {

    @Test
    public void readsFeatureCollectionAsWktRows() throws Exception {
        URL resource = getClass().getClassLoader().getResource("spatial_sample.geojson");
        assertNotNull(resource);
        ListCollector collector = new ListCollector();
        new GeoJsonSource.GeoJsonFlatMap().flatMap(resource.getPath(), collector);

        assertEquals(2, collector.rows.size());
        Row first = collector.rows.get(0);
        assertEquals("eco-1", first.getField(0));
        Geometry geom = WktUtils.read((String) first.getField(1));
        assertNotNull(geom);
        assertEquals("Polygon", geom.getGeometryType());
        String properties = (String) first.getField(2);
        assertTrue(properties.contains("生态保护红线"));
    }

    @Test
    public void samplePolygonsIntersect() throws Exception {
        URL resource = getClass().getClassLoader().getResource("spatial_sample.geojson");
        assertNotNull(resource);
        ListCollector collector = new ListCollector();
        new GeoJsonSource.GeoJsonFlatMap().flatMap(resource.getPath(), collector);
        String left = (String) collector.rows.get(0).getField(1);
        String right = (String) collector.rows.get(1).getField(1);
        assertTrue(new GeometryIntersectsUdf().eval(left, right));
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
