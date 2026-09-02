package io.github.interestinglab.waterdrop.flink.source;

import com.alibaba.fastjson.JSONObject;
import io.github.interestinglab.waterdrop.common.config.CheckConfigUtil;
import io.github.interestinglab.waterdrop.common.config.CheckResult;
import io.github.interestinglab.waterdrop.config.Config;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import io.github.interestinglab.waterdrop.flink.batch.FlinkBatchSource;
import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamSource;
import io.github.interestinglab.waterdrop.flink.transform.WktUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.geojson.feature.FeatureJSON;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Serializable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * 读取 GeoJSON FeatureCollection 或单个 Feature，输出 id / geometry(WKT) / properties。
 */
public class GeoJsonSource implements FlinkStreamSource<Row>, FlinkBatchSource<Row> {

    private static final String PATH = "path";
    private static final RowTypeInfo ROW_TYPE = new RowTypeInfo(
            new org.apache.flink.api.common.typeinfo.TypeInformation[]{
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO
            },
            new String[]{"id", "geometry", "properties"}
    );

    private Config config;
    private String path;

    @Override
    public DataStream<Row> getStreamData(FlinkEnvironment env) {
        return env.getStreamExecutionEnvironment()
                .fromElements(path)
                .flatMap(new GeoJsonFlatMap())
                .returns(ROW_TYPE);
    }

    @Override
    public DataSet<Row> getBatchData(FlinkEnvironment env) {
        return env.getBatchEnvironment()
                .fromElements(path)
                .flatMap(new GeoJsonFlatMap())
                .returns(ROW_TYPE);
    }

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public CheckResult checkConfig() {
        return CheckConfigUtil.check(config, PATH);
    }

    @Override
    public void prepare(FlinkEnvironment prepareEnv) {
        path = config.getString(PATH);
    }

    public static class GeoJsonFlatMap implements FlatMapFunction<String, Row>, Serializable {

        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(String filePath, Collector<Row> out) throws Exception {
            File file = resolveFile(filePath);
            String content = new String(Files.readAllBytes(Paths.get(file.getAbsolutePath())), StandardCharsets.UTF_8);
            String trimmed = content.trim();
            FeatureJSON featureJSON = new FeatureJSON();
            if (trimmed.contains("FeatureCollection")) {
                try (Reader reader = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8)) {
                    FeatureCollection collection = featureJSON.readFeatureCollection(reader);
                    emitCollection(collection, out);
                }
            } else {
                SimpleFeature feature = featureJSON.readFeature(trimmed);
                out.collect(toRow(feature));
            }
        }

        @SuppressWarnings("rawtypes")
        private void emitCollection(FeatureCollection collection, Collector<Row> out) {
            FeatureIterator iterator = collection.features();
            try {
                while (iterator.hasNext()) {
                    Object next = iterator.next();
                    if (next instanceof SimpleFeature) {
                        out.collect(toRow((SimpleFeature) next));
                    }
                }
            } finally {
                iterator.close();
            }
        }

        private Row toRow(SimpleFeature feature) {
            String id = feature.getID();
            Object geomObj = feature.getDefaultGeometry();
            Geometry geometry = geomObj instanceof Geometry ? (Geometry) geomObj : null;
            JSONObject properties = new JSONObject();
            for (Property property : feature.getProperties()) {
                Object value = property.getValue();
                if (value instanceof Geometry) {
                    continue;
                }
                properties.put(property.getName().getLocalPart(), value);
            }
            return Row.of(id, WktUtils.write(geometry), properties.toJSONString());
        }

        private File resolveFile(String filePath) {
            if (filePath.startsWith("file:")) {
                return new File(URI.create(filePath));
            }
            return new File(filePath);
        }
    }
}
