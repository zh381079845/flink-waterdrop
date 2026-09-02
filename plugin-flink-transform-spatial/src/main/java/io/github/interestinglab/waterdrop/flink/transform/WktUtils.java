package io.github.interestinglab.waterdrop.flink.transform;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 线程安全的 WKT 读写。JTS 的 WKTReader / WKTWriter 都不是线程安全的，
 * 不能放在 static 字段里给 Flink 并行算子共用。
 */
public final class WktUtils {

    private static final Logger LOG = LoggerFactory.getLogger(WktUtils.class);

    private static final ThreadLocal<WKTReader> READER = ThreadLocal.withInitial(WKTReader::new);
    private static final ThreadLocal<WKTWriter> WRITER = ThreadLocal.withInitial(WKTWriter::new);

    private WktUtils() {
    }

    public static Geometry read(String wkt) {
        if (wkt == null) {
            return null;
        }
        try {
            return READER.get().read(wkt);
        } catch (ParseException e) {
            LOG.warn("Invalid WKT, skip geometry: {}", abbreviate(wkt));
            return null;
        }
    }

    public static String write(Geometry geometry) {
        if (geometry == null) {
            return null;
        }
        return WRITER.get().write(geometry);
    }

    /**
     * SQL 入参可能是 WKT 字符串，也可能已经是 JTS Geometry。
     */
    public static Geometry toGeometry(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Geometry) {
            return (Geometry) value;
        }
        if (value instanceof String) {
            return read((String) value);
        }
        LOG.warn("Unsupported geometry input type: {}", value.getClass().getName());
        return null;
    }

    private static String abbreviate(String wkt) {
        if (wkt.length() <= 120) {
            return wkt;
        }
        return wkt.substring(0, 120) + "...";
    }
}
