package io.github.interestinglab.waterdrop.flink.transform;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

/**
 * 空间 UDF 集中注册，可供 SpatialUdfRegister / SpatialJoin 复用（幂等）。
 */
public final class SpatialFunctionRegistry {

    private SpatialFunctionRegistry() {
    }

    public static void registerAll(TableEnvironment tableEnv) {
        registerAll(tableEnv, "st_buffer");
    }

    public static void registerAll(TableEnvironment tableEnv, String bufferUdfName) {
        tableEnv.registerFunction(bufferUdfName, new GeometryBufferUdf());
        tableEnv.registerFunction("st_area", new GeometryAreaUdf());
        tableEnv.registerFunction("st_geomfromwkt", new GeometryFromWktUdf());
        tableEnv.registerFunction("st_astext", new GeometryAsTextUdf());
        tableEnv.registerFunction("st_distance", new GeometryDistanceUdf());
        tableEnv.registerFunction("st_envelope", new GeometryBoundsUdf.EnvelopeWkt());
        tableEnv.registerFunction("st_xmin", new GeometryBoundsUdf.XMin());
        tableEnv.registerFunction("st_xmax", new GeometryBoundsUdf.XMax());
        tableEnv.registerFunction("st_ymin", new GeometryBoundsUdf.YMin());
        tableEnv.registerFunction("st_ymax", new GeometryBoundsUdf.YMax());

        tableEnv.registerFunction("st_intersection", new GeometryOverlayUdf.Intersection());
        tableEnv.registerFunction("st_difference", new GeometryOverlayUdf.Difference());
        tableEnv.registerFunction("st_union", new GeometryOverlayUdf.Union());
        tableEnv.registerFunction("st_symdifference", new GeometryOverlayUdf.SymDifference());

        tableEnv.registerFunction("st_length", new GeometryLengthUdf.Length());
        tableEnv.registerFunction("st_perimeter", new GeometryLengthUdf.Perimeter());
        tableEnv.registerFunction("st_point", new GeometryConstructUdf.PointFromXY());
        tableEnv.registerFunction("st_centroid", new GeometryConstructUdf.Centroid());
        tableEnv.registerFunction("st_x", new GeometryConstructUdf.X());
        tableEnv.registerFunction("st_y", new GeometryConstructUdf.Y());
        tableEnv.registerFunction("st_isvalid", new GeometryValidUdf.IsValid());
        tableEnv.registerFunction("st_makevalid", new GeometryValidUdf.MakeValid());
        tableEnv.registerFunction("st_asgeojson", new GeometryAsGeoJsonUdf());
        tableEnv.registerFunction("st_simplify", new GeometrySimplifyUdf());

        tableEnv.registerFunction("st_geohash", new GeometryGridUdf.GeoHashFn());
        tableEnv.registerFunction("st_grid_id", new GeometryGridUdf.GridId());
        tableEnv.registerFunction("st_grid_ids", new GeometryGridUdf.GridIds());
        registerGridCellsIdempotent(tableEnv);

        tableEnv.registerFunction("st_intersects", new Predicates.ST_Intersects());
        tableEnv.registerFunction("st_contains", new Predicates.ST_Contains());
        tableEnv.registerFunction("st_within", new Predicates.ST_Within());
        tableEnv.registerFunction("st_covers", new Predicates.ST_Covers());
        tableEnv.registerFunction("st_coveredby", new Predicates.ST_CoveredBy());
        tableEnv.registerFunction("st_crosses", new Predicates.ST_Crosses());
        tableEnv.registerFunction("st_disjoint", new Predicates.ST_Disjoint());
        tableEnv.registerFunction("st_equals", new Predicates.ST_Equals());
        tableEnv.registerFunction("st_orderingequals", new Predicates.ST_OrderingEquals());
        tableEnv.registerFunction("st_overlaps", new Predicates.ST_Overlaps());
        tableEnv.registerFunction("st_touches", new Predicates.ST_Touches());
        tableEnv.registerFunction("st_dwithin", new Predicates.ST_DWithin());
        tableEnv.registerFunction("st_relate", new Predicates.ST_Relate());
        tableEnv.registerFunction("st_relatematch", new Predicates.ST_RelateMatch());
    }

    private static void registerGridCellsIdempotent(TableEnvironment tableEnv) {
        try {
            tableEnv.createTemporarySystemFunction("st_grid_cells", new GeometryGridUdf.GridCells());
        } catch (ValidationException ignored) {
            // 已由 SpatialUdfRegister 或其他插件注册
        }
    }

    public static void registerJtsKryoTypes(ExecutionConfig executionConfig) {
        Class<?>[] types = new Class<?>[]{
                Geometry.class,
                Point.class,
                LineString.class,
                LinearRing.class,
                Polygon.class,
                MultiPoint.class,
                MultiLineString.class,
                MultiPolygon.class,
                GeometryCollection.class,
                Coordinate.class,
                Envelope.class
        };
        for (Class<?> type : types) {
            executionConfig.registerKryoType(type);
        }
    }
}
