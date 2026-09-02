package io.github.interestinglab.waterdrop.flink.transform;

import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertTrue;

/**
 * 微基准：千面建 {@link LocalStrTreeIndex}，百万点探测（SpatialBroadcastJoin 热路径）。
 * <p>
 * 规模可用 JVM 属性覆盖：
 * <ul>
 *   <li>{@code -Dspatial.bench.polys=1000}</li>
 *   <li>{@code -Dspatial.bench.points=1000000}</li>
 * </ul>
 */
public class SpatialBroadcastJoinMicroBenchmark {

    private static final GeometryFactory GF = new GeometryFactory();

    @Test
    public void millionPointsAgainstThousandPolygons() {
        int polyCount = intProp("spatial.bench.polys", 1000);
        int pointCount = intProp("spatial.bench.points", 1_000_000);
        Random rnd = new Random(42);

        Map<String, String> build = new LinkedHashMap<String, String>();
        List<Polygon> polys = new ArrayList<Polygon>(polyCount);
        for (int i = 0; i < polyCount; i++) {
            double ox = 100.0 + (i % 50) * 0.2;
            double oy = 30.0 + (i / 50) * 0.2;
            Polygon p = square(ox, oy, 0.08);
            polys.add(p);
            build.put("p" + i, p.toText());
        }

        long t0 = System.nanoTime();
        LocalStrTreeIndex index = SpatialBroadcastJoin.buildIndex(build.entrySet());
        long buildMs = (System.nanoTime() - t0) / 1_000_000L;

        // 约 1% 点落入若干多边形中心，其余均匀散射（保证有命中可校验）
        long hits = 0;
        int expectedSeedHits = 0;
        t0 = System.nanoTime();
        for (int i = 0; i < pointCount; i++) {
            String wkt;
            if (i < polyCount && i % 100 == 0) {
                // 确定性命中：落在第 i 个方格中心
                Polygon p = polys.get(i);
                Point c = p.getCentroid();
                wkt = String.format(Locale.ROOT, "POINT(%s %s)", c.getX(), c.getY());
                expectedSeedHits++;
            } else {
                double x = 99.0 + rnd.nextDouble() * 12.0;
                double y = 29.0 + rnd.nextDouble() * 12.0;
                wkt = String.format(Locale.ROOT, "POINT(%s %s)", x, y);
            }
            hits += SpatialBroadcastJoin.probeHits(index, wkt).size();
        }
        long queryMs = (System.nanoTime() - t0) / 1_000_000L;
        double ptsPerSec = pointCount * 1000.0 / Math.max(1, queryMs);

        System.out.printf(Locale.ROOT,
                "SpatialBroadcastJoin microbench: polys=%d points=%d build=%dms query=%dms hits=%d throughput=%.0f pts/s%n",
                polyCount, pointCount, buildMs, queryMs, hits, ptsPerSec);

        assertTrue("index build should finish", buildMs >= 0);
        assertTrue("should find seeded hits", hits >= expectedSeedHits);
        // 粗吞吐门槛：本地一般远高于此；防止回归到 O(n*m) 嵌套循环
        assertTrue("throughput too low (possible nested-loop regression): " + ptsPerSec,
                ptsPerSec > 50_000);

        // 正确性抽样：中心点应命中对应面
        List<String> hit0 = SpatialBroadcastJoin.probeHits(index, polys.get(0).getCentroid().toText());
        assertTrue(hit0.contains("p0"));
        assertTrue(SpatialBroadcastJoin.probeHits(index, "POINT(0 0)").isEmpty());
    }

    private static Polygon square(double minX, double minY, double size) {
        Coordinate[] cs = new Coordinate[]{
                new Coordinate(minX, minY),
                new Coordinate(minX, minY + size),
                new Coordinate(minX + size, minY + size),
                new Coordinate(minX + size, minY),
                new Coordinate(minX, minY)
        };
        return GF.createPolygon(cs);
    }

    private static int intProp(String key, int def) {
        String v = System.getProperty(key);
        if (v == null || v.isEmpty()) {
            return def;
        }
        return Integer.parseInt(v);
    }
}
