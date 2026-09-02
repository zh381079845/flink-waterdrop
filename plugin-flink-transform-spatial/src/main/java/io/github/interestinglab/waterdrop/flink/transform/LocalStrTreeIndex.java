package io.github.interestinglab.waterdrop.flink.transform;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.strtree.STRtree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 单机/单 Task 局部 STRtree：适合广播小维表后对流式大表探测（Sedona broadcast index 思路）。
 */
public class LocalStrTreeIndex implements Serializable {

    private static final long serialVersionUID = 1L;

    private final STRtree tree = new STRtree();
    private boolean built;

    public void insert(String id, Geometry geometry) {
        if (id == null || geometry == null || geometry.isEmpty()) {
            return;
        }
        if (built) {
            throw new IllegalStateException("index already built");
        }
        tree.insert(geometry.getEnvelopeInternal(), new Entry(id, geometry));
    }

    public void build() {
        tree.build();
        built = true;
    }

    @SuppressWarnings("unchecked")
    public List<Entry> queryEnvelope(Geometry probe) {
        if (probe == null || probe.isEmpty()) {
            return Collections.emptyList();
        }
        ensureBuilt();
        List<Entry> raw = tree.query(probe.getEnvelopeInternal());
        return raw == null ? Collections.<Entry>emptyList() : raw;
    }

    /**
     * 包络候选后再精确谓词。
     */
    public List<Entry> queryIntersects(Geometry probe) {
        List<Entry> candidates = queryEnvelope(probe);
        if (candidates.isEmpty()) {
            return candidates;
        }
        List<Entry> hits = new ArrayList<Entry>();
        for (Entry e : candidates) {
            if (e.geometry.intersects(probe)) {
                hits.add(e);
            }
        }
        return hits;
    }

    private void ensureBuilt() {
        if (!built) {
            build();
        }
    }

    public static final class Entry implements Serializable {
        private static final long serialVersionUID = 1L;
        public final String id;
        public final Geometry geometry;

        public Entry(String id, Geometry geometry) {
            this.id = id;
            this.geometry = geometry;
        }
    }
}
