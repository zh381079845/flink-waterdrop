package io.github.interestinglab.waterdrop.flink.transform.business;

import io.github.interestinglab.waterdrop.flink.transform.GeometryAreaUdf;
import io.github.interestinglab.waterdrop.flink.transform.GeometryAsTextUdf;
import io.github.interestinglab.waterdrop.flink.transform.GeometryBufferUdf;
import io.github.interestinglab.waterdrop.flink.transform.GeometryDistanceUdf;
import io.github.interestinglab.waterdrop.flink.transform.GeometryFromWktUdf;
import io.github.interestinglab.waterdrop.flink.transform.Predicates;
import io.github.interestinglab.waterdrop.flink.transform.WktUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.github.interestinglab.waterdrop.flink.transform.business.BusinessSampleData.require;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * 对照 docs/spatial-business-requirements.md 的 P0 业务验收（UDF 级）。
 * 每个 @Test 方法名以需求 ID 开头，便于对照报告。
 */
public class BusinessRequirementsAcceptanceTest {

    private static Map<String, BusinessSampleData.Feature> eco;
    private static Map<String, BusinessSampleData.Feature> farm;
    private static Map<String, BusinessSampleData.Feature> project;
    private static Map<String, BusinessSampleData.Feature> dev;
    private static Map<String, BusinessSampleData.Feature> facility;
    private static Map<String, BusinessSampleData.Feature> road;
    private static Map<String, BusinessSampleData.Feature> parcel;
    private static Map<String, BusinessSampleData.Feature> zone;

    private final Predicates.ST_Intersects intersects = new Predicates.ST_Intersects();
    private final Predicates.ST_Contains contains = new Predicates.ST_Contains();
    private final Predicates.ST_Within within = new Predicates.ST_Within();
    private final Predicates.ST_Overlaps overlaps = new Predicates.ST_Overlaps();
    private final Predicates.ST_Touches touches = new Predicates.ST_Touches();
    private final Predicates.ST_Disjoint disjoint = new Predicates.ST_Disjoint();
    private final Predicates.ST_Crosses crosses = new Predicates.ST_Crosses();
    private final Predicates.ST_Covers covers = new Predicates.ST_Covers();
    private final Predicates.ST_DWithin dWithin = new Predicates.ST_DWithin();
    private final Predicates.ST_Equals equals = new Predicates.ST_Equals();
    private final Predicates.ST_Relate relate = new Predicates.ST_Relate();
    private final GeometryAreaUdf area = new GeometryAreaUdf();
    private final GeometryBufferUdf buffer = new GeometryBufferUdf();
    private final GeometryDistanceUdf distance = new GeometryDistanceUdf();
    private final GeometryFromWktUdf fromWkt = new GeometryFromWktUdf();
    private final GeometryAsTextUdf asText = new GeometryAsTextUdf();

    @BeforeClass
    public static void loadLayers() throws Exception {
        eco = BusinessSampleData.load(BusinessSampleData.ECO);
        farm = BusinessSampleData.load(BusinessSampleData.FARM);
        project = BusinessSampleData.load(BusinessSampleData.PROJECT);
        dev = BusinessSampleData.load(BusinessSampleData.DEV);
        facility = BusinessSampleData.load(BusinessSampleData.FACILITY);
        road = BusinessSampleData.load(BusinessSampleData.ROAD);
        parcel = BusinessSampleData.load(BusinessSampleData.PARCEL);
        zone = BusinessSampleData.load(BusinessSampleData.ZONE);
    }

    // ---- 国土规划 ----

    @Test
    public void L01_ecoRedlineConflictScreening() {
        String prj = require(project, "PRJ-CONFLICT").wkt;
        String ok = require(project, "PRJ-OK").wkt;
        assertTrue(intersects.eval(prj, require(eco, "ECO-001").wkt));
        assertFalse(intersects.eval(ok, require(eco, "ECO-001").wkt));
    }

    @Test
    public void L02_farmlandOccupation() {
        assertTrue(intersects.eval(require(project, "PRJ-CONFLICT").wkt, require(farm, "FARM-001").wkt));
        assertFalse(intersects.eval(require(project, "PRJ-OK").wkt, require(farm, "FARM-001").wkt));
    }

    @Test
    public void L04_developmentBoundaryInsideOutside() {
        String boundary = require(dev, "DEV-001").wkt;
        assertTrue(within.eval(require(project, "PRJ-OK").wkt, boundary)
                || intersects.eval(require(project, "PRJ-OK").wkt, boundary));
        assertTrue(contains.eval(boundary, require(project, "PRJ-OK").wkt)
                || intersects.eval(boundary, require(project, "PRJ-OK").wkt));
        assertTrue(disjoint.eval(require(project, "PRJ-OUTSIDE").wkt, boundary));
    }

    @Test
    public void L05_multiPlanConflictDetection() {
        String prj = require(project, "PRJ-CONFLICT").wkt;
        List<String> conflicts = new ArrayList<String>();
        if (intersects.eval(prj, require(eco, "ECO-001").wkt)) {
            conflicts.add("eco");
        }
        if (intersects.eval(prj, require(farm, "FARM-001").wkt)) {
            conflicts.add("farm");
        }
        assertEquals(2, conflicts.size());
    }

    @Test
    public void L06_spheroidAreaAccounting() {
        Double m2 = area.eval(require(eco, "ECO-001").wkt);
        assertNotNull(m2);
        assertTrue(m2 > 1e7);
        assertTrue(m2 / 1e4 > 100); // >100 公顷量级
    }

    @Test
    public void L07_polygonWithHoleNetArea() {
        String hole = require(zone, "HOLE-PARK").wkt;
        String outer =
                "POLYGON((116.32 39.92, 116.32 40.02, 116.48 40.02, 116.48 39.92, 116.32 39.92))";
        assertTrue(area.eval(hole) < area.eval(outer));
    }

    @Test
    public void L08_multiParcelAreaSum() {
        String multi = require(zone, "MULTI-PARCEL").wkt;
        Double total = area.eval(multi);
        assertNotNull(total);
        assertTrue(total > 1e6);
    }

    @Test
    public void L14_multiConstraintVeto() {
        String site = require(project, "PRJ-POINT").wkt;
        boolean hitEco = intersects.eval(site, require(eco, "ECO-001").wkt)
                || within.eval(site, require(eco, "ECO-001").wkt);
        boolean hitHeritage = intersects.eval(site, require(zone, "HERITAGE-001").wkt);
        boolean nearHazard = dWithin.eval(site, require(facility, "HAZARD-001").wkt, 3000.0);
        assertTrue(hitEco || nearHazard);
        assertFalse(hitHeritage);
    }

    // ---- 不动产 ----

    @Test
    public void R01_parcelTopologyOverlap() {
        assertTrue(overlaps.eval(require(parcel, "PARCEL-A").wkt, require(parcel, "PARCEL-B").wkt));
        assertFalse(overlaps.eval(require(parcel, "PARCEL-A").wkt, require(parcel, "PARCEL-C").wkt));
    }

    @Test
    public void R04_houseFallsInParcel() {
        assertTrue(within.eval(require(parcel, "HOUSE-OK").wkt, require(parcel, "PARCEL-A").wkt));
        assertFalse(within.eval(require(parcel, "HOUSE-OUT").wkt, require(parcel, "PARCEL-A").wkt));
    }

    @Test
    public void R07_enclaveDistanceParts() {
        Geometry multi = fromWkt.eval(require(zone, "MULTI-PARCEL").wkt);
        assertNotNull(multi);
        assertEquals(2, multi.getNumGeometries());
        Double d = distance.eval(
                asText.eval(multi.getGeometryN(0)),
                asText.eval(multi.getGeometryN(1)));
        assertNotNull(d);
        assertTrue(d > 10000);
    }

    @Test
    public void R09_mortgageWithinOwnership() {
        assertTrue(covers.eval(require(parcel, "PARCEL-A").wkt, require(parcel, "HOUSE-OK").wkt)
                || contains.eval(require(parcel, "PARCEL-A").wkt, require(parcel, "HOUSE-OK").wkt));
        assertFalse(within.eval(require(parcel, "HOUSE-OUT").wkt, require(parcel, "PARCEL-A").wkt));
    }

    @Test
    public void R10_geometryEqualsRoundtrip() {
        String wkt = require(eco, "ECO-001").wkt;
        Geometry g = fromWkt.eval(wkt);
        String back = asText.eval(g);
        assertTrue(equals.eval(wkt, back) || intersects.eval(wkt, back));
    }

    // ---- 城建 / 市政 ----

    @Test
    public void U03_buildingDensityProxy() {
        Double land = area.eval(require(parcel, "PARCEL-A").wkt);
        Double house = area.eval(require(parcel, "HOUSE-OK").wkt);
        assertTrue(house / land < 0.5);
    }

    @Test
    public void U07_heritageControlZone() {
        assertFalse(intersects.eval(require(project, "PRJ-OK").wkt, require(zone, "HERITAGE-001").wkt));
        assertTrue(intersects.eval(
                buffer.eval(require(facility, "FIRE-001").wkt, 3000.0),
                require(zone, "HERITAGE-001").wkt)
                || dWithin.eval(require(facility, "FIRE-001").wkt, require(zone, "HERITAGE-001").wkt, 5000.0));
    }

    @Test
    public void U08_facilityServiceRadiusCoverage() {
        String service = buffer.eval(require(facility, "HOSP-001").wkt, 5000.0);
        assertTrue(intersects.eval(service, require(parcel, "RES-001").wkt));
        assertFalse(intersects.eval(service, require(parcel, "PARCEL-C").wkt));
    }

    @Test
    public void M01_roadCrossesEcoRedline() {
        assertTrue(crosses.eval(require(road, "ROAD-001").wkt, require(eco, "ECO-001").wkt)
                || intersects.eval(require(road, "ROAD-001").wkt, require(eco, "ECO-001").wkt));
        assertTrue(disjoint.eval(require(road, "ROAD-002").wkt, require(eco, "ECO-001").wkt));
    }

    @Test
    public void M02_pipelineSafetyDistance() {
        String pipe = require(road, "PIPE-001").wkt;
        String building = require(parcel, "BUILD-NEAR-PIPE").wkt;
        // 最近点语义：管线到建筑最短距离应很小；也可用 buffer∩建筑做合规
        Double nearest = distance.eval(pipe, building);
        assertNotNull(nearest);
        assertTrue("nearest pipeline distance too large: " + nearest, nearest < 50.0);
        assertTrue(dWithin.eval(pipe, building, 50.0));
        assertTrue(intersects.eval(buffer.eval(pipe, 50.0), building));
    }

    @Test
    public void M04_roadWidenLandTakeFilter() {
        String corridor = buffer.eval(require(road, "ROAD-001").wkt, 50.0);
        assertTrue(intersects.eval(corridor, require(eco, "ECO-001").wkt)
                || intersects.eval(corridor, require(farm, "FARM-001").wkt));
    }

    @Test
    public void M06_transitCoverageBlindSpot() {
        String coverage = buffer.eval(require(facility, "HOSP-001").wkt, 800.0);
        // 远端宗地应在小半径外
        assertTrue(disjoint.eval(coverage, require(parcel, "PARCEL-C").wkt)
                || !intersects.eval(coverage, require(parcel, "PARCEL-C").wkt));
    }

    // ---- 生态 / 应急 / 城管 ----

    @Test
    public void E01_waterProtectionZoning() {
        // 红线北部与一级水源保护区空间关系可判定
        Boolean hit = intersects.eval(require(eco, "ECO-001").wkt, require(zone, "WPZ-1").wkt);
        assertNotNull(hit);
        assertTrue(contains.eval(require(zone, "WPZ-2").wkt, require(zone, "WPZ-1").wkt)
                || intersects.eval(require(zone, "WPZ-2").wkt, require(zone, "WPZ-1").wkt));
    }

    @Test
    public void E06_sensitiveTargetBuffer() {
        assertTrue(dWithin.eval(
                require(facility, "HAZARD-001").wkt,
                require(facility, "SCH-001").wkt,
                20000.0));
        assertFalse(dWithin.eval(
                require(facility, "HAZARD-001").wkt,
                require(facility, "SCH-001").wkt,
                100.0));
    }

    @Test
    public void S01_hazardProtectionDistance() {
        Double d = distance.eval(require(facility, "HAZARD-001").wkt, require(facility, "HOSP-001").wkt);
        assertNotNull(d);
        assertTrue(d > 0);
    }

    @Test
    public void S02_evacuationRadiusAssets() {
        String ring = buffer.eval(require(facility, "HAZARD-001").wkt, 3000.0);
        assertTrue(intersects.eval(ring, require(parcel, "RES-001").wkt));
    }

    @Test
    public void S03_fireStationReach() {
        assertTrue(dWithin.eval(
                require(facility, "FIRE-001").wkt,
                require(facility, "HAZARD-001").wkt,
                8000.0));
    }

    @Test
    public void S05_floodAssetOverlay() {
        assertTrue(intersects.eval(require(zone, "FLOOD-001").wkt, require(parcel, "RES-001").wkt));
        assertFalse(intersects.eval(require(zone, "FLOOD-001").wkt, require(parcel, "PARCEL-C").wkt));
    }

    @Test
    public void C01_eventFallsInGrid() {
        assertTrue(within.eval(require(zone, "EVT-001").wkt, require(zone, "GRID-01").wkt));
        assertFalse(within.eval(require(zone, "EVT-001").wkt, require(zone, "GRID-02").wkt));
    }

    @Test
    public void C02_illegalBuildingBeyondParcel() {
        assertTrue(overlaps.eval(require(parcel, "HOUSE-OUT").wkt, require(parcel, "PARCEL-B").wkt)
                || intersects.eval(require(parcel, "HOUSE-OUT").wkt, require(parcel, "PARCEL-B").wkt));
        assertFalse(within.eval(require(parcel, "HOUSE-OUT").wkt, require(parcel, "PARCEL-A").wkt));
    }

    // ---- 商业 / 数据治理 ----

    @Test
    public void B02_competitorDistanceConstraint() {
        Double d = distance.eval(require(facility, "STORE-A").wkt, require(facility, "STORE-B").wkt);
        assertNotNull(d);
        assertTrue(d < 1000);
        assertTrue(dWithin.eval(require(facility, "STORE-A").wkt, require(facility, "STORE-B").wkt, 1000.0));
    }

    @Test
    public void B01_storeServiceCoverage() {
        String ring = buffer.eval(require(facility, "STORE-A").wkt, 2000.0);
        assertTrue(intersects.eval(ring, require(parcel, "PARCEL-A").wkt));
    }

    @Test
    public void D01_invalidWktQualityGate() {
        assertNull(fromWkt.eval("NOT_A_WKT"));
        assertNull(area.eval("POLYGON(("));
        assertNull(intersects.eval("BAD", require(eco, "ECO-001").wkt));
    }

    @Test
    public void D05_referentialIntegrityPointInParent() {
        assertTrue(within.eval(require(zone, "EVT-001").wkt, require(zone, "GRID-01").wkt));
    }

    @Test
    public void D07_envelopePrefilterCorrectness() {
        String a = require(eco, "ECO-001").wkt;
        String b = require(farm, "FARM-001").wkt;
        String far = require(farm, "FARM-002").wkt;
        assertTrue(envelopeOverlaps(a, b));
        assertTrue(intersects.eval(a, b));
        assertFalse(envelopeOverlaps(a, far));
        assertFalse(intersects.eval(a, far));
    }

    @Test
    public void D09_geoJsonNormalizedColumns() {
        assertTrue(eco.size() >= 2);
        assertNotNull(require(eco, "ECO-001").wkt);
        assertTrue(require(eco, "ECO-001").properties.contains("生态")
                || require(eco, "ECO-001").properties.contains("redline")
                || require(eco, "ECO-001").properties.contains("name"));
        assertNotNull(WktUtils.read(require(project, "PRJ-POINT").wkt));
    }

    @Test
    public void D13_de9imRelateAudit() {
        String matrix = relate.eval(require(parcel, "PARCEL-A").wkt, require(parcel, "PARCEL-B").wkt);
        assertNotNull(matrix);
        assertEquals(9, matrix.length());
    }

    @Test
    public void D14_nullSafeDoesNotThrow() {
        String nullWkt = null;
        assertNull(intersects.eval(nullWkt, require(eco, "ECO-001").wkt));
        assertNull(area.eval(nullWkt));
        assertNull(buffer.eval(nullWkt, 100.0));
    }

    // ---- 端到端组合（UDF 链路）----

    @Test
    public void L_END_01_projectCompliancePipeline() {
        String prj = require(project, "PRJ-CONFLICT").wkt;
        boolean veto = intersects.eval(prj, require(eco, "ECO-001").wkt)
                || intersects.eval(prj, require(farm, "FARM-001").wkt);
        boolean insideDev = intersects.eval(prj, require(dev, "DEV-001").wkt);
        Double footprint = area.eval(prj);
        assertTrue(veto);
        assertTrue(insideDev);
        assertTrue(footprint > 1e5);
    }

    @Test
    public void S_END_01_hazardImpactQuickAssessment() {
        String hazard = require(facility, "HAZARD-001").wkt;
        String zone1k = buffer.eval(hazard, 1000.0);
        String zone3k = buffer.eval(hazard, 3000.0);
        assertTrue(intersects.eval(zone3k, require(parcel, "RES-001").wkt));
        Double toFire = distance.eval(hazard, require(facility, "FIRE-001").wkt);
        assertNotNull(toFire);
        assertTrue(toFire > 0);
        assertNotNull(zone1k);
    }

    @Test
    public void B_END_01_storeSiteFeasibilityFilter() {
        String candidate = require(facility, "STORE-A").wkt;
        boolean onFarm = intersects.eval(candidate, require(farm, "FARM-001").wkt);
        boolean onEco = intersects.eval(candidate, require(eco, "ECO-001").wkt);
        boolean tooCloseCompetitor = dWithin.eval(candidate, require(facility, "STORE-B").wkt, 500.0);
        // 店A不在农田/红线，但距竞品过近 → 不可行
        assertFalse(onFarm);
        assertFalse(onEco);
        assertTrue(tooCloseCompetitor);
    }

    private boolean envelopeOverlaps(String a, String b) {
        Geometry ga = fromWkt.eval(a);
        Geometry gb = fromWkt.eval(b);
        return ga.getEnvelopeInternal().intersects(gb.getEnvelopeInternal());
    }
}
