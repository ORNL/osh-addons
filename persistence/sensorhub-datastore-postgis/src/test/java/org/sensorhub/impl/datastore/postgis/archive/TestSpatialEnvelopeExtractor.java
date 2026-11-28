package org.sensorhub.impl.datastore.postgis.archive;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link SpatialEnvelopeExtractor} covering all supported WGS-84
 * coordinate payload formats, fast-path non-spatial rejection, bounds validation,
 * and {@link SpatialEnvelopeExtractor.BoundsAccumulator} envelopes.
 */
public class TestSpatialEnvelopeExtractor {

    @Test
    public void fastPathSkipsFixedPortalNonSpatialPayloads() {
        String fixedOccupancy = "{\"eventId\":\"occ-1\",\"gammaAlarm\":true,\"neutronAlarm\":false,\"maxGamma\":412}";
        String fixedGamma = "{\"gammaCounts\":118,\"alarmState\":\"Background\"}";

        assertFalse(SpatialEnvelopeExtractor.mayContainSpatialFields(fixedOccupancy));
        assertFalse(SpatialEnvelopeExtractor.mayContainSpatialFields(fixedGamma));
        assertNull(SpatialEnvelopeExtractor.extractPoint(fixedOccupancy));
        assertNull(SpatialEnvelopeExtractor.extractPoint(fixedGamma));
        assertNull(SpatialEnvelopeExtractor.extractPoint((String) null));
        assertNull(SpatialEnvelopeExtractor.extractPoint(""));
    }

    @Test
    public void extractsTopLevelLatitudeLongitudeAndAltitude() {
        String json = "{\"eventId\":\"mob-1\",\"latitude\":35.9281,\"longitude\":-84.3105,\"altitude\":265.4,\"gammaAlarm\":true}";
        assertTrue(SpatialEnvelopeExtractor.mayContainSpatialFields(json));
        SpatialEnvelopeExtractor.SpatialPoint p = SpatialEnvelopeExtractor.extractPoint(json);
        assertNotNull(p);
        assertEquals(35.9281, p.lat(), 1e-6);
        assertEquals(-84.3105, p.lon(), 1e-6);
        assertNotNull(p.alt());
        assertEquals(265.4, p.alt(), 1e-6);
    }

    @Test
    public void extractsShortLatLonLngAltKeys() {
        SpatialEnvelopeExtractor.SpatialPoint p1 =
                SpatialEnvelopeExtractor.extractPoint("{\"lat\":-33.8688,\"lon\":151.2093,\"alt\":42.0}");
        assertNotNull(p1);
        assertEquals(-33.8688, p1.lat(), 1e-6);
        assertEquals(151.2093, p1.lon(), 1e-6);
        assertEquals(42.0, p1.alt(), 1e-6);

        SpatialEnvelopeExtractor.SpatialPoint p2 =
                SpatialEnvelopeExtractor.extractPoint("{\"lat\":48.8566,\"lng\":2.3522}");
        assertNotNull(p2);
        assertEquals(48.8566, p2.lat(), 1e-6);
        assertEquals(2.3522, p2.lon(), 1e-6);
        assertNull(p2.alt());
    }

    @Test
    public void extractsNestedSensorLocationAndGenericLocation() {
        String rs350 = "{\"alarmCategoryCode\":\"Gamma\",\"sensorLocation\":{\"lat\":35.9312,\"lon\":-84.3099,\"alt\":270.0}}";
        SpatialEnvelopeExtractor.SpatialPoint p1 = SpatialEnvelopeExtractor.extractPoint(rs350);
        assertNotNull(p1);
        assertEquals(35.9312, p1.lat(), 1e-6);
        assertEquals(-84.3099, p1.lon(), 1e-6);
        assertEquals(270.0, p1.alt(), 1e-6);

        String mobile = "{\"location\":{\"latitude\":37.7749,\"longitude\":-122.4194,\"altitude\":18.5}}";
        SpatialEnvelopeExtractor.SpatialPoint p2 = SpatialEnvelopeExtractor.extractPoint(mobile);
        assertNotNull(p2);
        assertEquals(37.7749, p2.lat(), 1e-6);
        assertEquals(-122.4194, p2.lon(), 1e-6);
        assertEquals(18.5, p2.alt(), 1e-6);
    }

    @Test
    public void extractsGeoJsonPointCoordinatesLonLatOrder() {
        String geoJson = "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[-84.3105,35.9281,260.0]}}";
        SpatialEnvelopeExtractor.SpatialPoint p = SpatialEnvelopeExtractor.extractPoint(geoJson);
        assertNotNull(p);
        assertEquals(35.9281, p.lat(), 1e-6);
        assertEquals(-84.3105, p.lon(), 1e-6);
        assertEquals(260.0, p.alt(), 1e-6);

        // Non-Point GeoJSON geometries are ignored rather than misread.
        assertNull(SpatialEnvelopeExtractor.extractPoint(
                "{\"geometry\":{\"type\":\"LineString\",\"coordinates\":[[-84.3,35.9],[-84.2,35.9]]}}"));
    }

    @Test
    public void rejectsNullIslandAndOutOfRangeCoordinates() {
        assertNull(SpatialEnvelopeExtractor.extractPoint("{\"latitude\":0.0,\"longitude\":0.0}"));
        assertNull(SpatialEnvelopeExtractor.extractPoint("{\"latitude\":91.0,\"longitude\":10.0}"));
        assertNull(SpatialEnvelopeExtractor.extractPoint("{\"latitude\":-90.1,\"longitude\":10.0}"));
        assertNull(SpatialEnvelopeExtractor.extractPoint("{\"latitude\":10.0,\"longitude\":181.0}"));
        assertNull(SpatialEnvelopeExtractor.extractPoint("{\"latitude\":10.0,\"longitude\":-180.5}"));
        assertNull(SpatialEnvelopeExtractor.extractPoint("{\"latitude\":\"bad\",\"longitude\":10.0}"));
        assertNull(SpatialEnvelopeExtractor.extractPoint("{not valid json lat"));
    }

    @Test
    public void boundsAccumulatorComputesExactMinMaxEnvelope() {
        SpatialEnvelopeExtractor.BoundsAccumulator empty = new SpatialEnvelopeExtractor.BoundsAccumulator();
        empty.observe(null);
        assertNull(empty.finish());

        SpatialEnvelopeExtractor.BoundsAccumulator acc = new SpatialEnvelopeExtractor.BoundsAccumulator();
        acc.observe(new SpatialEnvelopeExtractor.SpatialPoint(35.9200, -84.3100, 260.0));
        acc.observe(new SpatialEnvelopeExtractor.SpatialPoint(35.9500, -84.2800, 275.0));
        acc.observe(new SpatialEnvelopeExtractor.SpatialPoint(35.9100, -84.3400, null));
        acc.observe(null);

        SpatialEnvelopeExtractor.SpatialBounds bounds = acc.finish();
        assertNotNull(bounds);
        assertEquals(35.9100, bounds.minLat(), 1e-6);
        assertEquals(35.9500, bounds.maxLat(), 1e-6);
        assertEquals(-84.3400, bounds.minLon(), 1e-6);
        assertEquals(-84.2800, bounds.maxLon(), 1e-6);
    }
}
