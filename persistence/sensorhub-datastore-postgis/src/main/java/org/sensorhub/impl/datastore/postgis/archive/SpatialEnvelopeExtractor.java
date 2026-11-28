package org.sensorhub.impl.datastore.postgis.archive;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Extracts optional WGS-84 spatial coordinates from serialized SWE/GeoJSON
 * observation payloads with a zero-allocation fast path for non-spatial
 * fixed-portal streams.
 *
 * <p>Supports the canonical OSCAR mobile detector and future PWA/TAK payload
 * representations:
 * <ul>
 *   <li>Top-level {@code latitude}/{@code longitude} (optional {@code altitude})
 *       as emitted by {@code OccupancyExtended}.</li>
 *   <li>Top-level {@code lat}/{@code lon} (or {@code lng}, optional {@code alt}).</li>
 *   <li>Nested {@code sensorLocation} or {@code location} objects containing
 *       {@code lat}/{@code latitude} and {@code lon}/{@code lng}/{@code longitude}
 *       as emitted by RS-350 {@code LocationOutput} and Kromek mobile drivers.</li>
 *   <li>GeoJSON {@code geometry} Point objects ({@code coordinates: [lon, lat(, alt)]}).</li>
 * </ul>
 * Finite WGS-84 bounds ({@code -90 <= lat <= 90}, {@code -180 <= lon <= 180})
 * are required and uninitialized {@code (0.0, 0.0)} coordinates are ignored.</p>
 */
public final class SpatialEnvelopeExtractor {

    public record SpatialPoint(double lat, double lon, Double alt) {}

    public record SpatialBounds(double minLat, double maxLat, double minLon, double maxLon) {}

    public static final class BoundsAccumulator {
        private boolean hasPoint;
        private double minLat = Double.POSITIVE_INFINITY;
        private double maxLat = Double.NEGATIVE_INFINITY;
        private double minLon = Double.POSITIVE_INFINITY;
        private double maxLon = Double.NEGATIVE_INFINITY;

        public void observe(SpatialPoint point) {
            if (point == null)
                return;
            if (!hasPoint) {
                minLat = point.lat();
                maxLat = point.lat();
                minLon = point.lon();
                maxLon = point.lon();
                hasPoint = true;
            } else {
                if (point.lat() < minLat) minLat = point.lat();
                if (point.lat() > maxLat) maxLat = point.lat();
                if (point.lon() < minLon) minLon = point.lon();
                if (point.lon() > maxLon) maxLon = point.lon();
            }
        }

        public SpatialBounds finish() {
            return hasPoint ? new SpatialBounds(minLat, maxLat, minLon, maxLon) : null;
        }
    }

    private SpatialEnvelopeExtractor() {}

    /**
     * Fast-path substring check so 1 Hz fixed-portal telemetry rows that carry
     * no geographic fields never parse JSON.
     */
    public static boolean mayContainSpatialFields(String resultJson) {
        if (resultJson == null || resultJson.isEmpty())
            return false;
        return resultJson.indexOf("lat") >= 0
                || resultJson.indexOf("Lat") >= 0
                || resultJson.indexOf("Location") >= 0
                || resultJson.indexOf("location") >= 0
                || resultJson.indexOf("coordinates") >= 0
                || resultJson.indexOf("\\u") >= 0;
    }

    public static SpatialPoint extractPoint(String resultJson) {
        if (!mayContainSpatialFields(resultJson))
            return null;
        try {
            JsonElement parsed = JsonParser.parseString(resultJson);
            if (!parsed.isJsonObject())
                return null;
            return extractPoint(parsed.getAsJsonObject());
        } catch (RuntimeException ignored) {
            return null;
        }
    }

    public static SpatialPoint extractPoint(JsonObject obj) {
        if (obj == null)
            return null;

        // 1. Direct lat/lon or latitude/longitude on the root object
        SpatialPoint direct = extractFromCoordinateObject(obj);
        if (direct != null)
            return direct;

        // 2. Nested SWE vector objects (RS-350 sensorLocation, generic location)
        for (String key : new String[]{"sensorLocation", "location"}) {
            JsonElement nested = obj.get(key);
            if (nested != null && nested.isJsonObject()) {
                SpatialPoint point = extractFromCoordinateObject(nested.getAsJsonObject());
                if (point != null)
                    return point;
            }
        }

        // 3. GeoJSON geometry Point { "type": "Point", "coordinates": [lon, lat(, alt)] }
        JsonElement geometry = obj.get("geometry");
        if (geometry != null && geometry.isJsonObject()) {
            SpatialPoint point = extractFromGeoJsonPoint(geometry.getAsJsonObject());
            if (point != null)
                return point;
        }
        return extractFromGeoJsonPoint(obj);
    }

    private static SpatialPoint extractFromCoordinateObject(JsonObject obj) {
        Double lat = readFiniteDouble(obj, "latitude", "lat");
        Double lon = readFiniteDouble(obj, "longitude", "lon", "lng");
        if (lat == null || lon == null)
            return null;
        Double alt = readFiniteDouble(obj, "altitude", "alt");
        return validatedPoint(lat, lon, alt);
    }

    private static SpatialPoint extractFromGeoJsonPoint(JsonObject obj) {
        JsonElement coordsEl = obj.get("coordinates");
        if (coordsEl == null || !coordsEl.isJsonArray())
            return null;
        JsonElement typeEl = obj.get("type");
        if (typeEl != null && typeEl.isJsonPrimitive() && typeEl.getAsJsonPrimitive().isString()
                && !"Point".equalsIgnoreCase(typeEl.getAsString())) {
            return null;
        }
        JsonArray coords = coordsEl.getAsJsonArray();
        if (coords.size() < 2)
            return null;
        Double lon = asFiniteDouble(coords.get(0));
        Double lat = asFiniteDouble(coords.get(1));
        if (lat == null || lon == null)
            return null;
        Double alt = coords.size() >= 3 ? asFiniteDouble(coords.get(2)) : null;
        return validatedPoint(lat, lon, alt);
    }

    private static Double readFiniteDouble(JsonObject obj, String... keys) {
        for (String key : keys) {
            JsonElement el = obj.get(key);
            if (el != null && !el.isJsonNull()) {
                Double value = asFiniteDouble(el);
                if (value != null)
                    return value;
            }
        }
        return null;
    }

    private static Double asFiniteDouble(JsonElement el) {
        if (el == null || !el.isJsonPrimitive())
            return null;
        try {
            double val = el.getAsDouble();
            return Double.isFinite(val) ? val : null;
        } catch (RuntimeException ignored) {
            return null;
        }
    }

    private static SpatialPoint validatedPoint(double lat, double lon, Double alt) {
        if (!Double.isFinite(lat) || !Double.isFinite(lon))
            return null;
        if (lat < -90.0 || lat > 90.0 || lon < -180.0 || lon > 180.0)
            return null;
        // Ignore uninitialized (0.0, 0.0) GPS placeholders per OccupancyExtended.hasLocation().
        if (lat == 0.0 && lon == 0.0)
            return null;
        Double validAlt = (alt != null && Double.isFinite(alt)) ? alt : null;
        return new SpatialPoint(lat, lon, validAlt);
    }
}
