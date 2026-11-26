package org.sensorhub.impl.datastore.postgis.archive;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Extracts the nine typed scalar event columns projected into Parquet V2
 * event generations ({@code RAW_EVENTS}, {@code DAILY_EVENTS}, and
 * {@code MONTHLY_EVENTS}).
 *
 * <p>Pre-extracting these scalars once in Java before native
 * {@code DuckDBAppender} insertion avoids instantiating dozens of native
 * {@code 8 MiB} DuckDB {@code yyjson} arenas during sorted Parquet writes,
 * keeping peak native memory well below the 256 MiB session ceiling while
 * enabling row-group min/max page indexes and Split-Block Bloom filters on
 * {@code ocr_norm}, {@code occ_obs_id}, and {@code event_id}.</p>
 */
final class ProjectedEventColumns {

    static final ProjectedEventColumns EMPTY = new ProjectedEventColumns(
            null, null, null, null, null, null, null, null, null);

    private final String ocrNorm;
    private final String occObsId;
    private final String eventId;
    private final Integer adjCode;
    private final Integer adjCount;
    private final Boolean gammaAlarm;
    private final Boolean neutronAlarm;
    private final Double maxGamma;
    private final Double maxNeutron;

    private ProjectedEventColumns(
            String ocrNorm,
            String occObsId,
            String eventId,
            Integer adjCode,
            Integer adjCount,
            Boolean gammaAlarm,
            Boolean neutronAlarm,
            Double maxGamma,
            Double maxNeutron) {
        this.ocrNorm = ocrNorm;
        this.occObsId = occObsId;
        this.eventId = eventId;
        this.adjCode = adjCode;
        this.adjCount = adjCount;
        this.gammaAlarm = gammaAlarm;
        this.neutronAlarm = neutronAlarm;
        this.maxGamma = maxGamma;
        this.maxNeutron = maxNeutron;
    }

    String ocrNorm() {
        return ocrNorm;
    }

    String occObsId() {
        return occObsId;
    }

    String eventId() {
        return eventId;
    }

    Integer adjCode() {
        return adjCode;
    }

    Integer adjCount() {
        return adjCount;
    }

    Boolean gammaAlarm() {
        return gammaAlarm;
    }

    Boolean neutronAlarm() {
        return neutronAlarm;
    }

    Double maxGamma() {
        return maxGamma;
    }

    Double maxNeutron() {
        return maxNeutron;
    }

    static ProjectedEventColumns extract(String resultJson) {
        if (resultJson == null || resultJson.isBlank())
            return EMPTY;
        try {
            JsonElement parsed = JsonParser.parseString(resultJson);
            if (!parsed.isJsonObject())
                return EMPTY;
            JsonObject obj = parsed.getAsJsonObject();
            String ocrNorm = extractJsonString(obj, "normalizedValue");
            String occObsId = extractJsonString(obj, "occupancyObsId");
            String eventId = extractJsonString(obj, "eventId");
            Integer adjCode = extractJsonInteger(obj.get("adjudicationCode"));
            Integer adjCount = extractAdjudicatedCount(obj);
            Boolean gammaAlarm = extractJsonBoolean(obj, "gammaAlarm");
            Boolean neutronAlarm = extractJsonBoolean(obj, "neutronAlarm");
            Double maxGamma = extractJsonDouble(obj.get("maxGammaCount"));
            Double maxNeutron = extractJsonDouble(obj.get("maxNeutronCount"));
            return new ProjectedEventColumns(
                    ocrNorm, occObsId, eventId, adjCode, adjCount,
                    gammaAlarm, neutronAlarm, maxGamma, maxNeutron);
        } catch (RuntimeException ignored) {
            return EMPTY;
        }
    }

    private static String extractJsonString(JsonObject obj, String key) {
        JsonElement el = obj.get(key);
        if (el != null && el.isJsonPrimitive() && el.getAsJsonPrimitive().isString())
            return el.getAsString();
        return null;
    }

    private static Boolean extractJsonBoolean(JsonObject obj, String key) {
        JsonElement el = obj.get(key);
        if (el != null && el.isJsonPrimitive() && el.getAsJsonPrimitive().isBoolean())
            return el.getAsBoolean();
        return null;
    }

    private static Integer extractJsonInteger(JsonElement el) {
        if (el == null || !el.isJsonPrimitive() || !el.getAsJsonPrimitive().isNumber())
            return null;
        try {
            double d = el.getAsDouble();
            if (!Double.isFinite(d))
                return null;
            long rounded = Math.round(d);
            if (rounded < Integer.MIN_VALUE || rounded > Integer.MAX_VALUE)
                return null;
            return (int) rounded;
        } catch (RuntimeException ignored) {
            return null;
        }
    }

    private static Integer extractAdjudicatedCount(JsonObject obj) {
        JsonElement cnt = obj.get("adjudicatedIdsCount");
        if (cnt != null && !cnt.isJsonNull()) {
            return extractJsonInteger(cnt);
        }
        JsonElement arr = obj.get("adjudicatedIds");
        if (arr != null && arr.isJsonArray()) {
            return arr.getAsJsonArray().size();
        }
        return null;
    }

    private static Double extractJsonDouble(JsonElement el) {
        if (el == null || !el.isJsonPrimitive() || !el.getAsJsonPrimitive().isNumber())
            return null;
        try {
            double d = el.getAsDouble();
            return Double.isFinite(d) ? d : null;
        } catch (RuntimeException ignored) {
            return null;
        }
    }
}
