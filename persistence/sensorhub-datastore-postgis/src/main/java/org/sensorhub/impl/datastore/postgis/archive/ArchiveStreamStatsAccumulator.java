package org.sensorhub.impl.datastore.postgis.archive;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.time.Instant;
import java.util.Map;
import org.sensorhub.impl.datastore.postgis.builder.filter.JsonCqlEvaluator;

/**
 * Single-pass streaming accumulator for all 26 {@code oscar_archive_schema}
 * summary counters, OCR Bloom filter sketches, event-UUID locators, and
 * optional WGS-84 spatial bounding envelopes.
 *
 * <p>Shared by both initial export activation ({@link ArchiveCatalog}) and
 * multi-tier event compaction ({@link ArchiveCompactionManager}) so both
 * pipelines remain in exact lockstep without scanning PostgreSQL temporary
 * tables or generating MVCC dead tuples.</p>
 */
final class ArchiveStreamStatsAccumulator {
    long rowCount;
    long gammaAlarmCount;
    long neutronAlarmCount;
    long gammaNeutronAlarmCount;
    long nonAlarmOccupancyCount;
    long gammaTrueCount;
    long neutronTrueCount;
    long anyAlarmCount;
    long unadjudicatedAlarmCount;
    long adjudicatedAlarmCount;
    long unadjudicatedGammaAlarmCount;
    long unadjudicatedNeutronAlarmCount;
    long unadjudicatedGammaNeutronAlarmCount;
    long adjudicatedGammaAlarmCount;
    long adjudicatedNeutronAlarmCount;
    long adjudicatedGammaNeutronAlarmCount;
    long gammaHighFaultCount;
    long gammaLowFaultCount;
    long neutronFaultCount;
    long neutronHighFaultCount;
    long extendedOccupancyFaultCount;
    long tamperTrueCount;
    long rs350GammaAlarmCount;
    long rs350NeutronAlarmCount;
    long alarmCategoryGammaCount;
    long alarmCategoryNeutronCount;
    final OcrBloomFilter.Accumulator ocrAccumulator = new OcrBloomFilter.Accumulator();
    final SpatialEnvelopeExtractor.BoundsAccumulator spatialBounds = new SpatialEnvelopeExtractor.BoundsAccumulator();

    int observeRow(long obsId, String resultJson, Map<String, Long> eventLocators, boolean trackOcr) {
        rowCount++;
        if (resultJson == null || resultJson.isBlank())
            return JsonCqlEvaluator.BIT_MASK_V2;

        boolean maybeSummary = resultJson.indexOf("gammaAlarm") >= 0
                || resultJson.indexOf("neutronAlarm") >= 0
                || resultJson.indexOf("alarmState") >= 0
                || resultJson.indexOf("tamperStatus") >= 0
                || resultJson.indexOf("derivedData") >= 0
                || resultJson.indexOf("alarmCategoryCode") >= 0
                || resultJson.indexOf("adjudicatedIds") >= 0
                || resultJson.indexOf("\\u") >= 0;
        boolean maybeOcr = trackOcr && (resultJson.indexOf("normalizedValue") >= 0 || resultJson.indexOf("\\u") >= 0);
        boolean maybeEventId = resultJson.indexOf("eventId") >= 0 || resultJson.indexOf("\\u") >= 0;
        boolean maybeSpatial = SpatialEnvelopeExtractor.mayContainSpatialFields(resultJson);
        if (!maybeSummary && !maybeOcr && !maybeEventId && !maybeSpatial)
            return JsonCqlEvaluator.BIT_MASK_V2;

        int mask = maybeSummary
                ? JsonCqlEvaluator.computeSummaryMask(resultJson)
                : JsonCqlEvaluator.BIT_MASK_V2;
        if (maybeSummary) {
            boolean gTrue = (mask & JsonCqlEvaluator.BIT_GAMMA_TRUE) != 0;
            boolean gFalse = (mask & JsonCqlEvaluator.BIT_GAMMA_FALSE) != 0;
            boolean nTrue = (mask & JsonCqlEvaluator.BIT_NEUTRON_TRUE) != 0;
            boolean nFalse = (mask & JsonCqlEvaluator.BIT_NEUTRON_FALSE) != 0;

            if (gTrue && nFalse) gammaAlarmCount++;
            if (gFalse && nTrue) neutronAlarmCount++;
            if (gTrue && nTrue) gammaNeutronAlarmCount++;
            if (gFalse && nFalse) nonAlarmOccupancyCount++;
            if (gTrue) gammaTrueCount++;
            if (nTrue) neutronTrueCount++;

            if (gTrue || nTrue) {
                anyAlarmCount++;
                AdjudicationState adjState = classifyAlarmAdjudicationForSummary(resultJson, mask);
                if (adjState == AdjudicationState.UNADJUDICATED) {
                    unadjudicatedAlarmCount++;
                    if (gTrue && nFalse) unadjudicatedGammaAlarmCount++;
                    if (gFalse && nTrue) unadjudicatedNeutronAlarmCount++;
                    if (gTrue && nTrue) unadjudicatedGammaNeutronAlarmCount++;
                } else if (adjState == AdjudicationState.ADJUDICATED) {
                    adjudicatedAlarmCount++;
                    if (gTrue && nFalse) adjudicatedGammaAlarmCount++;
                    if (gFalse && nTrue) adjudicatedNeutronAlarmCount++;
                    if (gTrue && nTrue) adjudicatedGammaNeutronAlarmCount++;
                }
            }
            if ((mask & JsonCqlEvaluator.BIT_FAULT_GAMMA_HIGH) != 0) gammaHighFaultCount++;
            if ((mask & JsonCqlEvaluator.BIT_FAULT_GAMMA_LOW) != 0) gammaLowFaultCount++;
            if ((mask & JsonCqlEvaluator.BIT_FAULT_NEUTRON) != 0) neutronFaultCount++;
            if ((mask & JsonCqlEvaluator.BIT_FAULT_NEUTRON_HIGH) != 0) neutronHighFaultCount++;
            if ((mask & JsonCqlEvaluator.BIT_FAULT_EXT_OCC) != 0) extendedOccupancyFaultCount++;
            if ((mask & JsonCqlEvaluator.BIT_TAMPER_TRUE) != 0) tamperTrueCount++;
            if ((mask & JsonCqlEvaluator.BIT_RS350_GAMMA) != 0) rs350GammaAlarmCount++;
            if ((mask & JsonCqlEvaluator.BIT_RS350_NEUTRON) != 0) rs350NeutronAlarmCount++;
            if ((mask & JsonCqlEvaluator.BIT_ALARM_CAT_GAMMA) != 0) alarmCategoryGammaCount++;
            if ((mask & JsonCqlEvaluator.BIT_ALARM_CAT_NEUTRON) != 0) alarmCategoryNeutronCount++;
        }

        if (maybeOcr || maybeEventId || maybeSpatial) {
            try {
                JsonElement parsed = JsonParser.parseString(resultJson);
                if (parsed.isJsonObject()) {
                    JsonObject obj = parsed.getAsJsonObject();
                    if (maybeOcr) {
                        JsonElement norm = obj.get("normalizedValue");
                        if (norm != null && norm.isJsonPrimitive() && norm.getAsJsonPrimitive().isString()) {
                            ocrAccumulator.addNormalizedValue(norm.getAsString());
                        }
                    }
                    if (maybeEventId && eventLocators != null) {
                        JsonElement ev = obj.get("eventId");
                        if (ev != null && ev.isJsonPrimitive() && ev.getAsJsonPrimitive().isString()) {
                            String evId = ev.getAsString();
                            if (!evId.isEmpty() && evId.length() <= 256) {
                                eventLocators.merge(evId, obsId, Math::max);
                            }
                        }
                    }
                    if (maybeSpatial) {
                        spatialBounds.observe(SpatialEnvelopeExtractor.extractPoint(obj));
                    }
                }
            } catch (RuntimeException ignored) {
            }
        }
        return mask;
    }

    ArchiveCatalog.StreamSummaryCounters finish() {
        return new ArchiveCatalog.StreamSummaryCounters(
                rowCount,
                gammaAlarmCount,
                neutronAlarmCount,
                gammaNeutronAlarmCount,
                nonAlarmOccupancyCount,
                gammaTrueCount,
                neutronTrueCount,
                anyAlarmCount,
                unadjudicatedAlarmCount,
                adjudicatedAlarmCount,
                unadjudicatedGammaAlarmCount,
                unadjudicatedNeutronAlarmCount,
                unadjudicatedGammaNeutronAlarmCount,
                adjudicatedGammaAlarmCount,
                adjudicatedNeutronAlarmCount,
                adjudicatedGammaNeutronAlarmCount,
                gammaHighFaultCount,
                gammaLowFaultCount,
                neutronFaultCount,
                neutronHighFaultCount,
                extendedOccupancyFaultCount,
                tamperTrueCount,
                rs350GammaAlarmCount,
                rs350NeutronAlarmCount,
                alarmCategoryGammaCount,
                alarmCategoryNeutronCount,
                ocrAccumulator.finish(),
                spatialBounds.finish());
    }

    static ArchiveCatalog.StreamLatestCandidate pickNewerLatest(
            ArchiveCatalog.StreamLatestCandidate prev,
            ArchiveCatalog.StreamLatestCandidate cand) {
        if (prev.phenomenonTime() == null && cand.phenomenonTime() == null) {
            return cand.observationId() >= prev.observationId() ? cand : prev;
        }
        if (prev.phenomenonTime() == null)
            return cand;
        if (cand.phenomenonTime() == null)
            return prev;
        int cmp = cand.phenomenonTime().compareTo(prev.phenomenonTime());
        if (cmp != 0)
            return cmp > 0 ? cand : prev;
        Instant prevRes = prev.resultTime();
        Instant candRes = cand.resultTime();
        if (prevRes != null || candRes != null) {
            if (prevRes == null)
                return cand;
            if (candRes == null)
                return prev;
            int rCmp = candRes.compareTo(prevRes);
            if (rCmp != 0)
                return rCmp > 0 ? cand : prev;
        }
        return cand.observationId() >= prev.observationId() ? cand : prev;
    }

    private enum AdjudicationState {
        UNADJUDICATED,
        ADJUDICATED,
        NEITHER
    }

    private static AdjudicationState classifyAlarmAdjudicationForSummary(String resultJson, int mask) {
        if ((mask & JsonCqlEvaluator.BIT_ADJ_ZERO) != 0)
            return AdjudicationState.UNADJUDICATED;
        if ((mask & JsonCqlEvaluator.BIT_ADJ_GT_ZERO) != 0)
            return AdjudicationState.ADJUDICATED;
        try {
            JsonElement parsed = JsonParser.parseString(resultJson);
            if (!parsed.isJsonObject())
                return AdjudicationState.NEITHER;
            JsonObject obj = parsed.getAsJsonObject();
            JsonElement arr = obj.get("adjudicatedIds");
            if (arr != null && !arr.isJsonNull()) {
                if (arr.isJsonArray()) {
                    return arr.getAsJsonArray().isEmpty()
                            ? AdjudicationState.UNADJUDICATED
                            : AdjudicationState.ADJUDICATED;
                }
                return AdjudicationState.NEITHER;
            }
            JsonElement cnt = obj.get("adjudicatedIdsCount");
            if (cnt == null || cnt.isJsonNull())
                return AdjudicationState.UNADJUDICATED;
            String cntStr = cnt.isJsonPrimitive() ? cnt.getAsString() : "";
            return "0".equals(cntStr) ? AdjudicationState.UNADJUDICATED : AdjudicationState.ADJUDICATED;
        } catch (RuntimeException ignored) {
            return AdjudicationState.NEITHER;
        }
    }
}
