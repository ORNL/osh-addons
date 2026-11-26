package org.sensorhub.impl.datastore.postgis.archive;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Compact 256-byte (2,048-bit) SplitMix64 double-hashed Bloom filter and
 * min/max zone-map summary for cold {@code vehicleOcr} {@code normalizedValue}
 * pruning in {@code oscar_archive_schema} without growing PostgreSQL indexes.
 */
public final class OcrBloomFilter {
    public static final int BLOOM_BYTES = 256;
    private static final int BLOOM_BITS = BLOOM_BYTES * 8;
    private static final int HASH_COUNT = 4;
    public static final byte[] EMPTY_INDEXED_SENTINEL = new byte[]{0};

    private OcrBloomFilter() {
    }

    public record Snapshot(byte[] bloomBytes, String minNorm, String maxNorm) {
        public boolean hasValues() {
            return bloomBytes != null && bloomBytes.length == BLOOM_BYTES;
        }
    }

    public static final class Accumulator {
        private byte[] bits;
        private String minNorm;
        private String maxNorm;

        public void addFromResultJson(String resultJson) {
            if (resultJson == null ||
                    (resultJson.indexOf("normalizedValue") < 0 && resultJson.indexOf("\\u") < 0)) {
                return;
            }
            try {
                JsonElement parsed = JsonParser.parseString(resultJson);
                if (!parsed.isJsonObject())
                    return;
                JsonObject obj = parsed.getAsJsonObject();
                JsonElement norm = obj.get("normalizedValue");
                if (norm != null && norm.isJsonPrimitive() && norm.getAsJsonPrimitive().isString()) {
                    addNormalizedValue(norm.getAsString());
                }
            } catch (RuntimeException ignored) {
                // Malformed payload rows do not contribute normalized OCR tokens.
            }
        }

        public void addNormalizedValue(String value) {
            if (value == null)
                return;
            if (bits == null)
                bits = new byte[BLOOM_BYTES];
            if (minNorm == null || value.compareTo(minNorm) < 0)
                minNorm = value;
            if (maxNorm == null || value.compareTo(maxNorm) > 0)
                maxNorm = value;

            String upper = value.toUpperCase(Locale.ROOT);
            insertToken(bits, "E:" + upper);
            if (upper.length() >= 1)
                insertToken(bits, "P1:" + upper.substring(0, 1));
            if (upper.length() >= 2)
                insertToken(bits, "P2:" + upper.substring(0, 2));
            for (int i = 0; i <= upper.length() - 3; i++) {
                insertToken(bits, "T:" + upper.substring(i, i + 3));
            }
        }

        public Snapshot finish() {
            if (bits == null)
                return new Snapshot(EMPTY_INDEXED_SENTINEL.clone(), null, null);
            return new Snapshot(bits.clone(), minNorm, maxNorm);
        }
    }

    public static boolean mightMatchExact(
            byte[] bloom,
            String minNorm,
            String maxNorm,
            String expected,
            boolean matchingCase) {
        if (bloom == null || bloom.length == 0)
            return true;
        if (bloom.length == 1)
            return false;
        if (bloom.length != BLOOM_BYTES)
            return true;
        if (expected == null)
            return false;
        if (!matchingCase && !isPureAscii(expected))
            return true;
        if (matchingCase && minNorm != null && maxNorm != null) {
            if (expected.compareTo(minNorm) < 0 || expected.compareTo(maxNorm) > 0)
                return false;
        }
        String upper = expected.toUpperCase(Locale.ROOT);
        if (!containsToken(bloom, "E:" + upper))
            return false;
        for (int i = 0; i <= upper.length() - 3; i++) {
            if (!containsToken(bloom, "T:" + upper.substring(i, i + 3)))
                return false;
        }
        return true;
    }

    public static boolean mightMatchLike(
            byte[] bloom,
            String minNorm,
            String maxNorm,
            String literal,
            String wildcard,
            String singleChar,
            String escape,
            boolean matchingCase) {
        if (bloom == null || bloom.length == 0)
            return true;
        if (bloom.length == 1)
            return false;
        if (bloom.length != BLOOM_BYTES)
            return true;
        if (literal == null)
            return false;
        if (!matchingCase && !isPureAscii(literal))
            return true;

        String wc = wildcard == null ? "" : wildcard;
        String sc = singleChar == null ? "" : singleChar;
        String esc = escape == null ? "" : escape;

        StringBuilder currentRun = new StringBuilder();
        List<String> runs = new ArrayList<>();
        String leadingPrefix = null;
        boolean sawWildcard = false;

        for (int index = 0; index < literal.length(); ) {
            if (!esc.isEmpty() && literal.startsWith(esc, index)) {
                index += esc.length();
                if (index < literal.length()) {
                    int cp = literal.codePointAt(index);
                    currentRun.appendCodePoint(cp);
                    index += Character.charCount(cp);
                }
            } else if (!wc.isEmpty() && literal.startsWith(wc, index)) {
                if (!sawWildcard) {
                    leadingPrefix = currentRun.toString();
                    sawWildcard = true;
                }
                if (!currentRun.isEmpty()) {
                    runs.add(currentRun.toString());
                    currentRun.setLength(0);
                }
                index += wc.length();
            } else if (!sc.isEmpty() && literal.startsWith(sc, index)) {
                if (!sawWildcard) {
                    leadingPrefix = currentRun.toString();
                    sawWildcard = true;
                }
                if (!currentRun.isEmpty()) {
                    runs.add(currentRun.toString());
                    currentRun.setLength(0);
                }
                index += sc.length();
            } else {
                int cp = literal.codePointAt(index);
                currentRun.appendCodePoint(cp);
                index += Character.charCount(cp);
            }
        }
        if (!currentRun.isEmpty()) {
            runs.add(currentRun.toString());
        }
        if (!sawWildcard) {
            return mightMatchExact(bloom, minNorm, maxNorm, currentRun.toString(), matchingCase);
        }

        if (leadingPrefix != null && !leadingPrefix.isEmpty()) {
            if (matchingCase && minNorm != null && maxNorm != null) {
                if (maxNorm.compareTo(leadingPrefix) < 0)
                    return false;
                if (minNorm.length() >= leadingPrefix.length() &&
                        minNorm.substring(0, leadingPrefix.length()).compareTo(leadingPrefix) > 0) {
                    return false;
                }
            }
            String upperPrefix = leadingPrefix.toUpperCase(Locale.ROOT);
            if (!containsToken(bloom, "P1:" + upperPrefix.substring(0, 1)))
                return false;
            if (upperPrefix.length() >= 2 && !containsToken(bloom, "P2:" + upperPrefix.substring(0, 2)))
                return false;
        }

        for (String run : runs) {
            if (run.length() < 3)
                continue;
            String upperRun = run.toUpperCase(Locale.ROOT);
            for (int i = 0; i <= upperRun.length() - 3; i++) {
                if (!containsToken(bloom, "T:" + upperRun.substring(i, i + 3)))
                    return false;
            }
        }
        return true;
    }

    private static boolean isPureAscii(String value) {
        for (int i = 0; i < value.length(); i++) {
            if (value.charAt(i) > 0x7F)
                return false;
        }
        return true;
    }

    private static void insertToken(byte[] bits, String token) {
        byte[] utf8 = token.getBytes(StandardCharsets.UTF_8);
        long h1 = mix64(fnv1a64(utf8, 0xcbf29ce484222325L));
        long h2 = mix64(fnv1a64(utf8, 0x84222b25cbf29ce4L)) | 1L;
        for (int i = 0; i < HASH_COUNT; i++) {
            int bitIndex = (int) ((h1 + i * h2) & (BLOOM_BITS - 1));
            bits[bitIndex >>> 3] |= (byte) (1 << (bitIndex & 7));
        }
    }

    private static boolean containsToken(byte[] bits, String token) {
        byte[] utf8 = token.getBytes(StandardCharsets.UTF_8);
        long h1 = mix64(fnv1a64(utf8, 0xcbf29ce484222325L));
        long h2 = mix64(fnv1a64(utf8, 0x84222b25cbf29ce4L)) | 1L;
        for (int i = 0; i < HASH_COUNT; i++) {
            int bitIndex = (int) ((h1 + i * h2) & (BLOOM_BITS - 1));
            if ((bits[bitIndex >>> 3] & (1 << (bitIndex & 7))) == 0)
                return false;
        }
        return true;
    }

    private static long fnv1a64(byte[] bytes, long seed) {
        long h = seed;
        for (byte b : bytes) {
            h ^= (b & 0xffL);
            h *= 0x100000001b3L;
        }
        return h;
    }

    private static long mix64(long z) {
        z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
        z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
        return z ^ (z >>> 31);
    }
}
