package org.sensorhub.impl.datastore.postgis.builder.filter;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import org.geotools.api.filter.And;
import org.geotools.api.filter.BinaryComparisonOperator;
import org.geotools.api.filter.Filter;
import org.geotools.api.filter.Not;
import org.geotools.api.filter.Or;
import org.geotools.api.filter.PropertyIsEqualTo;
import org.geotools.api.filter.PropertyIsGreaterThan;
import org.geotools.api.filter.PropertyIsGreaterThanOrEqualTo;
import org.geotools.api.filter.PropertyIsLessThan;
import org.geotools.api.filter.PropertyIsLessThanOrEqualTo;
import org.geotools.api.filter.PropertyIsLike;
import org.geotools.api.filter.PropertyIsNotEqualTo;
import org.geotools.api.filter.expression.Literal;
import org.geotools.api.filter.expression.PropertyName;
import org.sensorhub.impl.datastore.postgis.archive.OcrBloomFilter;
import org.sensorhub.impl.datastore.postgis.archive.ParquetArchiveReader;

import java.io.StringReader;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

/** Evaluates the supported OSCAR CQL subset directly against archived JSON. */
public final class JsonCqlEvaluator {
    private static final Pattern SAFE_JSON_KEY = Pattern.compile("^[A-Za-z0-9_-]+$");
    private static final Pattern SAFE_JSON_DOLLAR_PATH = Pattern.compile("^[A-Za-z0-9_]+(?:\\.[A-Za-z0-9_]+)*$");
    private static final Pattern SAFE_JSON_STRING_LITERAL = Pattern.compile("^[A-Za-z0-9 _.:-]+$");
    private static final Set<String> EXACT_DUCKDB_SCALAR_KEYS = Set.of(
            "gammaAlarm", "neutronAlarm", "tamperStatus",
            "alarmState", "eventId", "laneId",
            "maxGamma", "maxNeutron", "maxGammaCount", "maxNeutronCount",
            "occupancyCount", "neutronBackground",
            "normalizedValue", "occupancyObsId", "adjudicationCode", "vehicleId",
            "startTime", "endTime", "samplingTime");
    private static final Set<String> LIKE_EXACT_DUCKDB_STRING_KEYS = Set.of(
            "normalizedValue", "vehicleId", "eventId", "occupancyObsId",
            "alarmState", "laneId");
    private static final Pattern RFC3339_INSTANT = Pattern.compile(
            "^[0-9]{4}-(0[1-9]|1[0-2])-([0-2][0-9]|3[01])T" +
            "([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](\\.[0-9]{1,9})?" +
            "(Z|[+-]((0[0-9]|1[0-7]):[0-5][0-9]|18:00))$");

    public enum ProjectedColumnMode {
        NONE,
        PURE_V2_EVENTS,
        MIXED_UNION_BY_NAME
    }

    @FunctionalInterface
    public interface OcrGenerationMatcher {
        boolean mightMatch(byte[] bloom, String minNorm, String maxNorm);
    }

    private JsonCqlEvaluator() {
    }

    public static boolean matches(Filter filter, String json) {
        if (filter == null || filter == Filter.INCLUDE)
            return true;
        if (filter == Filter.EXCLUDE)
            return false;
        JsonElement value = JsonParser.parseString(json);
        if (!value.isJsonObject())
            return false;
        return evaluate(filter, value.getAsJsonObject());
    }

    /**
     * Compiles {@code filter} once into a reusable row matcher that pre-compiles
     * property paths, LIKE regexes, and scalar/RFC-3339 literals, and skips
     * unreferenced top-level JSON arrays/objects (such as {@code gammaCount} and
     * {@code neutronCount}) via streaming {@link JsonReader#skipValue()}.
     */
    public static Predicate<String> compileMatcher(Filter filter) {
        if (filter == null || filter == Filter.INCLUDE)
            return json -> true;
        if (filter == Filter.EXCLUDE)
            return json -> false;
        Set<String> topLevelKeys = new LinkedHashSet<>();
        boolean[] allSupported = {true};
        CompiledNode root = compileNode(filter, topLevelKeys, allSupported);
        Set<String> projectedKeys = allSupported[0] ? Collections.unmodifiableSet(topLevelKeys) : null;
        return json -> {
            JsonObject obj = parseProjectedJsonObject(json, projectedKeys);
            if (obj == null)
                return false;
            return root.evaluate(obj);
        };
    }

    /**
     * Appends a sound (zero-false-negative) DuckDB SQL prefilter over {@code column} when possible.
     * Callers must still run {@link #matches(Filter, String)} on surviving candidate rows.
     */
    public static boolean appendDuckDbPrefilter(
            Filter filter, String column, StringBuilder sql, List<Object> parameters) {
        return appendDuckDbPrefilter(filter, column, ProjectedColumnMode.NONE, sql, parameters);
    }

    public static boolean appendDuckDbPrefilter(
            Filter filter, String column, ProjectedColumnMode mode,
            StringBuilder sql, List<Object> parameters) {
        if (mode != null && mode != ProjectedColumnMode.NONE) {
            Optional<ExactDuckDbPredicate> exact = tryBuildExactDuckDbPredicate(filter, column, mode);
            if (exact.isPresent()) {
                ExactDuckDbPredicate pred = exact.get();
                if ("FALSE".equals(pred.sql())) {
                    sql.append(" AND FALSE");
                } else if (!"TRUE".equals(pred.sql())) {
                    sql.append(" AND (").append(pred.sql()).append(')');
                    parameters.addAll(pred.parameters());
                }
                return true;
            }
        }
        PrefilterClause clause = buildDuckDbPrefilter(filter, column);
        if (clause == null)
            return false;
        if ("FALSE".equals(clause.sql())) {
            sql.append(" AND FALSE");
            return true;
        }
        if (clause.sql().contains("json_")) {
            ExactDuckDbPredicate fastGuard = buildFastStrposGuard(filter, column);
            if (fastGuard != null && !"TRUE".equals(fastGuard.sql()) && !"FALSE".equals(fastGuard.sql())) {
                sql.append(" AND (CASE WHEN ((").append(fastGuard.sql())
                        .append(") OR ").append(nonStandardJsonFormattingEscape(column))
                        .append(") THEN ((").append(clause.sql())
                        .append(") OR strpos(").append(column).append(", '\\u') > 0) ELSE FALSE END)");
                parameters.addAll(fastGuard.parameters());
                parameters.addAll(clause.parameters());
                return true;
            }
        }
        sql.append(" AND ((").append(clause.sql())
                .append(") OR strpos(").append(column).append(", '\\u') > 0)");
        parameters.addAll(clause.parameters());
        return true;
    }

    public static Optional<OcrGenerationMatcher> extractOcrGenerationMatcher(Filter filter) {
        if (filter == null || filter == Filter.INCLUDE || filter == Filter.EXCLUDE)
            return Optional.empty();
        if (filter instanceof PropertyIsEqualTo eq &&
                eq.getExpression1() instanceof PropertyName prop &&
                "normalizedValue".equals(prop.getPropertyName()) &&
                eq.getExpression2() instanceof Literal lit &&
                lit.getValue() instanceof String expected) {
            boolean matchCase = eq.isMatchingCase();
            return Optional.of((bloom, min, max) ->
                    OcrBloomFilter.mightMatchExact(bloom, min, max, expected, matchCase));
        }
        if (filter instanceof PropertyIsLike like &&
                like.getExpression() instanceof PropertyName prop &&
                "normalizedValue".equals(prop.getPropertyName())) {
            String literal = like.getLiteral();
            String wildcard = like.getWildCard();
            String single = like.getSingleChar();
            String escape = like.getEscape();
            boolean matchCase = like.isMatchingCase();
            return Optional.of((bloom, min, max) ->
                    OcrBloomFilter.mightMatchLike(bloom, min, max, literal, wildcard, single, escape, matchCase));
        }
        if (filter instanceof And and) {
            List<OcrGenerationMatcher> matchers = new ArrayList<>();
            for (Filter child : and.getChildren()) {
                extractOcrGenerationMatcher(child).ifPresent(matchers::add);
            }
            if (matchers.isEmpty())
                return Optional.empty();
            return Optional.of((bloom, min, max) -> {
                for (OcrGenerationMatcher m : matchers) {
                    if (!m.mightMatch(bloom, min, max))
                        return false;
                }
                return true;
            });
        }
        if (filter instanceof Or or) {
            if (or.getChildren().isEmpty())
                return Optional.empty();
            List<OcrGenerationMatcher> matchers = new ArrayList<>(or.getChildren().size());
            for (Filter child : or.getChildren()) {
                Optional<OcrGenerationMatcher> sub = extractOcrGenerationMatcher(child);
                if (sub.isEmpty())
                    return Optional.empty();
                matchers.add(sub.get());
            }
            return Optional.of((bloom, min, max) -> {
                for (OcrGenerationMatcher m : matchers) {
                    if (m.mightMatch(bloom, min, max))
                        return true;
                }
                return false;
            });
        }
        return Optional.empty();
    }

    /**
     * Reject only explicit nonzero numeric adjudication counts and explicit
     * opposite boolean alarm flags. Arrays, missing fields, nulls and other
     * types remain candidates for exact Java evaluation. The caller supplies a
     * trusted PostgreSQL jsonb column expression; never emit bare {@code ?}
     * operators here because JDBC {@code PreparedStatement} parses {@code ?} as
     * a positional bind parameter.
     */
    public static boolean appendPostgresOverlayPrefilter(
            Filter filter, String column, StringBuilder sql) {
        if (filter == null)
            return false;
        if (filter == Filter.EXCLUDE) {
            sql.append(" AND FALSE");
            return true;
        }
        String clause = buildPostgresOverlayPrefilter(filter, column);
        if (clause == null)
            return false;
        sql.append(" AND (").append(clause).append(')');
        return true;
    }

    private static String buildPostgresOverlayPrefilter(Filter filter, String column) {
        if (filter == null || filter == Filter.INCLUDE)
            return null;
        if (filter == Filter.EXCLUDE)
            return "FALSE";
        if (filter instanceof And and) {
            List<String> clauses = new ArrayList<>();
            for (Filter child : and.getChildren()) {
                String sub = buildPostgresOverlayPrefilter(child, column);
                if ("FALSE".equals(sub))
                    return "FALSE";
                if (sub != null)
                    clauses.add("(" + sub + ")");
            }
            return clauses.isEmpty() ? null : String.join(" AND ", clauses);
        }
        if (filter instanceof Or or) {
            if (or.getChildren().isEmpty())
                return null;
            List<String> clauses = new ArrayList<>();
            for (Filter child : or.getChildren()) {
                String sub = buildPostgresOverlayPrefilter(child, column);
                if (sub == null)
                    return null;
                if (!"FALSE".equals(sub))
                    clauses.add("(" + sub + ")");
            }
            return clauses.isEmpty() ? "FALSE" : String.join(" OR ", clauses);
        }
        if (filter instanceof PropertyIsEqualTo eq &&
                eq.getExpression1() instanceof PropertyName property &&
                eq.getExpression2() instanceof Literal literal) {
            String path = property.getPropertyName();
            Object expected = literal.getValue();
            if ("adjudicatedIdsCount".equals(path) && isExactNumericZero(expected)) {
                return "NOT COALESCE((jsonb_typeof(" + column +
                        "->'adjudicatedIdsCount') = 'number' AND " +
                        column + "->'adjudicatedIdsCount' <> '0'::jsonb), false)";
            }
            if (("gammaAlarm".equals(path) || "neutronAlarm".equals(path)) &&
                    expected instanceof Boolean boolVal) {
                String expectedJson = boolVal ? "'true'::jsonb" : "'false'::jsonb";
                return "NOT COALESCE((jsonb_typeof(" + column + "->'" + path +
                        "') = 'boolean' AND " + column + "->'" + path +
                        "' <> " + expectedJson + "), false)";
            }
        }
        return null;
    }

    private static boolean isExactNumericZero(Object value) {
        if (!(value instanceof Number))
            return false;
        try {
            return new BigDecimal(value.toString()).compareTo(BigDecimal.ZERO) == 0;
        } catch (NumberFormatException ignored) {
            return false;
        }
    }

    public static boolean hasSelectiveDuckDbPrefilter(Filter filter) {
        PrefilterClause clause = buildDuckDbPrefilter(filter, "result_json");
        return clause != null && clause.selective();
    }

    public record SummaryCounterSpec(String upperBoundExpr, String exactExpr, String exactReadyExpr) {}

    public record TimeContainedSummarySpec(
            String upperBoundExpr,
            String exactExpr,
            String exactReadyExpr,
            Instant minContainedWindowStart,
            Instant maxContainedWindowEnd,
            Set<String> requiredTimeFields) {}

    public record CqlTimeBounds(Instant minPhenomenonTime, Instant maxPhenomenonTime) {}

    /**
     * Maps a CQL filter (or {@code null}/{@link Filter#INCLUDE}) to an exact SQL
     * counter expression over {@code oscar_archive_schema} alias {@code s} when
     * the predicate is covered by per-generation summary counters.
     */
    public static Optional<String> summaryCounterExpression(Filter filter) {
        return summaryCounterSpec(filter).map(SummaryCounterSpec::upperBoundExpr);
    }

    private static final int STATUS_MASK_GAMMA_NEUTRON = 1 << 0;
    private static final int STATUS_MASK_GAMMA_ONLY = 1 << 1;
    private static final int STATUS_MASK_NEUTRON_ONLY = 1 << 2;
    private static final int STATUS_MASK_NON_ALARM = 1 << 3;
    private static final int STATUS_MASK_ANY_ALARM =
            STATUS_MASK_GAMMA_NEUTRON | STATUS_MASK_GAMMA_ONLY | STATUS_MASK_NEUTRON_ONLY;

    public static Optional<SummaryCounterSpec> summaryCounterSpec(Filter filter) {
        if (filter == null || filter == Filter.INCLUDE) {
            String expr = "COALESCE(s.row_count, 0)";
            return Optional.of(new SummaryCounterSpec(expr, expr, "s.summary_populated"));
        }
        if (filter == Filter.EXCLUDE) {
            return Optional.of(new SummaryCounterSpec("0::bigint", "0::bigint", "true"));
        }
        if (filter instanceof PropertyIsEqualTo eq) {
            if (eq.getExpression1() instanceof PropertyName prop &&
                    eq.getExpression2() instanceof Literal lit &&
                    "adjudicatedIdsCount".equals(prop.getPropertyName()) &&
                    isZeroNumber(lit.getValue())) {
                return resolveBaseAndSummarySpec(null, null, false, Boolean.FALSE);
            }
            return singleEqualitySummarySpec(eq);
        }
        if (filter instanceof PropertyIsGreaterThan gt &&
                gt.getExpression1() instanceof PropertyName prop &&
                gt.getExpression2() instanceof Literal lit &&
                "adjudicatedIdsCount".equals(prop.getPropertyName()) &&
                isZeroNumber(lit.getValue())) {
            String expr = "COALESCE(s.adjudicated_alarm_count, 0)";
            return Optional.of(new SummaryCounterSpec(expr, expr, "s.summary_populated"));
        }
        if (filter instanceof PropertyIsLike like) {
            if (!like.isMatchingCase() ||
                    !"%".equals(like.getWildCard()) ||
                    !"_".equals(like.getSingleChar()) ||
                    !"\\".equals(like.getEscape()) ||
                    !(like.getExpression() instanceof PropertyName prop))
                return Optional.empty();
            if ("derivedData.alarmCategory".equals(prop.getPropertyName())) {
                if ("%Gamma%".equals(like.getLiteral())) {
                    String expr = "COALESCE(s.rs350_gamma_alarm_count, 0)";
                    return Optional.of(new SummaryCounterSpec(expr, expr, "s.summary_populated"));
                }
                if ("%Neutron%".equals(like.getLiteral())) {
                    String expr = "COALESCE(s.rs350_neutron_alarm_count, 0)";
                    return Optional.of(new SummaryCounterSpec(expr, expr, "s.summary_populated"));
                }
            }
            return Optional.empty();
        }
        if (filter instanceof And and) {
            return andSummaryCounterSpec(and);
        }
        if (filter instanceof Or or) {
            if (isAnyAlarmOr(or)) {
                String expr = "COALESCE(s.any_alarm_count, 0)";
                return Optional.of(new SummaryCounterSpec(expr, expr, "s.summary_populated"));
            }
            Integer statusMask = extractDisjointStatusMask(or);
            if (statusMask != null) {
                return resolveDisjointStatusMaskSpec(statusMask, null);
            }
            Optional<SummaryCounterSpec> disjointStringOr = disjointStringEqualityOrSummarySpec(or);
            if (disjointStringOr.isPresent()) {
                return disjointStringOr;
            }
            if (!or.getChildren().isEmpty()) {
                List<String> childUpperExprs = new ArrayList<>(or.getChildren().size());
                boolean allChildrenBounded = true;
                for (Filter child : or.getChildren()) {
                    Optional<SummaryCounterSpec> childSpec = summaryCounterSpec(child);
                    if (childSpec.isEmpty() || "COALESCE(s.row_count, 0)".equals(childSpec.get().upperBoundExpr())) {
                        allChildrenBounded = false;
                        break;
                    }
                    if (!"0::bigint".equals(childSpec.get().upperBoundExpr())) {
                        childUpperExprs.add(childSpec.get().upperBoundExpr());
                    }
                }
                if (allChildrenBounded) {
                    if (childUpperExprs.isEmpty()) {
                        return Optional.of(new SummaryCounterSpec("0::bigint", "0::bigint", "true"));
                    }
                    String sumUpper = childUpperExprs.size() == 1
                            ? childUpperExprs.get(0)
                            : "(" + String.join(" + ", childUpperExprs) + ")";
                    return Optional.of(new SummaryCounterSpec(sumUpper, "0::bigint", "false"));
                }
            }
        }
        return Optional.empty();
    }

    private static void flattenAndChildren(Filter filter, List<Filter> out) {
        if (filter instanceof And and) {
            for (Filter child : and.getChildren())
                flattenAndChildren(child, out);
        } else {
            out.add(filter);
        }
    }

    private static boolean isAnyAlarmOr(Or or) {
        if (or.getChildren().size() != 2)
            return false;
        boolean hasGammaTrue = false;
        boolean hasNeutronTrue = false;
        for (Filter child : or.getChildren()) {
            if (child instanceof PropertyIsEqualTo eq &&
                    eq.getExpression1() instanceof PropertyName prop &&
                    eq.getExpression2() instanceof Literal lit &&
                    Boolean.TRUE.equals(lit.getValue())) {
                if ("gammaAlarm".equals(prop.getPropertyName()))
                    hasGammaTrue = true;
                else if ("neutronAlarm".equals(prop.getPropertyName()))
                    hasNeutronTrue = true;
            }
        }
        return hasGammaTrue && hasNeutronTrue;
    }

    private static Integer extractDisjointStatusMask(Or or) {
        List<Filter> disjuncts = new ArrayList<>();
        flattenOrChildren(or, disjuncts);
        if (disjuncts.isEmpty())
            return null;
        int mask = 0;
        for (Filter disjunct : disjuncts) {
            Integer pairMask = extractSingleStatusPairMask(disjunct);
            if (pairMask == null)
                return null;
            mask |= pairMask;
        }
        return mask;
    }

    private static void flattenOrChildren(Filter filter, List<Filter> out) {
        if (filter instanceof Or or) {
            for (Filter child : or.getChildren())
                flattenOrChildren(child, out);
        } else {
            out.add(filter);
        }
    }

    private static Integer extractSingleStatusPairMask(Filter filter) {
        if (!(filter instanceof And and))
            return null;
        List<Filter> parts = new ArrayList<>();
        flattenAndChildren(and, parts);
        if (parts.isEmpty())
            return null;
        Boolean gamma = null;
        Boolean neutron = null;
        for (Filter part : parts) {
            if (!(part instanceof PropertyIsEqualTo eq) ||
                    !(eq.getExpression1() instanceof PropertyName prop) ||
                    !(eq.getExpression2() instanceof Literal lit) ||
                    !(lit.getValue() instanceof Boolean boolVal))
                return null;
            String name = prop.getPropertyName();
            if ("gammaAlarm".equals(name)) {
                if (gamma != null && !gamma.equals(boolVal))
                    return 0;
                gamma = boolVal;
            } else if ("neutronAlarm".equals(name)) {
                if (neutron != null && !neutron.equals(boolVal))
                    return 0;
                neutron = boolVal;
            } else {
                return null;
            }
        }
        if (gamma == null || neutron == null)
            return null;
        if (gamma && neutron)
            return STATUS_MASK_GAMMA_NEUTRON;
        if (gamma)
            return STATUS_MASK_GAMMA_ONLY;
        if (neutron)
            return STATUS_MASK_NEUTRON_ONLY;
        return STATUS_MASK_NON_ALARM;
    }

    private static Optional<SummaryCounterSpec> resolveDisjointStatusMaskSpec(
            int statusMask, Boolean adjudicated) {
        if (statusMask == 0)
            return Optional.of(new SummaryCounterSpec("0::bigint", "0::bigint", "true"));
        List<SummaryCounterSpec> terms = new ArrayList<>(4);
        if ((statusMask & STATUS_MASK_GAMMA_NEUTRON) != 0)
            resolveBaseAndSummarySpec(Boolean.TRUE, Boolean.TRUE, false, adjudicated).ifPresent(terms::add);
        if ((statusMask & STATUS_MASK_GAMMA_ONLY) != 0)
            resolveBaseAndSummarySpec(Boolean.TRUE, Boolean.FALSE, false, adjudicated).ifPresent(terms::add);
        if ((statusMask & STATUS_MASK_NEUTRON_ONLY) != 0)
            resolveBaseAndSummarySpec(Boolean.FALSE, Boolean.TRUE, false, adjudicated).ifPresent(terms::add);
        if ((statusMask & STATUS_MASK_NON_ALARM) != 0)
            resolveBaseAndSummarySpec(Boolean.FALSE, Boolean.FALSE, false, adjudicated).ifPresent(terms::add);

        List<SummaryCounterSpec> nonZero = terms.stream()
                .filter(spec -> !"0::bigint".equals(spec.upperBoundExpr()))
                .toList();
        if (nonZero.isEmpty())
            return Optional.of(new SummaryCounterSpec("0::bigint", "0::bigint", "true"));
        if (nonZero.size() == 1)
            return Optional.of(nonZero.get(0));

        List<String> uppers = new ArrayList<>(nonZero.size());
        List<String> exacts = new ArrayList<>(nonZero.size());
        Set<String> readies = new LinkedHashSet<>();
        for (SummaryCounterSpec spec : nonZero) {
            uppers.add(spec.upperBoundExpr());
            exacts.add(spec.exactExpr());
            if ("false".equals(spec.exactReadyExpr())) {
                readies.clear();
                readies.add("false");
            } else if (!readies.contains("false") && !"true".equals(spec.exactReadyExpr())) {
                readies.add(spec.exactReadyExpr());
            }
        }
        String combinedReady;
        if (readies.isEmpty())
            combinedReady = "true";
        else if (readies.size() == 1)
            combinedReady = readies.iterator().next();
        else
            combinedReady = "(" + String.join(" AND ", readies) + ")";

        return Optional.of(new SummaryCounterSpec(
                "(" + String.join(" + ", uppers) + ")",
                "(" + String.join(" + ", exacts) + ")",
                combinedReady));
    }

    private static int constrainDisjointStatusMask(
            int statusMask, Boolean gamma, Boolean neutron, boolean requireAnyAlarm) {
        int masked = statusMask;
        if (gamma != null) {
            masked &= gamma
                    ? (STATUS_MASK_GAMMA_NEUTRON | STATUS_MASK_GAMMA_ONLY)
                    : (STATUS_MASK_NEUTRON_ONLY | STATUS_MASK_NON_ALARM);
        }
        if (neutron != null) {
            masked &= neutron
                    ? (STATUS_MASK_GAMMA_NEUTRON | STATUS_MASK_NEUTRON_ONLY)
                    : (STATUS_MASK_GAMMA_ONLY | STATUS_MASK_NON_ALARM);
        }
        if (requireAnyAlarm)
            masked &= STATUS_MASK_ANY_ALARM;
        return masked;
    }

    private static Optional<SummaryCounterSpec> andSummaryCounterSpec(And rootAnd) {
        List<Filter> conjuncts = new ArrayList<>();
        flattenAndChildren(rootAnd, conjuncts);
        if (conjuncts.isEmpty())
            return Optional.empty();

        Boolean gamma = null;
        Boolean neutron = null;
        boolean requireAnyAlarm = false;
        Boolean adjudicated = null; // false = unadjudicated (count=0), true = adjudicated (count>0)
        Integer disjointStatusMask = null;
        boolean hasExtraConjuncts = false;

        for (Filter child : conjuncts) {
            if (child instanceof Or or) {
                if (isAnyAlarmOr(or)) {
                    requireAnyAlarm = true;
                    continue;
                }
                Integer orMask = extractDisjointStatusMask(or);
                if (orMask != null) {
                    disjointStatusMask = disjointStatusMask == null ? orMask : (disjointStatusMask & orMask);
                    continue;
                }
            }
            if (child instanceof PropertyIsEqualTo eq &&
                    eq.getExpression1() instanceof PropertyName prop &&
                    eq.getExpression2() instanceof Literal lit &&
                    lit.getValue() != null) {
                String name = prop.getPropertyName();
                Object val = lit.getValue();
                if (val instanceof Boolean boolVal) {
                    if ("gammaAlarm".equals(name)) {
                        if (gamma != null && !gamma.equals(boolVal))
                            return Optional.of(new SummaryCounterSpec("0::bigint", "0::bigint", "true"));
                        gamma = boolVal;
                        continue;
                    }
                    if ("neutronAlarm".equals(name)) {
                        if (neutron != null && !neutron.equals(boolVal))
                            return Optional.of(new SummaryCounterSpec("0::bigint", "0::bigint", "true"));
                        neutron = boolVal;
                        continue;
                    }
                }
                if ("adjudicatedIdsCount".equals(name) && isZeroNumber(val)) {
                    if (adjudicated != null && adjudicated)
                        return Optional.of(new SummaryCounterSpec("0::bigint", "0::bigint", "true"));
                    adjudicated = Boolean.FALSE;
                    continue;
                }
                hasExtraConjuncts = true;
                continue;
            }
            if (child instanceof PropertyIsGreaterThan gt &&
                    gt.getExpression1() instanceof PropertyName prop &&
                    gt.getExpression2() instanceof Literal lit &&
                    "adjudicatedIdsCount".equals(prop.getPropertyName()) &&
                    isZeroNumber(lit.getValue())) {
                if (adjudicated != null && !adjudicated)
                    return Optional.of(new SummaryCounterSpec("0::bigint", "0::bigint", "true"));
                adjudicated = Boolean.TRUE;
                continue;
            }
            hasExtraConjuncts = true;
        }

        if (requireAnyAlarm && Boolean.FALSE.equals(gamma) && Boolean.FALSE.equals(neutron)) {
            return Optional.of(new SummaryCounterSpec("0::bigint", "0::bigint", "true"));
        }

        Optional<SummaryCounterSpec> baseSpec = disjointStatusMask != null
                ? resolveDisjointStatusMaskSpec(
                        constrainDisjointStatusMask(disjointStatusMask, gamma, neutron, requireAnyAlarm),
                        adjudicated)
                : resolveBaseAndSummarySpec(gamma, neutron, requireAnyAlarm, adjudicated);
        if (baseSpec.isEmpty())
            return Optional.empty();
        SummaryCounterSpec spec = baseSpec.get();
        if (hasExtraConjuncts && !"0::bigint".equals(spec.upperBoundExpr())) {
            return Optional.of(new SummaryCounterSpec(spec.upperBoundExpr(), "0::bigint", "false"));
        }
        return Optional.of(spec);
    }

    /**
     * Identifies CQL filters consisting strictly of an exact summary-counter
     * base predicate conjoined with one or more RFC-3339 range comparisons
     * ({@code <}, {@code <=}, {@code >}, {@code >=}) on top-level
     * {@code startTime}, {@code endTime}, or {@code samplingTime}.
     * For archive generations whose schema declares those SWE {@code Time}
     * fields and whose window lies strictly inside the conservative 24-hour
     * interior region, the time comparisons are tautologically true and the
     * generation's summary counter is exact.
     */
    public static Optional<TimeContainedSummarySpec> timeContainedSummarySpec(Filter filter) {
        if (filter == null || filter == Filter.INCLUDE || filter == Filter.EXCLUDE)
            return Optional.empty();
        List<Filter> conjuncts = new ArrayList<>();
        flattenAndChildren(filter, conjuncts);
        if (conjuncts.isEmpty())
            return Optional.empty();

        final java.time.Duration safetyMargin = java.time.Duration.ofHours(24);
        Instant minContainedStart = null;
        Instant maxContainedEnd = null;
        Set<String> requiredTimeFields = new LinkedHashSet<>();
        List<Filter> nonTimeConjuncts = new ArrayList<>();

        for (Filter child : conjuncts) {
            if ((child instanceof PropertyIsLessThan ||
                    child instanceof PropertyIsLessThanOrEqualTo ||
                    child instanceof PropertyIsGreaterThan ||
                    child instanceof PropertyIsGreaterThanOrEqualTo) &&
                    ((BinaryComparisonOperator) child).getExpression1() instanceof PropertyName prop &&
                    ((BinaryComparisonOperator) child).getExpression2() instanceof Literal lit &&
                    lit.getValue() instanceof String text) {
                String name = prop.getPropertyName();
                if ("startTime".equals(name) || "endTime".equals(name) || "samplingTime".equals(name)) {
                    Instant parsed = parseRfc3339Instant(text.trim());
                    if (parsed == null)
                        return Optional.empty();
                    requiredTimeFields.add(name);
                    if (child instanceof PropertyIsLessThan || child instanceof PropertyIsLessThanOrEqualTo) {
                        Instant safeEnd = parsed.minus(safetyMargin);
                        maxContainedEnd = maxContainedEnd == null || safeEnd.isBefore(maxContainedEnd)
                                ? safeEnd : maxContainedEnd;
                    } else {
                        Instant safeStart;
                        if ("endTime".equals(name)) {
                            // Occupancy records always satisfy endTime >= phenomenonTime across
                            // Rapiscan, Aspect, and RS350 streams, so any generation whose windowStart
                            // lies after the cutoff (with 1 ms tolerance for millisecond ISO rendering)
                            // tautologically satisfies the endTime lower bound for every row.
                            if (child instanceof PropertyIsGreaterThan) {
                                safeStart = parsed.plusMillis(1L);
                            } else {
                                safeStart = parsed.getNano() % 1_000_000 == 0
                                        ? parsed
                                        : parsed.plusMillis(1L);
                            }
                        } else {
                            safeStart = parsed.plus(safetyMargin);
                        }
                        minContainedStart = minContainedStart == null || safeStart.isAfter(minContainedStart)
                                ? safeStart : minContainedStart;
                    }
                    continue;
                }
            }
            nonTimeConjuncts.add(child);
        }

        if (requiredTimeFields.isEmpty())
            return Optional.empty();
        if (minContainedStart != null && maxContainedEnd != null && !minContainedStart.isBefore(maxContainedEnd))
            return Optional.empty();

        SummaryCounterSpec baseSpec;
        if (nonTimeConjuncts.isEmpty()) {
            String expr = "COALESCE(s.row_count, 0)";
            baseSpec = new SummaryCounterSpec(expr, expr, "s.summary_populated");
        } else if (nonTimeConjuncts.size() == 1) {
            Optional<SummaryCounterSpec> single = summaryCounterSpec(nonTimeConjuncts.get(0));
            if (single.isEmpty())
                return Optional.empty();
            baseSpec = single.get();
        } else {
            Optional<SummaryCounterSpec> combined = summaryCounterSpecForConjuncts(nonTimeConjuncts);
            if (combined.isEmpty())
                return Optional.empty();
            baseSpec = combined.get();
        }

        if ("false".equals(baseSpec.exactReadyExpr()) || "0::bigint".equals(baseSpec.upperBoundExpr()))
            return Optional.empty();

        return Optional.of(new TimeContainedSummarySpec(
                baseSpec.upperBoundExpr(),
                baseSpec.exactExpr(),
                baseSpec.exactReadyExpr(),
                minContainedStart,
                maxContainedEnd,
                Collections.unmodifiableSet(requiredTimeFields)));
    }

    private static Optional<SummaryCounterSpec> summaryCounterSpecForConjuncts(List<Filter> conjuncts) {
        Boolean gamma = null;
        Boolean neutron = null;
        boolean requireAnyAlarm = false;
        Boolean adjudicated = null;
        Integer disjointStatusMask = null;
        for (Filter child : conjuncts) {
            if (child instanceof Or or) {
                if (isAnyAlarmOr(or)) {
                    requireAnyAlarm = true;
                    continue;
                }
                Integer orMask = extractDisjointStatusMask(or);
                if (orMask != null) {
                    disjointStatusMask = disjointStatusMask == null ? orMask : (disjointStatusMask & orMask);
                    continue;
                }
            }
            if (child instanceof PropertyIsEqualTo eq &&
                    eq.getExpression1() instanceof PropertyName prop &&
                    eq.getExpression2() instanceof Literal lit &&
                    lit.getValue() != null) {
                String name = prop.getPropertyName();
                Object val = lit.getValue();
                if (val instanceof Boolean boolVal) {
                    if ("gammaAlarm".equals(name)) {
                        if (gamma != null && !gamma.equals(boolVal))
                            return Optional.of(new SummaryCounterSpec("0::bigint", "0::bigint", "true"));
                        gamma = boolVal;
                        continue;
                    }
                    if ("neutronAlarm".equals(name)) {
                        if (neutron != null && !neutron.equals(boolVal))
                            return Optional.of(new SummaryCounterSpec("0::bigint", "0::bigint", "true"));
                        neutron = boolVal;
                        continue;
                    }
                }
                if ("adjudicatedIdsCount".equals(name) && isZeroNumber(val)) {
                    if (adjudicated != null && adjudicated)
                        return Optional.of(new SummaryCounterSpec("0::bigint", "0::bigint", "true"));
                    adjudicated = Boolean.FALSE;
                    continue;
                }
                return Optional.empty();
            }
            if (child instanceof PropertyIsGreaterThan gt &&
                    gt.getExpression1() instanceof PropertyName prop &&
                    gt.getExpression2() instanceof Literal lit &&
                    "adjudicatedIdsCount".equals(prop.getPropertyName()) &&
                    isZeroNumber(lit.getValue())) {
                if (adjudicated != null && !adjudicated)
                    return Optional.of(new SummaryCounterSpec("0::bigint", "0::bigint", "true"));
                adjudicated = Boolean.TRUE;
                continue;
            }
            return Optional.empty();
        }
        if (requireAnyAlarm && Boolean.FALSE.equals(gamma) && Boolean.FALSE.equals(neutron))
            return Optional.of(new SummaryCounterSpec("0::bigint", "0::bigint", "true"));
        if (disjointStatusMask != null) {
            return resolveDisjointStatusMaskSpec(
                    constrainDisjointStatusMask(disjointStatusMask, gamma, neutron, requireAnyAlarm),
                    adjudicated);
        }
        return resolveBaseAndSummarySpec(gamma, neutron, requireAnyAlarm, adjudicated);
    }

    private static Optional<SummaryCounterSpec> resolveBaseAndSummarySpec(
            Boolean gamma, Boolean neutron, boolean requireAnyAlarm, Boolean adjudicated) {
        if (adjudicated == null) {
            if (gamma != null && neutron != null) {
                if (gamma && !neutron) {
                    String expr = "COALESCE(s.gamma_alarm_count, 0)";
                    return Optional.of(new SummaryCounterSpec(expr, expr, "s.summary_populated"));
                }
                if (!gamma && neutron) {
                    String expr = "COALESCE(s.neutron_alarm_count, 0)";
                    return Optional.of(new SummaryCounterSpec(expr, expr, "s.summary_populated"));
                }
                if (gamma) {
                    String expr = "COALESCE(s.gamma_neutron_alarm_count, 0)";
                    return Optional.of(new SummaryCounterSpec(expr, expr, "s.summary_populated"));
                }
                String expr = "COALESCE(s.non_alarm_occupancy_count, 0)";
                return Optional.of(new SummaryCounterSpec(expr, expr, "s.summary_populated"));
            }
            if (requireAnyAlarm && gamma == null && neutron == null) {
                String expr = "COALESCE(s.any_alarm_count, 0)";
                return Optional.of(new SummaryCounterSpec(expr, expr, "s.summary_populated"));
            }
            if (Boolean.TRUE.equals(gamma) && neutron == null) {
                String expr = "COALESCE(s.gamma_true_count, 0)";
                return Optional.of(new SummaryCounterSpec(expr, expr, "s.summary_populated"));
            }
            if (Boolean.TRUE.equals(neutron) && gamma == null) {
                String expr = "COALESCE(s.neutron_true_count, 0)";
                return Optional.of(new SummaryCounterSpec(expr, expr, "s.summary_populated"));
            }
            return Optional.empty();
        }

        boolean wantAdj = adjudicated;
        String adjPrefix = wantAdj ? "adjudicated" : "unadjudicated";
        String totalAdjCol = "s." + adjPrefix + "_alarm_count";

        if (gamma != null && neutron != null) {
            if (!gamma && !neutron) {
                if (wantAdj) {
                    return Optional.of(new SummaryCounterSpec("0::bigint", "0::bigint", "true"));
                }
                String expr = "COALESCE(s.non_alarm_occupancy_count, 0)";
                return Optional.of(new SummaryCounterSpec(expr, expr, "s.summary_populated"));
            }
            String catCol;
            String splitCol;
            if (gamma && !neutron) {
                catCol = "s.gamma_alarm_count";
                splitCol = "s." + adjPrefix + "_gamma_alarm_count";
            } else if (!gamma && neutron) {
                catCol = "s.neutron_alarm_count";
                splitCol = "s." + adjPrefix + "_neutron_alarm_count";
            } else {
                catCol = "s.gamma_neutron_alarm_count";
                splitCol = "s." + adjPrefix + "_gamma_neutron_alarm_count";
            }
            String upperBound = "COALESCE(" + splitCol + ", LEAST(COALESCE(" + totalAdjCol + ", 0), COALESCE(" + catCol + ", 0)))";
            String exact = "COALESCE(" + splitCol + ", 0)";
            String ready = "(s.summary_populated AND " + splitCol + " IS NOT NULL)";
            return Optional.of(new SummaryCounterSpec(upperBound, exact, ready));
        }

        if ((requireAnyAlarm || wantAdj) && gamma == null && neutron == null) {
            String expr = "COALESCE(" + totalAdjCol + ", 0)";
            return Optional.of(new SummaryCounterSpec(expr, expr, "s.summary_populated"));
        }

        if (!wantAdj && !requireAnyAlarm && gamma == null && neutron == null) {
            String occComplete = "(COALESCE(s.any_alarm_count, 0) + COALESCE(s.non_alarm_occupancy_count, 0) = COALESCE(s.row_count, 0))";
            String occExact = "(COALESCE(s.unadjudicated_alarm_count, 0) + COALESCE(s.non_alarm_occupancy_count, 0))";
            String upperBound = "(CASE WHEN " + occComplete + " THEN " + occExact +
                    " ELSE GREATEST(COALESCE(s.row_count, 0) - COALESCE(s.adjudicated_alarm_count, 0), 0) END)";
            String ready = "(s.summary_populated AND " + occComplete + ")";
            return Optional.of(new SummaryCounterSpec(upperBound, occExact, ready));
        }

        if (Boolean.TRUE.equals(gamma) && neutron == null) {
            String splitCol1 = "s." + adjPrefix + "_gamma_alarm_count";
            String splitCol2 = "s." + adjPrefix + "_gamma_neutron_alarm_count";
            String upperBound = "COALESCE(" + splitCol1 + " + " + splitCol2 +
                    ", LEAST(COALESCE(" + totalAdjCol + ", 0), COALESCE(s.gamma_true_count, 0)))";
            String exact = "(COALESCE(" + splitCol1 + ", 0) + COALESCE(" + splitCol2 + ", 0))";
            String ready = "(s.summary_populated AND " + splitCol1 + " IS NOT NULL AND " + splitCol2 + " IS NOT NULL)";
            return Optional.of(new SummaryCounterSpec(upperBound, exact, ready));
        }

        if (Boolean.TRUE.equals(neutron) && gamma == null) {
            String splitCol1 = "s." + adjPrefix + "_neutron_alarm_count";
            String splitCol2 = "s." + adjPrefix + "_gamma_neutron_alarm_count";
            String upperBound = "COALESCE(" + splitCol1 + " + " + splitCol2 +
                    ", LEAST(COALESCE(" + totalAdjCol + ", 0), COALESCE(s.neutron_true_count, 0)))";
            String exact = "(COALESCE(" + splitCol1 + ", 0) + COALESCE(" + splitCol2 + ", 0))";
            String ready = "(s.summary_populated AND " + splitCol1 + " IS NOT NULL AND " + splitCol2 + " IS NOT NULL)";
            return Optional.of(new SummaryCounterSpec(upperBound, exact, ready));
        }

        return Optional.empty();
    }

    private static boolean isZeroNumber(Object value) {
        if (value instanceof Number num)
            return num.longValue() == 0L && Double.compare(num.doubleValue(), 0.0) == 0;
        if (value instanceof String str) {
            try {
                return new BigDecimal(str.trim()).compareTo(BigDecimal.ZERO) == 0;
            } catch (NumberFormatException ignored) {
                return false;
            }
        }
        return false;
    }

    /**
     * Extracts conservative phenomenon-time bounds implied by top-level
     * {@code startTime} / {@code endTime} RFC-3339 comparisons in a CQL filter.
     * Occupancy observations always have {@code phenomenonTime == endTime >= startTime},
     * so applying a 24-hour buffer around those bounds safely prunes unrelated
     * historical archive windows (such as during live event-ID resolution).
     */
    public static Optional<CqlTimeBounds> extractCqlPhenomenonTimeBounds(Filter filter) {
        if (filter == null || filter == Filter.INCLUDE || filter == Filter.EXCLUDE)
            return Optional.empty();
        List<Filter> conjuncts = new ArrayList<>();
        flattenAndChildren(filter, conjuncts);
        Instant minTime = null;
        Instant maxTime = null;
        final java.time.Duration safetyMargin = java.time.Duration.ofHours(24);
        for (Filter child : conjuncts) {
            if (!(child instanceof BinaryComparisonOperator cmp) ||
                    !(cmp.getExpression1() instanceof PropertyName prop) ||
                    !(cmp.getExpression2() instanceof Literal lit) ||
                    !(lit.getValue() instanceof String text))
                continue;
            String name = prop.getPropertyName();
            if (!"startTime".equals(name) && !"endTime".equals(name) && !"samplingTime".equals(name))
                continue;
            Instant parsed = parseRfc3339Instant(text.trim());
            if (parsed == null)
                continue;
            if (cmp instanceof PropertyIsEqualTo) {
                Instant low = "startTime".equals(name)
                        ? parsed.minus(java.time.Duration.ofHours(1))
                        : parsed.minus(safetyMargin);
                Instant high = "endTime".equals(name)
                        ? parsed.plus(java.time.Duration.ofHours(1))
                        : parsed.plus(safetyMargin);
                minTime = minTime == null || low.isAfter(minTime) ? low : minTime;
                maxTime = maxTime == null || high.isBefore(maxTime) ? high : maxTime;
            } else if (cmp instanceof PropertyIsGreaterThan || cmp instanceof PropertyIsGreaterThanOrEqualTo) {
                Instant low = parsed.minus(safetyMargin);
                minTime = minTime == null || low.isAfter(minTime) ? low : minTime;
            } else if (cmp instanceof PropertyIsLessThan || cmp instanceof PropertyIsLessThanOrEqualTo) {
                Instant high = parsed.plus(safetyMargin);
                maxTime = maxTime == null || high.isBefore(maxTime) ? high : maxTime;
            }
        }
        if (minTime == null && maxTime == null)
            return Optional.empty();
        return Optional.of(new CqlTimeBounds(minTime, maxTime));
    }

    private static Optional<SummaryCounterSpec> singleEqualitySummarySpec(PropertyIsEqualTo eq) {
        if (!(eq.getExpression1() instanceof PropertyName prop) ||
                !(eq.getExpression2() instanceof Literal lit) ||
                lit.getValue() == null)
            return Optional.empty();
        String name = prop.getPropertyName();
        Object val = lit.getValue();
        if (val instanceof Boolean boolVal) {
            if ("gammaAlarm".equals(name) && boolVal) {
                String expr = "COALESCE(s.gamma_true_count, 0)";
                return Optional.of(new SummaryCounterSpec(expr, expr, "s.summary_populated"));
            }
            if ("neutronAlarm".equals(name) && boolVal) {
                String expr = "COALESCE(s.neutron_true_count, 0)";
                return Optional.of(new SummaryCounterSpec(expr, expr, "s.summary_populated"));
            }
            if ("tamperStatus".equals(name) && boolVal) {
                String expr = "COALESCE(s.tamper_true_count, 0)";
                return Optional.of(new SummaryCounterSpec(expr, expr, "s.summary_populated"));
            }
            return Optional.empty();
        }
        return singleDisjointStringSummarySpec(eq).map(DisjointStringTerm::spec);
    }

    private record DisjointStringTerm(String propertyName, String literalValue, SummaryCounterSpec spec) {}

    private static Optional<DisjointStringTerm> singleDisjointStringSummarySpec(PropertyIsEqualTo eq) {
        if (!eq.isMatchingCase() ||
                !(eq.getExpression1() instanceof PropertyName prop) ||
                !(eq.getExpression2() instanceof Literal lit) ||
                !(lit.getValue() instanceof String text))
            return Optional.empty();
        String propName = prop.getPropertyName();
        if ("alarmState".equals(propName)) {
            SummaryCounterSpec spec = switch (text) {
                case "Fault - Gamma High" -> new SummaryCounterSpec(
                        "COALESCE(s.gamma_high_fault_count, 0)",
                        "COALESCE(s.gamma_high_fault_count, 0)",
                        "s.summary_populated");
                case "Fault - Gamma Low" -> new SummaryCounterSpec(
                        "COALESCE(s.gamma_low_fault_count, 0)",
                        "COALESCE(s.gamma_low_fault_count, 0)",
                        "s.summary_populated");
                case "Fault - Neutron" -> new SummaryCounterSpec(
                        "COALESCE(s.neutron_fault_count, 0)",
                        "COALESCE(s.neutron_fault_count, 0)",
                        "s.summary_populated");
                case "Fault - Neutron High" -> new SummaryCounterSpec(
                        "COALESCE(s.neutron_high_fault_count, COALESCE(s.row_count, 0))",
                        "COALESCE(s.neutron_high_fault_count, 0)",
                        "(s.summary_populated AND s.neutron_high_fault_count IS NOT NULL)");
                case "Fault - Extended Occupancy" -> new SummaryCounterSpec(
                        "COALESCE(s.extended_occupancy_fault_count, 0)",
                        "COALESCE(s.extended_occupancy_fault_count, 0)",
                        "s.summary_populated");
                default -> null;
            };
            return spec == null ? Optional.empty() : Optional.of(new DisjointStringTerm(propName, text, spec));
        }
        if ("alarmCategoryCode".equals(propName)) {
            SummaryCounterSpec spec = switch (text) {
                case "Gamma" -> new SummaryCounterSpec(
                        "COALESCE(s.alarm_category_gamma_count, COALESCE(s.row_count, 0))",
                        "COALESCE(s.alarm_category_gamma_count, 0)",
                        "(s.summary_populated AND s.alarm_category_gamma_count IS NOT NULL)");
                case "Neutron" -> new SummaryCounterSpec(
                        "COALESCE(s.alarm_category_neutron_count, COALESCE(s.row_count, 0))",
                        "COALESCE(s.alarm_category_neutron_count, 0)",
                        "(s.summary_populated AND s.alarm_category_neutron_count IS NOT NULL)");
                default -> null;
            };
            return spec == null ? Optional.empty() : Optional.of(new DisjointStringTerm(propName, text, spec));
        }
        return Optional.empty();
    }

    private static Optional<SummaryCounterSpec> disjointStringEqualityOrSummarySpec(Or or) {
        List<Filter> disjuncts = new ArrayList<>();
        flattenOrChildren(or, disjuncts);
        if (disjuncts.isEmpty())
            return Optional.empty();
        String sharedProperty = null;
        Map<String, SummaryCounterSpec> byValue = new LinkedHashMap<>();
        for (Filter child : disjuncts) {
            if (!(child instanceof PropertyIsEqualTo eq))
                return Optional.empty();
            Optional<DisjointStringTerm> termOpt = singleDisjointStringSummarySpec(eq);
            if (termOpt.isEmpty())
                return Optional.empty();
            DisjointStringTerm term = termOpt.get();
            if (sharedProperty == null)
                sharedProperty = term.propertyName();
            else if (!sharedProperty.equals(term.propertyName()))
                return Optional.empty();
            byValue.putIfAbsent(term.literalValue(), term.spec());
        }
        if (byValue.isEmpty())
            return Optional.empty();
        if (byValue.size() == 1)
            return Optional.of(byValue.values().iterator().next());

        List<String> uppers = new ArrayList<>(byValue.size());
        List<String> exacts = new ArrayList<>(byValue.size());
        Set<String> readies = new LinkedHashSet<>();
        boolean hasStrictSummaryPopulated = false;
        for (SummaryCounterSpec spec : byValue.values()) {
            uppers.add(spec.upperBoundExpr());
            exacts.add(spec.exactExpr());
            String ready = spec.exactReadyExpr();
            if (!"true".equals(ready)) {
                readies.add(ready);
                if (ready.contains("s.summary_populated AND "))
                    hasStrictSummaryPopulated = true;
            }
        }
        if (hasStrictSummaryPopulated)
            readies.remove("s.summary_populated");
        String combinedReady = readies.isEmpty()
                ? "true"
                : (readies.size() == 1
                        ? readies.iterator().next()
                        : "(" + String.join(" AND ", readies) + ")");
        return Optional.of(new SummaryCounterSpec(
                "(" + String.join(" + ", uppers) + ")",
                "(" + String.join(" + ", exacts) + ")",
                combinedReady));
    }

    /**
     * Extracts an exact {@code eventId = '<id>'} predicate when the CQL filter
     * is an exact case-sensitive equality on {@code eventId} (or an {@code And}
     * containing one).
     */
    public static Optional<String> extractExactEventIdFilter(Filter filter) {
        if (filter instanceof PropertyIsEqualTo eq &&
                eq.isMatchingCase() &&
                eq.getExpression1() instanceof PropertyName prop &&
                "eventId".equals(prop.getPropertyName()) &&
                eq.getExpression2() instanceof Literal lit &&
                lit.getValue() instanceof String text &&
                !text.isEmpty() && text.length() <= 256) {
            return Optional.of(text);
        }
        if (filter instanceof And and) {
            for (Filter child : and.getChildren()) {
                Optional<String> found = extractExactEventIdFilter(child);
                if (found.isPresent())
                    return found;
            }
        }
        return Optional.empty();
    }

    private record PrefilterClause(String sql, List<Object> parameters, boolean selective) {}

    private static PrefilterClause buildDuckDbPrefilter(Filter filter, String column) {
        if (filter == null || filter == Filter.INCLUDE)
            return null;
        if (filter == Filter.EXCLUDE)
            return new PrefilterClause("FALSE", List.of(), true);
        if (filter instanceof And and) {
            List<String> clauses = new ArrayList<>();
            List<Object> params = new ArrayList<>();
            boolean selective = false;
            for (Filter child : and.getChildren()) {
                PrefilterClause sub = buildDuckDbPrefilter(child, column);
                if (sub != null) {
                    clauses.add("(" + sub.sql() + ")");
                    params.addAll(sub.parameters());
                    selective |= sub.selective();
                }
            }
            return clauses.isEmpty() ? null :
                    new PrefilterClause(String.join(" AND ", clauses), params, selective);
        }
        if (filter instanceof Or or) {
            if (or.getChildren().isEmpty())
                return null;
            PrefilterClause factored = tryBuildFactoredStringOrPrefilter(or, column);
            if (factored != null)
                return factored;
            List<String> clauses = new ArrayList<>();
            List<Object> params = new ArrayList<>();
            boolean selective = true;
            for (Filter child : or.getChildren()) {
                PrefilterClause sub = buildDuckDbPrefilter(child, column);
                if (sub == null)
                    return null;
                clauses.add("(" + sub.sql() + ")");
                params.addAll(sub.parameters());
                selective &= sub.selective();
            }
            return new PrefilterClause(String.join(" OR ", clauses), params, selective);
        }
        if (filter instanceof PropertyIsLike like) {
            if (!(like.getExpression() instanceof PropertyName property))
                return null;
            String leafKey = extractSafeLeafKey(property.getPropertyName());
            String literalRun = extractLongestSafeLikeSubstring(like);
            if (!literalRun.isEmpty()) {
                List<Object> params = new ArrayList<>();
                StringBuilder expr = new StringBuilder();
                if (like.isMatchingCase()) {
                    expr.append("strpos(").append(column).append(", ?) > 0");
                    params.add(literalRun);
                } else {
                    expr.append("strpos(lower(").append(column).append("), ?) > 0");
                    params.add(literalRun.toLowerCase(Locale.ROOT));
                }
                if (leafKey != null) {
                    expr.append(" AND strpos(").append(column).append(", ?) > 0");
                    params.add(leafKey);
                }
                return new PrefilterClause(expr.toString(), params, literalRun.length() >= 2);
            }
            return leafKeyPrefilter(property.getPropertyName(), column);
        }
        if (filter instanceof BinaryComparisonOperator comparison) {
            if (!(comparison.getExpression1() instanceof PropertyName property) ||
                    !(comparison.getExpression2() instanceof Literal literal))
                return null;
            Object expected = literal.getValue();
            if (expected == null)
                return null;
            String leafKey = extractSafeLeafKey(property.getPropertyName());
            if (comparison instanceof PropertyIsEqualTo) {
                if (expected instanceof String text && !text.isEmpty() &&
                        SAFE_JSON_STRING_LITERAL.matcher(text).matches()) {
                    List<Object> params = new ArrayList<>();
                    StringBuilder expr = new StringBuilder();
                    if (comparison.isMatchingCase()) {
                        expr.append("strpos(").append(column).append(", ?) > 0");
                        params.add(text);
                    } else {
                        expr.append("strpos(lower(").append(column).append("), ?) > 0");
                        params.add(text.toLowerCase(Locale.ROOT));
                    }
                    if (leafKey != null) {
                        expr.append(" AND strpos(").append(column).append(", ?) > 0");
                        params.add(leafKey);
                    }
                    return new PrefilterClause(expr.toString(), params, true);
                }
                if (expected instanceof Boolean boolVal) {
                    if (leafKey != null && leafKey.equals(property.getPropertyName()) &&
                            ("gammaAlarm".equals(leafKey) || "neutronAlarm".equals(leafKey) || "tamperStatus".equals(leafKey))) {
                        String valStr = boolVal ? "true" : "false";
                        String compactPattern = "\"" + leafKey + "\":" + valStr;
                        String spacedPattern = "\"" + leafKey + "\": " + valStr;
                        String expr = "(strpos(" + column + ", ?) > 0 OR strpos(" + column + ", ?) > 0)";
                        return new PrefilterClause(expr, List.of(compactPattern, spacedPattern), boolVal);
                    }
                    List<Object> params = new ArrayList<>();
                    StringBuilder expr = new StringBuilder();
                    if (leafKey != null) {
                        expr.append("strpos(").append(column).append(", ?) > 0 AND ");
                        params.add(leafKey);
                    }
                    expr.append("strpos(lower(").append(column).append("), ?) > 0");
                    params.add(boolVal ? "true" : "false");
                    return new PrefilterClause(expr.toString(), params, false);
                }
                if ("adjudicatedIdsCount".equals(property.getPropertyName()) && isZeroNumber(expected)) {
                    String expr = "(strpos(" + column + ", '\"adjudicatedIdsCount\":0') > 0 OR " +
                            "strpos(" + column + ", '\"adjudicatedIdsCount\": 0') > 0 OR " +
                            "strpos(" + column + ", '\"adjudicatedIds\":[]') > 0 OR " +
                            "strpos(" + column + ", '\"adjudicatedIds\": []') > 0)";
                    return new PrefilterClause(expr, List.of(), true);
                }
                if (expected instanceof Number &&
                        !"adjudicatedIdsCount".equals(property.getPropertyName()) &&
                        SAFE_JSON_DOLLAR_PATH.matcher(property.getPropertyName()).matches()) {
                    String dec = formatExactDecimal38_9(expected);
                    if (dec != null) {
                        String jsonPath = "'$." + property.getPropertyName() + "'";
                        List<Object> params = new ArrayList<>(2);
                        StringBuilder expr = new StringBuilder();
                        if (leafKey != null) {
                            expr.append("strpos(").append(column).append(", ?) > 0 AND ");
                            params.add(leafKey);
                        }
                        expr.append("(json_type(").append(column).append(", ").append(jsonPath)
                                .append(") = 'ARRAY' OR (json_type(").append(column).append(", ").append(jsonPath)
                                .append(") IN ('UBIGINT', 'BIGINT', 'DOUBLE') AND TRY_CAST(json_extract(")
                                .append(column).append(", ").append(jsonPath)
                                .append(") AS DECIMAL(38, 9)) = CAST(? AS DECIMAL(38, 9))))");
                        params.add(dec);
                        return new PrefilterClause(expr.toString(), params, true);
                    }
                }
            } else if ((comparison instanceof PropertyIsGreaterThan ||
                    comparison instanceof PropertyIsGreaterThanOrEqualTo ||
                    comparison instanceof PropertyIsLessThan ||
                    comparison instanceof PropertyIsLessThanOrEqualTo) &&
                    !"adjudicatedIdsCount".equals(property.getPropertyName()) &&
                    SAFE_JSON_DOLLAR_PATH.matcher(property.getPropertyName()).matches()) {
                if (expected instanceof String text) {
                    Instant parsedInstant = parseRfc3339Instant(text);
                    if (parsedInstant != null && parsedInstant.getNano() % 1_000 == 0) {
                        String sqlOp = sqlComparisonOp(comparison);
                        String jsonPath = "'$." + property.getPropertyName() + "'";
                        String valExpr = "json_extract_string(" + column + ", " + jsonPath + ")";
                        List<Object> params = new ArrayList<>(3);
                        StringBuilder expr = new StringBuilder();
                        if (leafKey != null) {
                            expr.append("strpos(").append(column).append(", ?) > 0 AND ");
                            params.add(leafKey);
                        }
                        expr.append("json_type(").append(column).append(", ").append(jsonPath)
                                .append(") = 'VARCHAR' AND regexp_matches(").append(valExpr).append(", ?) ")
                                .append("AND TRY_CAST(").append(valExpr)
                                .append(" AS TIMESTAMPTZ) >= TIMESTAMPTZ '0001-01-01T00:00:00Z' ")
                                .append("AND TRY_CAST(").append(valExpr)
                                .append(" AS TIMESTAMPTZ) < TIMESTAMPTZ '10000-01-01T00:00:00Z' ")
                                .append("AND TRY_CAST(").append(valExpr).append(" AS TIMESTAMPTZ) ")
                                .append(sqlOp).append(" TRY_CAST(? AS TIMESTAMPTZ)");
                        params.add(RFC3339_INSTANT.pattern());
                        params.add(parsedInstant.toString());
                        return new PrefilterClause(expr.toString(), params, true);
                    }
                }
                if (expected instanceof Number ||
                        (expected instanceof String text && parseRfc3339Instant(text) == null)) {
                    String dec = formatExactDecimal38_9(expected);
                    if (dec != null) {
                        String sqlOp = sqlComparisonOp(comparison);
                        String jsonPath = "'$." + property.getPropertyName() + "'";
                        List<Object> params = new ArrayList<>(2);
                        StringBuilder expr = new StringBuilder();
                        if (leafKey != null) {
                            expr.append("strpos(").append(column).append(", ?) > 0 AND ");
                            params.add(leafKey);
                        }
                        expr.append("json_type(").append(column).append(", ").append(jsonPath)
                                .append(") IN ('UBIGINT', 'BIGINT', 'DOUBLE') AND TRY_CAST(json_extract(")
                                .append(column).append(", ").append(jsonPath)
                                .append(") AS DECIMAL(38, 9)) ").append(sqlOp).append(" CAST(? AS DECIMAL(38, 9))");
                        params.add(dec);
                        return new PrefilterClause(expr.toString(), params, true);
                    }
                }
            }
            return leafKey == null ? null : leafKeyPrefilter(property.getPropertyName(), column);
        }
        return null;
    }

    public record ExactDuckDbPredicate(String sql, List<Object> parameters) {}

    /**
     * Compiles {@code filter} into an exact DuckDB SQL predicate over JSON column
     * {@code column} when every AST node is 100% equivalent to {@link #matches(Filter, String)},
     * allowing {@code countCql} to aggregate {@code GROUP BY filename} natively inside
     * DuckDB without transferring {@code result_json} strings across JDBC/JNI.
     */
    public static Optional<ExactDuckDbPredicate> tryBuildExactDuckDbPredicate(Filter filter, String column) {
        return tryBuildExactDuckDbPredicate(filter, column, ProjectedColumnMode.NONE);
    }

    public static Optional<ExactDuckDbPredicate> tryBuildExactDuckDbPredicate(
            Filter filter, String column, ProjectedColumnMode mode) {
        ProjectedColumnMode effectiveMode = mode == null ? ProjectedColumnMode.NONE : mode;
        if (filter == null || filter == Filter.INCLUDE)
            return Optional.of(new ExactDuckDbPredicate("TRUE", List.of()));
        if (filter == Filter.EXCLUDE)
            return Optional.of(new ExactDuckDbPredicate("FALSE", List.of()));
        ExactDuckDbPredicate node = buildExactDuckDbNode(filter, column, effectiveMode);
        if (node == null)
            return Optional.empty();
        if ("FALSE".equals(node.sql()) || "TRUE".equals(node.sql()))
            return Optional.of(node);

        if (effectiveMode == ProjectedColumnMode.PURE_V2_EVENTS) {
            if (!node.sql().contains(column) && !containsNegation(filter)) {
                return Optional.of(node);
            }
            return Optional.of(new ExactDuckDbPredicate(
                    "json_type(" + column + ") = 'OBJECT' AND (" + node.sql() + ")",
                    node.parameters()));
        }
        if (effectiveMode == ProjectedColumnMode.MIXED_UNION_BY_NAME) {
            if (!containsNegation(filter)) {
                return Optional.of(node);
            }
            return Optional.of(new ExactDuckDbPredicate(
                    "((COALESCE(archive_version, 1) >= 2 AND profile IN ('RAW_EVENTS', 'DAILY_EVENTS', 'MONTHLY_EVENTS')) OR json_type(" +
                            column + ") = 'OBJECT') AND (" + node.sql() + ")",
                    node.parameters()));
        }

        ExactDuckDbPredicate fastGuard = buildFastStrposGuard(filter, column);
        if (fastGuard != null && !"TRUE".equals(fastGuard.sql())) {
            List<Object> combined = new ArrayList<>(fastGuard.parameters().size() + node.parameters().size());
            combined.addAll(fastGuard.parameters());
            combined.addAll(node.parameters());
            String caseSql = "CASE WHEN ((" + fastGuard.sql() + ") OR " +
                    nonStandardJsonFormattingEscape(column) + ") " +
                    "THEN (json_type(" + column + ") = 'OBJECT' AND (" + node.sql() + ")) " +
                    "ELSE FALSE END";
            return Optional.of(new ExactDuckDbPredicate(caseSql, combined));
        }
        return Optional.of(new ExactDuckDbPredicate(
                "json_type(" + column + ") = 'OBJECT' AND (" + node.sql() + ")",
                node.parameters()));
    }

    private static boolean containsNegation(Filter filter) {
        if (filter instanceof Not)
            return true;
        if (filter instanceof And and) {
            for (Filter child : and.getChildren()) {
                if (containsNegation(child))
                    return true;
            }
        } else if (filter instanceof Or or) {
            for (Filter child : or.getChildren()) {
                if (containsNegation(child))
                    return true;
            }
        }
        return false;
    }

    private static String nonStandardJsonFormattingEscape(String column) {
        return "strpos(" + column + ", '\\u') > 0 OR " +
                "strpos(" + column + ", '\" ') > 0 OR " +
                "strpos(" + column + ", ':  ') > 0 OR " +
                "strpos(" + column + ", chr(9)) > 0 OR " +
                "strpos(" + column + ", chr(10)) > 0 OR " +
                "strpos(" + column + ", chr(13)) > 0";
    }

    /**
     * Builds a pure SIMD {@code strpos}-only sound pre-guard (zero {@code yyjson} parses)
     * for {@code CASE WHEN ... THEN ... ELSE FALSE END} selection-vector pruning in DuckDB.
     */
    private static ExactDuckDbPredicate buildFastStrposGuard(Filter filter, String column) {
        if (filter == null || filter == Filter.INCLUDE)
            return new ExactDuckDbPredicate("TRUE", List.of());
        if (filter == Filter.EXCLUDE)
            return new ExactDuckDbPredicate("FALSE", List.of());
        if (filter instanceof And and) {
            List<String> parts = new ArrayList<>();
            List<Object> params = new ArrayList<>();
            for (Filter child : and.getChildren()) {
                ExactDuckDbPredicate sub = buildFastStrposGuard(child, column);
                if (sub == null || "TRUE".equals(sub.sql()))
                    continue;
                if ("FALSE".equals(sub.sql()))
                    return sub;
                parts.add("(" + sub.sql() + ")");
                params.addAll(sub.parameters());
            }
            return parts.isEmpty()
                    ? new ExactDuckDbPredicate("TRUE", List.of())
                    : new ExactDuckDbPredicate(String.join(" AND ", parts), params);
        }
        if (filter instanceof Or or) {
            if (or.getChildren().isEmpty())
                return new ExactDuckDbPredicate("FALSE", List.of());
            List<String> parts = new ArrayList<>();
            List<Object> params = new ArrayList<>();
            for (Filter child : or.getChildren()) {
                ExactDuckDbPredicate sub = buildFastStrposGuard(child, column);
                if (sub == null || "TRUE".equals(sub.sql()))
                    return new ExactDuckDbPredicate("TRUE", List.of());
                if (!"FALSE".equals(sub.sql())) {
                    parts.add("(" + sub.sql() + ")");
                    params.addAll(sub.parameters());
                }
            }
            return parts.isEmpty()
                    ? new ExactDuckDbPredicate("FALSE", List.of())
                    : new ExactDuckDbPredicate(String.join(" OR ", parts), params);
        }
        if (filter instanceof PropertyIsLike like &&
                like.getExpression() instanceof PropertyName property) {
            String leafKey = extractSafeLeafKey(property.getPropertyName());
            if (leafKey != null) {
                return new ExactDuckDbPredicate("strpos(" + column + ", ?) > 0", List.of(leafKey));
            }
        }
        if (filter instanceof BinaryComparisonOperator cmp &&
                cmp.getExpression1() instanceof PropertyName property &&
                cmp.getExpression2() instanceof Literal literal) {
            String path = property.getPropertyName();
            Object expected = literal.getValue();
            if (path == null || expected == null)
                return null;
            String leafKey = extractSafeLeafKey(path);
            if ("adjudicatedIdsCount".equals(path)) {
                return new ExactDuckDbPredicate(
                        "strpos(" + column + ", 'adjudicatedIds') > 0", List.of());
            }
            if (cmp instanceof PropertyIsEqualTo && expected instanceof Boolean boolVal) {
                String tok = boolVal ? "true" : "false";
                if (leafKey != null && leafKey.equals(path)) {
                    return new ExactDuckDbPredicate(
                            "strpos(" + column + ", ?) > 0 OR strpos(" + column + ", ?) > 0",
                            List.of("\"" + leafKey + "\":" + tok, "\"" + leafKey + "\": " + tok));
                }
                if (leafKey != null) {
                    return new ExactDuckDbPredicate(
                            "strpos(" + column + ", ?) > 0 AND strpos(" + column + ", ?) > 0",
                            List.of(tok, leafKey));
                }
                return new ExactDuckDbPredicate("strpos(" + column + ", ?) > 0", List.of(tok));
            }
            if (leafKey != null) {
                return new ExactDuckDbPredicate("strpos(" + column + ", ?) > 0", List.of(leafKey));
            }
        }
        return null;
    }

    private static ExactDuckDbPredicate buildExactDuckDbNode(
            Filter filter, String column, ProjectedColumnMode mode) {
        if (filter == Filter.INCLUDE)
            return new ExactDuckDbPredicate("TRUE", List.of());
        if (filter == Filter.EXCLUDE)
            return new ExactDuckDbPredicate("FALSE", List.of());
        if (filter instanceof And and) {
            if (and.getChildren().isEmpty())
                return new ExactDuckDbPredicate("TRUE", List.of());
            List<String> clauses = new ArrayList<>(and.getChildren().size());
            List<Object> params = new ArrayList<>();
            for (Filter child : and.getChildren()) {
                ExactDuckDbPredicate sub = buildExactDuckDbNode(child, column, mode);
                if (sub == null)
                    return null;
                if ("FALSE".equals(sub.sql()))
                    return new ExactDuckDbPredicate("FALSE", List.of());
                if (!"TRUE".equals(sub.sql())) {
                    clauses.add("(" + sub.sql() + ")");
                    params.addAll(sub.parameters());
                }
            }
            return clauses.isEmpty()
                    ? new ExactDuckDbPredicate("TRUE", List.of())
                    : new ExactDuckDbPredicate(String.join(" AND ", clauses), params);
        }
        if (filter instanceof Or or) {
            if (or.getChildren().isEmpty())
                return new ExactDuckDbPredicate("FALSE", List.of());
            List<String> clauses = new ArrayList<>(or.getChildren().size());
            List<Object> params = new ArrayList<>();
            for (Filter child : or.getChildren()) {
                ExactDuckDbPredicate sub = buildExactDuckDbNode(child, column, mode);
                if (sub == null)
                    return null;
                if ("TRUE".equals(sub.sql()))
                    return new ExactDuckDbPredicate("TRUE", List.of());
                if (!"FALSE".equals(sub.sql())) {
                    clauses.add("(" + sub.sql() + ")");
                    params.addAll(sub.parameters());
                }
            }
            return clauses.isEmpty()
                    ? new ExactDuckDbPredicate("FALSE", List.of())
                    : new ExactDuckDbPredicate(String.join(" OR ", clauses), params);
        }
        if (filter instanceof Not not) {
            ExactDuckDbPredicate sub = buildExactDuckDbNode(not.getFilter(), column, mode);
            if (sub == null)
                return null;
            return new ExactDuckDbPredicate("NOT COALESCE((" + sub.sql() + "), FALSE)", sub.parameters());
        }
        if (filter instanceof PropertyIsLike like) {
            return buildExactDuckDbLikeNode(like, column, mode);
        }
        if (filter instanceof BinaryComparisonOperator cmp) {
            if (!(cmp.getExpression1() instanceof PropertyName property) ||
                    !(cmp.getExpression2() instanceof Literal literal))
                return null;
            String path = property.getPropertyName();
            Object expected = literal.getValue();
            if (expected == null || path == null || !SAFE_JSON_DOLLAR_PATH.matcher(path).matches())
                return null;

            ExactDuckDbPredicate v1Pred = buildV1JsonComparisonPredicate(cmp, path, expected, column);
            if (v1Pred == null)
                return null;
            if (mode == ProjectedColumnMode.NONE)
                return v1Pred;

            ExactDuckDbPredicate v2Pred = tryBuildV2ProjectedComparisonPredicate(cmp, path, expected);
            if (v2Pred != null) {
                if (mode == ProjectedColumnMode.PURE_V2_EVENTS)
                    return v2Pred;
                return combineMixedV2AndV1(v2Pred, v1Pred, column);
            }
            if (mode == ProjectedColumnMode.MIXED_UNION_BY_NAME) {
                return new ExactDuckDbPredicate(
                        "(json_type(" + column + ") = 'OBJECT' AND (" + v1Pred.sql() + "))",
                        v1Pred.parameters());
            }
            return v1Pred;
        }
        return null;
    }

    private static ExactDuckDbPredicate combineMixedV2AndV1(
            ExactDuckDbPredicate v2Pred, ExactDuckDbPredicate v1Pred, String jsonColumn) {
        List<Object> params = new ArrayList<>(v2Pred.parameters().size() + v1Pred.parameters().size());
        params.addAll(v2Pred.parameters());
        params.addAll(v1Pred.parameters());
        String sql = "(CASE WHEN COALESCE(archive_version, 1) = 2 AND profile IN ('RAW_EVENTS', 'DAILY_EVENTS', 'MONTHLY_EVENTS') " +
                "THEN COALESCE((" + v2Pred.sql() + "), FALSE) " +
                "ELSE (json_type(" + jsonColumn + ") = 'OBJECT' AND COALESCE((" + v1Pred.sql() + "), FALSE)) END)";
        return new ExactDuckDbPredicate(sql, params);
    }

    private static String projectedStringColumnFor(String path) {
        return switch (path) {
            case "normalizedValue" -> "ocr_norm";
            case "occupancyObsId" -> "occ_obs_id";
            case "eventId" -> "event_id";
            default -> null;
        };
    }

    private static ExactDuckDbPredicate tryBuildV2ProjectedComparisonPredicate(
            BinaryComparisonOperator cmp, String path, Object expected) {
        String sqlOp = sqlComparisonOp(cmp);
        if (sqlOp == null)
            return null;

        String stringCol = projectedStringColumnFor(path);
        if (stringCol != null && cmp instanceof PropertyIsEqualTo && expected instanceof String text) {
            if (cmp.isMatchingCase()) {
                return new ExactDuckDbPredicate(stringCol + " = ?", List.of(text));
            }
            return new ExactDuckDbPredicate("lower(" + stringCol + ") = lower(?)", List.of(text));
        }

        if (("gammaAlarm".equals(path) || "neutronAlarm".equals(path)) &&
                cmp instanceof PropertyIsEqualTo && expected instanceof Boolean boolVal) {
            String boolCol = "gammaAlarm".equals(path) ? "gamma_alarm" : "neutron_alarm";
            return new ExactDuckDbPredicate(boolCol + " = ?", List.of(boolVal));
        }

        if ("adjudicationCode".equals(path) || "adjudicatedIdsCount".equals(path)) {
            if (cmp instanceof PropertyIsEqualTo && !(expected instanceof Number))
                return null;
            String intCol = "adjudicationCode".equals(path) ? "adj_code" : "adj_count";
            Integer exactInt = tryExactInteger(expected);
            if (exactInt != null) {
                return new ExactDuckDbPredicate(intCol + " " + sqlOp + " ?", List.of(exactInt));
            }
            String dec = formatExactDecimal38_9(expected);
            if (dec != null) {
                return new ExactDuckDbPredicate(
                        "CAST(" + intCol + " AS DECIMAL(38, 9)) " + sqlOp + " CAST(? AS DECIMAL(38, 9))",
                        List.of(dec));
            }
            return null;
        }

        if ("maxGammaCount".equals(path) || "maxNeutronCount".equals(path)) {
            if (cmp instanceof PropertyIsEqualTo && !(expected instanceof Number))
                return null;
            Double exactDouble = tryExactIeeeDouble(expected);
            if (exactDouble != null) {
                String dblCol = "maxGammaCount".equals(path) ? "max_gamma" : "max_neutron";
                return new ExactDuckDbPredicate(dblCol + " " + sqlOp + " ?", List.of(exactDouble));
            }
        }
        return null;
    }

    private static Integer tryExactInteger(Object value) {
        if (!(value instanceof Number || value instanceof String))
            return null;
        try {
            BigDecimal bd = new BigDecimal(value.toString()).stripTrailingZeros();
            if (bd.scale() > 0)
                return null;
            return bd.intValueExact();
        } catch (RuntimeException ignored) {
            return null;
        }
    }

    private static Double tryExactIeeeDouble(Object value) {
        if (!(value instanceof Number || value instanceof String))
            return null;
        try {
            BigDecimal bd = new BigDecimal(value.toString()).stripTrailingZeros();
            double d = bd.doubleValue();
            if (!Double.isFinite(d))
                return null;
            if (new BigDecimal(Double.toString(d)).compareTo(bd) == 0)
                return d;
            return null;
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private static ExactDuckDbPredicate buildV1JsonComparisonPredicate(
            BinaryComparisonOperator cmp, String path, Object expected, String column) {
        String jsonPath = "'$." + path + "'";
        String leafKey = extractSafeLeafKey(path);

        if ("adjudicatedIdsCount".equals(path)) {
            String sqlOp = sqlComparisonOp(cmp);
            if (sqlOp == null)
                return null;
            if (cmp instanceof PropertyIsEqualTo && !(expected instanceof Number))
                return null;
            String dec = formatExactDecimal38_9(expected);
            if (dec == null)
                return null;
            String expr = "(CASE " +
                    "WHEN json_type(" + column + ", '$.adjudicatedIdsCount') IN ('UBIGINT', 'BIGINT', 'DOUBLE') " +
                    "THEN TRY_CAST(json_extract(" + column + ", '$.adjudicatedIdsCount') AS DECIMAL(38, 9)) " +
                    "WHEN (json_type(" + column + ", '$.adjudicatedIdsCount') IS NULL OR " +
                    "json_type(" + column + ", '$.adjudicatedIdsCount') = 'NULL') AND " +
                    "json_type(" + column + ", '$.adjudicatedIds') = 'ARRAY' " +
                    "THEN CAST(json_array_length(json_extract(" + column + ", '$.adjudicatedIds')) AS DECIMAL(38, 9)) " +
                    "ELSE NULL END " + sqlOp + " CAST(? AS DECIMAL(38, 9)))";
            return new ExactDuckDbPredicate(expr, List.of(dec));
        }

        if (cmp instanceof PropertyIsGreaterThan ||
                cmp instanceof PropertyIsGreaterThanOrEqualTo ||
                cmp instanceof PropertyIsLessThan ||
                cmp instanceof PropertyIsLessThanOrEqualTo) {
            String sqlOp = sqlComparisonOp(cmp);
            if (expected instanceof String text) {
                Instant parsedInstant = parseRfc3339Instant(text);
                if (parsedInstant != null) {
                    if (parsedInstant.getNano() % 1_000 != 0)
                        return null;
                    String valExpr = "json_extract_string(" + column + ", " + jsonPath + ")";
                    List<Object> params = new ArrayList<>(3);
                    StringBuilder sb = new StringBuilder("(");
                    if (leafKey != null) {
                        sb.append("(strpos(").append(column).append(", ?) > 0 OR strpos(")
                                .append(column).append(", '\\u') > 0) AND ");
                        params.add(leafKey);
                    }
                    sb.append("json_type(").append(column).append(", ").append(jsonPath)
                            .append(") = 'VARCHAR' AND regexp_matches(").append(valExpr).append(", ?) ")
                            .append("AND TRY_CAST(").append(valExpr)
                            .append(" AS TIMESTAMPTZ) >= TIMESTAMPTZ '0001-01-01T00:00:00Z' ")
                            .append("AND TRY_CAST(").append(valExpr)
                            .append(" AS TIMESTAMPTZ) < TIMESTAMPTZ '10000-01-01T00:00:00Z' ")
                            .append("AND TRY_CAST(").append(valExpr).append(" AS TIMESTAMPTZ) ")
                            .append(sqlOp).append(" TRY_CAST(? AS TIMESTAMPTZ))");
                    params.add(RFC3339_INSTANT.pattern());
                    params.add(parsedInstant.toString());
                    return new ExactDuckDbPredicate(sb.toString(), params);
                }
            }
            String dec = formatExactDecimal38_9(expected);
            if (dec == null)
                return null;
            List<Object> params = new ArrayList<>(2);
            StringBuilder sb = new StringBuilder("(");
            if (leafKey != null) {
                sb.append("(strpos(").append(column).append(", ?) > 0 OR strpos(")
                        .append(column).append(", '\\u') > 0) AND ");
                params.add(leafKey);
            }
            sb.append("json_type(").append(column).append(", ").append(jsonPath)
                    .append(") IN ('UBIGINT', 'BIGINT', 'DOUBLE') AND TRY_CAST(json_extract(")
                    .append(column).append(", ").append(jsonPath)
                    .append(") AS DECIMAL(38, 9)) ").append(sqlOp).append(" CAST(? AS DECIMAL(38, 9)))");
            params.add(dec);
            return new ExactDuckDbPredicate(sb.toString(), params);
        }

        if (cmp instanceof PropertyIsEqualTo && EXACT_DUCKDB_SCALAR_KEYS.contains(path)) {
            if (expected instanceof Boolean boolVal) {
                String token = boolVal ? "true" : "false";
                String expr = "((strpos(" + column + ", ?) > 0 OR strpos(" + column + ", '\\u') > 0) AND " +
                        "json_type(" + column + ", " + jsonPath + ") = 'BOOLEAN' AND " +
                        "CAST(json_extract(" + column + ", " + jsonPath + ") AS BOOLEAN) = ?)";
                return new ExactDuckDbPredicate(expr, List.of(token, boolVal));
            }
            if (expected instanceof Number) {
                String dec = formatExactDecimal38_9(expected);
                if (dec == null)
                    return null;
                String expr = "((strpos(" + column + ", ?) > 0 OR strpos(" + column + ", '\\u') > 0) AND " +
                        "json_type(" + column + ", " + jsonPath + ") IN ('UBIGINT', 'BIGINT', 'DOUBLE') AND " +
                        "TRY_CAST(json_extract(" + column + ", " + jsonPath +
                        ") AS DECIMAL(38, 9)) = CAST(? AS DECIMAL(38, 9)))";
                return new ExactDuckDbPredicate(expr, List.of(path, dec));
            }
            if (expected instanceof String text) {
                List<Object> params = new ArrayList<>(2);
                StringBuilder sb = new StringBuilder("(");
                if (!text.isEmpty() && SAFE_JSON_STRING_LITERAL.matcher(text).matches()) {
                    if (cmp.isMatchingCase()) {
                        sb.append("(strpos(").append(column).append(", ?) > 0 OR strpos(")
                                .append(column).append(", '\\u') > 0) AND ");
                        params.add(text);
                    } else {
                        sb.append("(strpos(lower(").append(column).append("), ?) > 0 OR strpos(")
                                .append(column).append(", '\\u') > 0) AND ");
                        params.add(text.toLowerCase(Locale.ROOT));
                    }
                }
                sb.append("json_type(").append(column).append(", ").append(jsonPath).append(") = 'VARCHAR' AND ");
                if (cmp.isMatchingCase()) {
                    sb.append("json_extract_string(").append(column).append(", ").append(jsonPath).append(") = ?)");
                    params.add(text);
                } else {
                    sb.append("lower(json_extract_string(").append(column).append(", ").append(jsonPath)
                            .append(")) = lower(?))");
                    params.add(text);
                }
                return new ExactDuckDbPredicate(sb.toString(), params);
            }
        }
        return null;
    }

    private enum LikeShape {
        EXACT,
        PREFIX,
        CONTAINS,
        GENERAL
    }

    private record AnalyzedLikePattern(
            LikeShape shape,
            String literalValue,
            String duckLikePattern) {}

    private static AnalyzedLikePattern analyzeLikePattern(PropertyIsLike like) {
        String literal = like.getLiteral() == null ? "" : like.getLiteral();
        String wildcard = like.getWildCard() == null ? "" : like.getWildCard();
        String single = like.getSingleChar() == null ? "" : like.getSingleChar();
        String escape = like.getEscape() == null ? "" : like.getEscape();

        List<Object> tokens = new ArrayList<>();
        StringBuilder currentLiteral = new StringBuilder();
        StringBuilder sqlLike = new StringBuilder(literal.length() + 4);
        boolean hasSingle = false;

        for (int index = 0; index < literal.length(); ) {
            if (!escape.isEmpty() && literal.startsWith(escape, index)) {
                index += escape.length();
                if (index < literal.length()) {
                    int cp = literal.codePointAt(index);
                    currentLiteral.appendCodePoint(cp);
                    appendDuckLikeEscapedCodePoint(sqlLike, cp);
                    index += Character.charCount(cp);
                }
            } else if (!wildcard.isEmpty() && literal.startsWith(wildcard, index)) {
                if (!currentLiteral.isEmpty()) {
                    tokens.add(currentLiteral.toString());
                    currentLiteral.setLength(0);
                }
                if (tokens.isEmpty() || !WILDCARD_MARKER.equals(tokens.get(tokens.size() - 1))) {
                    tokens.add(WILDCARD_MARKER);
                }
                sqlLike.append('%');
                index += wildcard.length();
            } else if (!single.isEmpty() && literal.startsWith(single, index)) {
                if (!currentLiteral.isEmpty()) {
                    tokens.add(currentLiteral.toString());
                    currentLiteral.setLength(0);
                }
                tokens.add(SINGLE_MARKER);
                hasSingle = true;
                sqlLike.append('_');
                index += single.length();
            } else {
                int cp = literal.codePointAt(index);
                currentLiteral.appendCodePoint(cp);
                appendDuckLikeEscapedCodePoint(sqlLike, cp);
                index += Character.charCount(cp);
            }
        }
        if (!currentLiteral.isEmpty()) {
            tokens.add(currentLiteral.toString());
        }

        if (tokens.isEmpty()) {
            return new AnalyzedLikePattern(LikeShape.EXACT, "", "");
        }
        if (!hasSingle) {
            if (tokens.size() == 1 && tokens.get(0) instanceof String exact) {
                return new AnalyzedLikePattern(LikeShape.EXACT, exact, sqlLike.toString());
            }
            if (tokens.size() == 2 && tokens.get(0) instanceof String prefix &&
                    WILDCARD_MARKER.equals(tokens.get(1))) {
                return new AnalyzedLikePattern(LikeShape.PREFIX, prefix, sqlLike.toString());
            }
            if (tokens.size() == 3 && WILDCARD_MARKER.equals(tokens.get(0)) &&
                    tokens.get(1) instanceof String inner &&
                    WILDCARD_MARKER.equals(tokens.get(2))) {
                return new AnalyzedLikePattern(LikeShape.CONTAINS, inner, sqlLike.toString());
            }
        }
        return new AnalyzedLikePattern(LikeShape.GENERAL, null, sqlLike.toString());
    }

    private static final Object WILDCARD_MARKER = new Object();
    private static final Object SINGLE_MARKER = new Object();

    private static void appendDuckLikeEscapedCodePoint(StringBuilder out, int cp) {
        if (cp == '\\' || cp == '%' || cp == '_') {
            out.append('\\').append((char) cp);
        } else {
            out.appendCodePoint(cp);
        }
    }

    private static boolean isPureAsciiString(String s) {
        if (s == null)
            return true;
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) > 0x7F)
                return false;
        }
        return true;
    }

    private static ExactDuckDbPredicate buildStringExprLikePredicate(
            String expr, AnalyzedLikePattern analyzed, boolean matchingCase) {
        return switch (analyzed.shape()) {
            case EXACT -> matchingCase
                    ? new ExactDuckDbPredicate(expr + " = ?", List.of(analyzed.literalValue()))
                    : new ExactDuckDbPredicate("lower(" + expr + ") = lower(?)", List.of(analyzed.literalValue()));
            case PREFIX -> matchingCase
                    ? new ExactDuckDbPredicate("starts_with(" + expr + ", ?)", List.of(analyzed.literalValue()))
                    : new ExactDuckDbPredicate(expr + " ILIKE ? ESCAPE '\\'", List.of(analyzed.duckLikePattern()));
            case CONTAINS -> {
                if (matchingCase) {
                    yield new ExactDuckDbPredicate("strpos(" + expr + ", ?) > 0", List.of(analyzed.literalValue()));
                }
                if (isPureAsciiString(analyzed.literalValue())) {
                    yield new ExactDuckDbPredicate(
                            "strpos(lower(" + expr + "), ?) > 0",
                            List.of(analyzed.literalValue().toLowerCase(Locale.ROOT)));
                }
                yield new ExactDuckDbPredicate(expr + " ILIKE ? ESCAPE '\\'", List.of(analyzed.duckLikePattern()));
            }
            case GENERAL -> new ExactDuckDbPredicate(
                    expr + (matchingCase ? " LIKE " : " ILIKE ") + "? ESCAPE '\\'",
                    List.of(analyzed.duckLikePattern()));
        };
    }

    private static ExactDuckDbPredicate buildExactDuckDbLikeNode(
            PropertyIsLike like, String column, ProjectedColumnMode mode) {
        if (!(like.getExpression() instanceof PropertyName property))
            return null;
        String path = property.getPropertyName();
        if (path == null || !SAFE_JSON_DOLLAR_PATH.matcher(path).matches() ||
                !LIKE_EXACT_DUCKDB_STRING_KEYS.contains(path)) {
            return null;
        }
        if (!like.isMatchingCase() && !isPureAsciiString(like.getLiteral())) {
            return null;
        }

        AnalyzedLikePattern analyzed = analyzeLikePattern(like);
        String jsonPath = "'$." + path + "'";
        String leafKey = extractSafeLeafKey(path);
        String extractedExpr = "json_extract_string(" + column + ", " + jsonPath + ")";
        ExactDuckDbPredicate rawV1 = buildStringExprLikePredicate(extractedExpr, analyzed, like.isMatchingCase());
        List<Object> v1Params = new ArrayList<>(rawV1.parameters().size() + 1);
        StringBuilder v1Sql = new StringBuilder("(");
        if (leafKey != null) {
            v1Sql.append("(strpos(").append(column).append(", ?) > 0 OR strpos(")
                    .append(column).append(", '\\u') > 0) AND ");
            v1Params.add(leafKey);
        }
        v1Sql.append("json_type(").append(column).append(", ").append(jsonPath)
                .append(") = 'VARCHAR' AND ").append(rawV1.sql()).append(')');
        v1Params.addAll(rawV1.parameters());
        ExactDuckDbPredicate v1Pred = new ExactDuckDbPredicate(v1Sql.toString(), v1Params);

        if (mode == ProjectedColumnMode.NONE)
            return v1Pred;

        String projCol = projectedStringColumnFor(path);
        if (projCol != null) {
            ExactDuckDbPredicate v2Pred = buildStringExprLikePredicate(projCol, analyzed, like.isMatchingCase());
            if (mode == ProjectedColumnMode.PURE_V2_EVENTS)
                return v2Pred;
            return combineMixedV2AndV1(v2Pred, v1Pred, column);
        }
        if (mode == ProjectedColumnMode.MIXED_UNION_BY_NAME) {
            return new ExactDuckDbPredicate(
                    "(json_type(" + column + ") = 'OBJECT' AND (" + v1Pred.sql() + "))",
                    v1Pred.parameters());
        }
        return v1Pred;
    }

    private static String sqlComparisonOp(BinaryComparisonOperator cmp) {
        if (cmp instanceof PropertyIsEqualTo)
            return "=";
        if (cmp instanceof PropertyIsGreaterThan)
            return ">";
        if (cmp instanceof PropertyIsGreaterThanOrEqualTo)
            return ">=";
        if (cmp instanceof PropertyIsLessThan)
            return "<";
        if (cmp instanceof PropertyIsLessThanOrEqualTo)
            return "<=";
        return null;
    }

    private static String formatExactDecimal38_9(Object value) {
        if (!(value instanceof Number || value instanceof String))
            return null;
        try {
            BigDecimal bd = new BigDecimal(value.toString()).stripTrailingZeros();
            int scale = Math.max(0, bd.scale());
            int integerDigits = bd.precision() - bd.scale();
            if (scale > 9 || integerDigits > 29)
                return null;
            return bd.toPlainString();
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    /**
     * Optimizes multi-branch string equality OR filters (such as {@code faultCQL} over 90M+ archived count rows)
     * by testing a shared selective literal prefix and shared property key once before evaluating individual branches.
     */
    private static PrefilterClause tryBuildFactoredStringOrPrefilter(Or or, String column) {
        if (or.getChildren().size() < 2)
            return null;
        String sharedLeafKey = null;
        boolean firstBranch = true;
        List<String> caseSensitiveLiterals = new ArrayList<>(or.getChildren().size());
        for (Filter child : or.getChildren()) {
            if (!(child instanceof PropertyIsEqualTo eq) || !eq.isMatchingCase())
                return null;
            if (!(eq.getExpression1() instanceof PropertyName prop) ||
                    !(eq.getExpression2() instanceof Literal lit) ||
                    !(lit.getValue() instanceof String text) ||
                    text.isEmpty() ||
                    !SAFE_JSON_STRING_LITERAL.matcher(text).matches())
                return null;
            String leafKey = extractSafeLeafKey(prop.getPropertyName());
            if (firstBranch) {
                sharedLeafKey = leafKey;
                firstBranch = false;
            } else if (!java.util.Objects.equals(sharedLeafKey, leafKey)) {
                sharedLeafKey = null;
            }
            caseSensitiveLiterals.add(text);
        }

        String commonPrefix = longestCommonPrefix(caseSensitiveLiterals);
        List<Object> params = new ArrayList<>();
        StringBuilder expr = new StringBuilder();
        if (commonPrefix.length() >= 4) {
            expr.append("strpos(").append(column).append(", ?) > 0 AND ");
            params.add(commonPrefix);
        }
        if (sharedLeafKey != null) {
            expr.append("strpos(").append(column).append(", ?) > 0 AND ");
            params.add(sharedLeafKey);
        }
        expr.append('(');
        for (int i = 0; i < caseSensitiveLiterals.size(); i++) {
            if (i > 0)
                expr.append(" OR ");
            expr.append("strpos(").append(column).append(", ?) > 0");
            params.add(caseSensitiveLiterals.get(i));
        }
        expr.append(')');
        return new PrefilterClause(expr.toString(), params, true);
    }

    private static String longestCommonPrefix(List<String> values) {
        if (values == null || values.isEmpty())
            return "";
        String prefix = values.get(0);
        for (int i = 1; i < values.size() && !prefix.isEmpty(); i++) {
            String candidate = values.get(i);
            int max = Math.min(prefix.length(), candidate.length());
            int j = 0;
            while (j < max && prefix.charAt(j) == candidate.charAt(j))
                j++;
            prefix = prefix.substring(0, j);
        }
        return prefix;
    }

    private static String extractLongestSafeLikeSubstring(PropertyIsLike like) {
        String literal = like.getLiteral();
        if (literal == null || literal.isEmpty())
            return "";
        String wildcard = like.getWildCard() == null ? "" : like.getWildCard();
        String single = like.getSingleChar() == null ? "" : like.getSingleChar();
        String escape = like.getEscape() == null ? "" : like.getEscape();

        String best = "";
        StringBuilder current = new StringBuilder();
        for (int index = 0; index < literal.length();) {
            if (!escape.isEmpty() && literal.startsWith(escape, index)) {
                index += escape.length();
                if (index < literal.length()) {
                    int point = literal.codePointAt(index);
                    appendOrFlushSafeCodePoint(point, current);
                    if (current.length() > best.length())
                        best = current.toString();
                    index += Character.charCount(point);
                } else {
                    current.setLength(0);
                }
            } else if (!wildcard.isEmpty() && literal.startsWith(wildcard, index)) {
                current.setLength(0);
                index += wildcard.length();
            } else if (!single.isEmpty() && literal.startsWith(single, index)) {
                current.setLength(0);
                index += single.length();
            } else {
                int point = literal.codePointAt(index);
                appendOrFlushSafeCodePoint(point, current);
                if (current.length() > best.length())
                    best = current.toString();
                index += Character.charCount(point);
            }
        }
        return best;
    }

    private static void appendOrFlushSafeCodePoint(int codePoint, StringBuilder current) {
        if ((codePoint >= 'A' && codePoint <= 'Z')
                || (codePoint >= 'a' && codePoint <= 'z')
                || (codePoint >= '0' && codePoint <= '9')
                || codePoint == ' '
                || codePoint == '_'
                || codePoint == '.'
                || codePoint == ':'
                || codePoint == '-') {
            current.appendCodePoint(codePoint);
        } else {
            current.setLength(0);
        }
    }

    private static PrefilterClause leafKeyPrefilter(String propertyPath, String column) {
        String leafKey = extractSafeLeafKey(propertyPath);
        if (leafKey == null)
            return null;
        return new PrefilterClause(
                "strpos(" + column + ", ?) > 0",
                List.of(leafKey),
                false);
    }

    private static String extractSafeLeafKey(String propertyPath) {
        if (propertyPath == null || propertyPath.isEmpty())
            return null;
        String[] parts = propertyPath.split("\\.");
        String leaf = parts[parts.length - 1];
        return SAFE_JSON_KEY.matcher(leaf).matches() ? leaf : null;
    }

    private static boolean evaluate(Filter filter, JsonObject json) {
        if (filter == Filter.INCLUDE)
            return true;
        if (filter == Filter.EXCLUDE)
            return false;
        if (filter instanceof And and)
            return and.getChildren().stream().allMatch(child -> evaluate(child, json));
        if (filter instanceof Or or)
            return or.getChildren().stream().anyMatch(child -> evaluate(child, json));
        if (filter instanceof Not not)
            return !evaluate(not.getFilter(), json);
        if (filter instanceof PropertyIsLike like)
            return like(like, json);
        if (filter instanceof BinaryComparisonOperator comparison)
            return compare(comparison, json);
        throw new IllegalArgumentException("Unsupported archived CQL predicate: " + filter.getClass().getName());
    }

    private static boolean compare(BinaryComparisonOperator filter, JsonObject json) {
        if (!(filter.getExpression1() instanceof PropertyName property) ||
                !(filter.getExpression2() instanceof Literal literal))
            throw new IllegalArgumentException("Archived CQL comparisons require property-to-literal expressions");
        JsonElement actual = resolve(json, property.getPropertyName());
        Object expected = literal.getValue();
        boolean absent = actual == null || actual instanceof JsonNull || actual.isJsonNull();

        if (filter instanceof PropertyIsEqualTo) {
            if (expected == null)
                return absent;
            if (absent)
                return false;
            if (actual.isJsonArray()) {
                return StreamSupport.stream(actual.getAsJsonArray().spliterator(), false)
                        .anyMatch(el -> el != null && el.isJsonPrimitive() &&
                                equalsScalar(el, expected, filter.isMatchingCase()));
            }
            return equalsScalar(actual, expected, filter.isMatchingCase());
        }
        if (filter instanceof PropertyIsNotEqualTo) {
            if (expected == null)
                return !absent;
            if (absent)
                return false;
            if (actual.isJsonArray()) {
                return StreamSupport.stream(actual.getAsJsonArray().spliterator(), false)
                        .noneMatch(el -> el != null && el.isJsonPrimitive() &&
                                equalsScalar(el, expected, filter.isMatchingCase()));
            }
            return sameScalarType(actual, expected) &&
                    !equalsScalar(actual, expected, filter.isMatchingCase());
        }
        if (absent || !actual.isJsonPrimitive())
            return false;

        Integer order = orderedScalarComparison(actual, expected);
        if (order == null)
            return false;
        if (filter instanceof PropertyIsGreaterThan)
            return order > 0;
        if (filter instanceof PropertyIsGreaterThanOrEqualTo)
            return order >= 0;
        if (filter instanceof PropertyIsLessThan)
            return order < 0;
        if (filter instanceof PropertyIsLessThanOrEqualTo)
            return order <= 0;
        throw new IllegalArgumentException("Unsupported archived CQL comparison: " + filter.getClass().getName());
    }

    private static Integer orderedScalarComparison(JsonElement actual, Object expected) {
        if (actual.getAsJsonPrimitive().isNumber() &&
                (expected instanceof Number || expected instanceof String)) {
            try {
                return actual.getAsBigDecimal().compareTo(new BigDecimal(expected.toString()));
            } catch (NumberFormatException error) {
                return null;
            }
        }
        if (expected instanceof String expectedText && actual.getAsJsonPrimitive().isString()) {
            Instant left = parseRfc3339Instant(actual.getAsString());
            Instant right = parseRfc3339Instant(expectedText);
            return left != null && right != null ? left.compareTo(right) : null;
        }
        return null;
    }

    private static Instant parseRfc3339Instant(String value) {
        if (!RFC3339_INSTANT.matcher(value).matches())
            return null;
        try {
            Instant instant = Instant.parse(value);
            int utcYear = instant.atOffset(ZoneOffset.UTC).getYear();
            return utcYear >= 1 && utcYear <= 9999 ? instant : null;
        } catch (DateTimeParseException error) {
            return null;
        }
    }

    private static boolean like(PropertyIsLike filter, JsonObject json) {
        if (!(filter.getExpression() instanceof PropertyName property))
            throw new IllegalArgumentException("Archived CQL LIKE requires a property expression");
        JsonElement actual = resolve(json, property.getPropertyName());
        if (actual == null || actual.isJsonNull())
            return false;
        int flags = filter.isMatchingCase() ? 0 : Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE;
        Pattern compiled = Pattern.compile(likeRegex(filter), flags);
        if (actual.isJsonArray()) {
            return StreamSupport.stream(actual.getAsJsonArray().spliterator(), false)
                    .anyMatch(el -> el != null && el.isJsonPrimitive() &&
                            compiled.matcher(el.getAsString()).matches());
        }
        if (!actual.isJsonPrimitive())
            return false;
        return compiled.matcher(actual.getAsString()).matches();
    }

    private static String likeRegex(PropertyIsLike filter) {
        String wildcard = filter.getWildCard();
        String single = filter.getSingleChar();
        String escape = filter.getEscape();
        StringBuilder regex = new StringBuilder("^");
        String literal = filter.getLiteral();
        for (int index = 0; index < literal.length();) {
            if (!escape.isEmpty() && literal.startsWith(escape, index)) {
                index += escape.length();
                if (index < literal.length()) {
                    int point = literal.codePointAt(index);
                    regex.append(Pattern.quote(new String(Character.toChars(point))));
                    index += Character.charCount(point);
                }
            } else if (!wildcard.isEmpty() && literal.startsWith(wildcard, index)) {
                regex.append(".*");
                index += wildcard.length();
            } else if (!single.isEmpty() && literal.startsWith(single, index)) {
                regex.append('.');
                index += single.length();
            } else {
                int point = literal.codePointAt(index);
                regex.append(Pattern.quote(new String(Character.toChars(point))));
                index += Character.charCount(point);
            }
        }
        return regex.append('$').toString();
    }

    private static JsonElement resolve(JsonObject root, String path) {
        JsonElement current = root;
        for (String part : path.split("\\.")) {
            if (current == null || !current.isJsonObject())
                return null;
            current = current.getAsJsonObject().get(part);
        }
        if ((current == null || current.isJsonNull()) && "adjudicatedIdsCount".equals(path)) {
            JsonElement arr = root.get("adjudicatedIds");
            if (arr != null && arr.isJsonArray())
                return new com.google.gson.JsonPrimitive(arr.getAsJsonArray().size());
        }
        return current;
    }

    private static boolean sameScalarType(JsonElement actual, Object expected) {
        if (!actual.isJsonPrimitive())
            return false;
        if (expected instanceof Boolean)
            return actual.getAsJsonPrimitive().isBoolean();
        if (expected instanceof Number)
            return actual.getAsJsonPrimitive().isNumber();
        if (expected instanceof String)
            return actual.getAsJsonPrimitive().isString();
        return true;
    }

    private static boolean equalsScalar(JsonElement actual, Object expected, boolean matchingCase) {
        if (!sameScalarType(actual, expected))
            return false;
        if (expected instanceof Number)
            return actual.getAsBigDecimal().compareTo(new BigDecimal(expected.toString())) == 0;
        if (expected instanceof Boolean)
            return actual.getAsBoolean() == (Boolean) expected;
        String left = actual.getAsString();
        String right = String.valueOf(expected);
        return matchingCase ? left.equals(right) : left.equalsIgnoreCase(right);
    }

    @FunctionalInterface
    private interface CompiledNode {
        boolean evaluate(JsonObject json);
    }

    private enum ComparisonKind {
        EQ, NE, GT, GE, LT, LE
    }

    private static JsonObject parseProjectedJsonObject(String json, Set<String> projectedKeys) {
        if (json == null || projectedKeys == null) {
            JsonElement value = JsonParser.parseString(json);
            return value.isJsonObject() ? value.getAsJsonObject() : null;
        }
        try (JsonReader reader = new JsonReader(new StringReader(json))) {
            if (reader.peek() != JsonToken.BEGIN_OBJECT) {
                JsonElement value = JsonParser.parseString(json);
                return value.isJsonObject() ? value.getAsJsonObject() : null;
            }
            JsonObject projected = new JsonObject();
            reader.beginObject();
            while (reader.hasNext()) {
                String key = reader.nextName();
                if (projectedKeys.contains(key)) {
                    projected.add(key, JsonParser.parseReader(reader));
                } else {
                    reader.skipValue();
                }
            }
            reader.endObject();
            if (reader.peek() != JsonToken.END_DOCUMENT) {
                JsonElement value = JsonParser.parseString(json);
                return value.isJsonObject() ? value.getAsJsonObject() : null;
            }
            return projected;
        } catch (Exception ignored) {
            JsonElement value = JsonParser.parseString(json);
            return value.isJsonObject() ? value.getAsJsonObject() : null;
        }
    }

    private static CompiledNode compileNode(Filter filter, Set<String> topLevelKeys, boolean[] allSupported) {
        if (filter == Filter.INCLUDE)
            return json -> true;
        if (filter == Filter.EXCLUDE)
            return json -> false;
        if (filter instanceof And and) {
            List<CompiledNode> children = new ArrayList<>(and.getChildren().size());
            for (Filter child : and.getChildren())
                children.add(compileNode(child, topLevelKeys, allSupported));
            return json -> {
                for (CompiledNode child : children) {
                    if (!child.evaluate(json))
                        return false;
                }
                return true;
            };
        }
        if (filter instanceof Or or) {
            List<CompiledNode> children = new ArrayList<>(or.getChildren().size());
            for (Filter child : or.getChildren())
                children.add(compileNode(child, topLevelKeys, allSupported));
            return json -> {
                for (CompiledNode child : children) {
                    if (child.evaluate(json))
                        return true;
                }
                return false;
            };
        }
        if (filter instanceof Not not) {
            CompiledNode inner = compileNode(not.getFilter(), topLevelKeys, allSupported);
            return json -> !inner.evaluate(json);
        }
        if (filter instanceof PropertyIsLike like) {
            if (!(like.getExpression() instanceof PropertyName property)) {
                allSupported[0] = false;
                return json -> {
                    throw new IllegalArgumentException("Archived CQL LIKE requires a property expression");
                };
            }
            String path = property.getPropertyName();
            String[] segments = path.split("\\.");
            registerTopLevelKeys(path, segments, topLevelKeys);
            int flags = like.isMatchingCase() ? 0 : Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE;
            Pattern compiled = Pattern.compile(likeRegex(like), flags);
            return json -> {
                JsonElement actual = resolveCompiled(json, path, segments);
                if (actual == null || actual.isJsonNull())
                    return false;
                if (actual.isJsonArray()) {
                    return StreamSupport.stream(actual.getAsJsonArray().spliterator(), false)
                            .anyMatch(el -> el != null && el.isJsonPrimitive() &&
                                    compiled.matcher(el.getAsString()).matches());
                }
                if (!actual.isJsonPrimitive())
                    return false;
                return compiled.matcher(actual.getAsString()).matches();
            };
        }
        if (filter instanceof BinaryComparisonOperator comparison) {
            if (!(comparison.getExpression1() instanceof PropertyName property) ||
                    !(comparison.getExpression2() instanceof Literal literal)) {
                allSupported[0] = false;
                return json -> {
                    throw new IllegalArgumentException("Archived CQL comparisons require property-to-literal expressions");
                };
            }
            ComparisonKind kind;
            if (comparison instanceof PropertyIsEqualTo)
                kind = ComparisonKind.EQ;
            else if (comparison instanceof PropertyIsNotEqualTo)
                kind = ComparisonKind.NE;
            else if (comparison instanceof PropertyIsGreaterThan)
                kind = ComparisonKind.GT;
            else if (comparison instanceof PropertyIsGreaterThanOrEqualTo)
                kind = ComparisonKind.GE;
            else if (comparison instanceof PropertyIsLessThan)
                kind = ComparisonKind.LT;
            else if (comparison instanceof PropertyIsLessThanOrEqualTo)
                kind = ComparisonKind.LE;
            else {
                allSupported[0] = false;
                String className = comparison.getClass().getName();
                return json -> {
                    throw new IllegalArgumentException("Unsupported archived CQL comparison: " + className);
                };
            }
            String path = property.getPropertyName();
            String[] segments = path.split("\\.");
            registerTopLevelKeys(path, segments, topLevelKeys);
            Object expected = literal.getValue();
            boolean matchingCase = comparison.isMatchingCase();
            BigDecimal preParsedDecimal = null;
            if (expected instanceof Number || expected instanceof String) {
                try {
                    preParsedDecimal = new BigDecimal(expected.toString());
                } catch (NumberFormatException ignored) {
                }
            }
            Instant preParsedInstant = expected instanceof String expectedText
                    ? parseRfc3339Instant(expectedText)
                    : null;
            final BigDecimal expectedDecimal = preParsedDecimal;
            final Instant expectedInstant = preParsedInstant;
            return json -> evaluateCompiledComparison(
                    kind, matchingCase, path, segments, expected, expectedDecimal, expectedInstant, json);
        }
        allSupported[0] = false;
        String className = filter.getClass().getName();
        return json -> {
            throw new IllegalArgumentException("Unsupported archived CQL predicate: " + className);
        };
    }

    private static void registerTopLevelKeys(String path, String[] segments, Set<String> topLevelKeys) {
        if (segments.length > 0)
            topLevelKeys.add(segments[0]);
        if ("adjudicatedIdsCount".equals(path))
            topLevelKeys.add("adjudicatedIds");
    }

    private static JsonElement resolveCompiled(JsonObject root, String path, String[] segments) {
        JsonElement current = root;
        for (String part : segments) {
            if (current == null || !current.isJsonObject())
                return null;
            current = current.getAsJsonObject().get(part);
        }
        if ((current == null || current.isJsonNull()) && "adjudicatedIdsCount".equals(path)) {
            JsonElement arr = root.get("adjudicatedIds");
            if (arr != null && arr.isJsonArray())
                return new com.google.gson.JsonPrimitive(arr.getAsJsonArray().size());
        }
        return current;
    }

    private static boolean evaluateCompiledComparison(
            ComparisonKind kind,
            boolean matchingCase,
            String path,
            String[] segments,
            Object expected,
            BigDecimal expectedDecimal,
            Instant expectedInstant,
            JsonObject json) {
        JsonElement actual = resolveCompiled(json, path, segments);
        boolean absent = actual == null || actual instanceof JsonNull || actual.isJsonNull();

        if (kind == ComparisonKind.EQ) {
            if (expected == null)
                return absent;
            if (absent)
                return false;
            if (actual.isJsonArray()) {
                return StreamSupport.stream(actual.getAsJsonArray().spliterator(), false)
                        .anyMatch(el -> el != null && el.isJsonPrimitive() &&
                                equalsCompiledScalar(el, expected, expectedDecimal, matchingCase));
            }
            return equalsCompiledScalar(actual, expected, expectedDecimal, matchingCase);
        }
        if (kind == ComparisonKind.NE) {
            if (expected == null)
                return !absent;
            if (absent)
                return false;
            if (actual.isJsonArray()) {
                return StreamSupport.stream(actual.getAsJsonArray().spliterator(), false)
                        .noneMatch(el -> el != null && el.isJsonPrimitive() &&
                                equalsCompiledScalar(el, expected, expectedDecimal, matchingCase));
            }
            return sameScalarType(actual, expected) &&
                    !equalsCompiledScalar(actual, expected, expectedDecimal, matchingCase);
        }
        if (absent || !actual.isJsonPrimitive())
            return false;

        Integer order = orderedCompiledScalarComparison(actual, expected, expectedDecimal, expectedInstant);
        if (order == null)
            return false;
        return switch (kind) {
            case GT -> order > 0;
            case GE -> order >= 0;
            case LT -> order < 0;
            case LE -> order <= 0;
            default -> false;
        };
    }

    private static boolean equalsCompiledScalar(
            JsonElement actual, Object expected, BigDecimal expectedDecimal, boolean matchingCase) {
        if (!sameScalarType(actual, expected))
            return false;
        if (expected instanceof Number) {
            BigDecimal rhs = expectedDecimal != null ? expectedDecimal : new BigDecimal(expected.toString());
            return actual.getAsBigDecimal().compareTo(rhs) == 0;
        }
        if (expected instanceof Boolean)
            return actual.getAsBoolean() == (Boolean) expected;
        String left = actual.getAsString();
        String right = String.valueOf(expected);
        return matchingCase ? left.equals(right) : left.equalsIgnoreCase(right);
    }

    private static Integer orderedCompiledScalarComparison(
            JsonElement actual, Object expected, BigDecimal expectedDecimal, Instant expectedInstant) {
        if (actual.getAsJsonPrimitive().isNumber() &&
                (expected instanceof Number || expected instanceof String)) {
            if (expectedDecimal == null)
                return null;
            try {
                return actual.getAsBigDecimal().compareTo(expectedDecimal);
            } catch (NumberFormatException error) {
                return null;
            }
        }
        if (expectedInstant != null && actual.getAsJsonPrimitive().isString()) {
            Instant left = parseRfc3339Instant(actual.getAsString());
            return left != null ? left.compareTo(expectedInstant) : null;
        }
        return null;
    }

    /**
     * Returns a deterministic, collision-free canonical key for supported CQL
     * predicates (preserving operator kind, case-sensitivity, wildcard/escape
     * settings, property path, and scalar literal type/value), or empty when the
     * predicate uses unsupported AST nodes.
     */
    public static Optional<String> canonicalCacheKey(Filter filter) {
        if (filter == null || filter == Filter.INCLUDE)
            return Optional.of("INCLUDE");
        if (filter == Filter.EXCLUDE)
            return Optional.of("EXCLUDE");
        StringBuilder out = new StringBuilder(128);
        return appendCanonicalKey(filter, out) ? Optional.of(out.toString()) : Optional.empty();
    }

    private static boolean appendCanonicalKey(Filter filter, StringBuilder out) {
        if (filter == Filter.INCLUDE) {
            out.append("INCLUDE");
            return true;
        }
        if (filter == Filter.EXCLUDE) {
            out.append("EXCLUDE");
            return true;
        }
        if (filter instanceof And and) {
            out.append("AND(").append(and.getChildren().size()).append(':');
            for (int i = 0; i < and.getChildren().size(); i++) {
                if (i > 0)
                    out.append(',');
                if (!appendCanonicalKey(and.getChildren().get(i), out))
                    return false;
            }
            out.append(')');
            return true;
        }
        if (filter instanceof Or or) {
            out.append("OR(").append(or.getChildren().size()).append(':');
            for (int i = 0; i < or.getChildren().size(); i++) {
                if (i > 0)
                    out.append(',');
                if (!appendCanonicalKey(or.getChildren().get(i), out))
                    return false;
            }
            out.append(')');
            return true;
        }
        if (filter instanceof Not not) {
            out.append("NOT(");
            if (!appendCanonicalKey(not.getFilter(), out))
                return false;
            out.append(')');
            return true;
        }
        if (filter instanceof PropertyIsLike like) {
            if (!(like.getExpression() instanceof PropertyName prop))
                return false;
            out.append("LIKE(").append(like.isMatchingCase()).append('|');
            appendLengthPrefixed(out, like.getWildCard());
            appendLengthPrefixed(out, like.getSingleChar());
            appendLengthPrefixed(out, like.getEscape());
            appendLengthPrefixed(out, prop.getPropertyName());
            appendLengthPrefixed(out, like.getLiteral());
            out.append(')');
            return true;
        }
        if (filter instanceof BinaryComparisonOperator cmp) {
            if (!(cmp.getExpression1() instanceof PropertyName prop) ||
                    !(cmp.getExpression2() instanceof Literal lit))
                return false;
            String op;
            if (cmp instanceof PropertyIsEqualTo)
                op = "EQ";
            else if (cmp instanceof PropertyIsNotEqualTo)
                op = "NE";
            else if (cmp instanceof PropertyIsGreaterThan)
                op = "GT";
            else if (cmp instanceof PropertyIsGreaterThanOrEqualTo)
                op = "GE";
            else if (cmp instanceof PropertyIsLessThan)
                op = "LT";
            else if (cmp instanceof PropertyIsLessThanOrEqualTo)
                op = "LE";
            else
                return false;
            out.append(op).append('(').append(cmp.isMatchingCase()).append('|');
            appendLengthPrefixed(out, prop.getPropertyName());
            Object val = lit.getValue();
            if (val == null) {
                out.append("NULL");
            } else if (val instanceof Boolean b) {
                out.append("B:").append(b);
            } else if (val instanceof Number n) {
                try {
                    out.append("N:").append(new BigDecimal(n.toString()).stripTrailingZeros().toPlainString());
                } catch (NumberFormatException ignored) {
                    return false;
                }
            } else if (val instanceof String s) {
                out.append("S:");
                appendLengthPrefixed(out, s);
            } else {
                return false;
            }
            out.append(')');
            return true;
        }
        return false;
    }

    private static void appendLengthPrefixed(StringBuilder out, String text) {
        if (text == null) {
            out.append("-1#");
        } else {
            out.append(text.length()).append('#').append(text);
        }
    }

    public static final int BIT_GAMMA_TRUE = 1;
    public static final int BIT_GAMMA_FALSE = 1 << 1;
    public static final int BIT_NEUTRON_TRUE = 1 << 2;
    public static final int BIT_NEUTRON_FALSE = 1 << 3;
    public static final int BIT_ADJ_ZERO = 1 << 4;
    public static final int BIT_ADJ_GT_ZERO = 1 << 5;
    public static final int BIT_FAULT_GAMMA_HIGH = 1 << 6;
    public static final int BIT_FAULT_GAMMA_LOW = 1 << 7;
    public static final int BIT_FAULT_NEUTRON = 1 << 8;
    public static final int BIT_FAULT_EXT_OCC = 1 << 9;
    public static final int BIT_TAMPER_TRUE = 1 << 10;
    public static final int BIT_RS350_GAMMA = 1 << 11;
    public static final int BIT_RS350_NEUTRON = 1 << 12;
    public static final int BIT_FAULT_NEUTRON_HIGH = 1 << 13;
    public static final int BIT_ALARM_CAT_GAMMA = 1 << 14;
    public static final int BIT_ALARM_CAT_NEUTRON = 1 << 15;
    /** Marker bit set on masks computed with v2 extended fault/alarm-category bits (bits 13..15). */
    public static final int BIT_MASK_V2 = 1 << 30;
    private static final int V2_SUMMARY_BITS_MASK =
            BIT_FAULT_NEUTRON_HIGH | BIT_ALARM_CAT_GAMMA | BIT_ALARM_CAT_NEUTRON;

    private static final Set<String> SUMMARY_MASK_PROJECTED_KEYS = Set.of(
            "gammaAlarm", "neutronAlarm", "adjudicatedIdsCount", "adjudicatedIds",
            "alarmState", "tamperStatus", "derivedData", "alarmCategoryCode");
    private static final String[] SEG_GAMMA_ALARM = {"gammaAlarm"};
    private static final String[] SEG_NEUTRON_ALARM = {"neutronAlarm"};
    private static final String[] SEG_ADJ_COUNT = {"adjudicatedIdsCount"};
    private static final String[] SEG_ALARM_STATE = {"alarmState"};
    private static final String[] SEG_TAMPER_STATUS = {"tamperStatus"};
    private static final String[] SEG_ALARM_CATEGORY = {"derivedData", "alarmCategory"};
    private static final String[] SEG_ALARM_CATEGORY_CODE = {"alarmCategoryCode"};
    private static final Pattern RS350_GAMMA_PATTERN = Pattern.compile(".*Gamma.*", Pattern.DOTALL);
    private static final Pattern RS350_NEUTRON_PATTERN = Pattern.compile(".*Neutron.*", Pattern.DOTALL);

    /**
     * Computes a compact 4-byte non-negative bitmask capturing all atomic
     * summary-counter predicates over an immutable cold base observation's JSON,
     * allowing PostgreSQL overlay/tombstone rows to answer summary inclusion-exclusion
     * counts without duplicating cold JSONB payloads or touching Parquet files.
     */
    public static int computeSummaryMask(String resultJson) {
        if (resultJson == null || resultJson.isBlank())
            return BIT_MASK_V2;
        JsonObject obj = parseProjectedJsonObject(resultJson, SUMMARY_MASK_PROJECTED_KEYS);
        if (obj == null)
            return BIT_MASK_V2;
        int mask = BIT_MASK_V2;
        if (evaluateCompiledComparison(ComparisonKind.EQ, true, "gammaAlarm", SEG_GAMMA_ALARM, Boolean.TRUE, null, null, obj))
            mask |= BIT_GAMMA_TRUE;
        if (evaluateCompiledComparison(ComparisonKind.EQ, true, "gammaAlarm", SEG_GAMMA_ALARM, Boolean.FALSE, null, null, obj))
            mask |= BIT_GAMMA_FALSE;
        if (evaluateCompiledComparison(ComparisonKind.EQ, true, "neutronAlarm", SEG_NEUTRON_ALARM, Boolean.TRUE, null, null, obj))
            mask |= BIT_NEUTRON_TRUE;
        if (evaluateCompiledComparison(ComparisonKind.EQ, true, "neutronAlarm", SEG_NEUTRON_ALARM, Boolean.FALSE, null, null, obj))
            mask |= BIT_NEUTRON_FALSE;
        if (evaluateCompiledComparison(ComparisonKind.EQ, true, "adjudicatedIdsCount", SEG_ADJ_COUNT, 0L, BigDecimal.ZERO, null, obj))
            mask |= BIT_ADJ_ZERO;
        if (evaluateCompiledComparison(ComparisonKind.GT, true, "adjudicatedIdsCount", SEG_ADJ_COUNT, 0L, BigDecimal.ZERO, null, obj))
            mask |= BIT_ADJ_GT_ZERO;
        if (evaluateCompiledComparison(ComparisonKind.EQ, true, "alarmState", SEG_ALARM_STATE, "Fault - Gamma High", null, null, obj))
            mask |= BIT_FAULT_GAMMA_HIGH;
        if (evaluateCompiledComparison(ComparisonKind.EQ, true, "alarmState", SEG_ALARM_STATE, "Fault - Gamma Low", null, null, obj))
            mask |= BIT_FAULT_GAMMA_LOW;
        if (evaluateCompiledComparison(ComparisonKind.EQ, true, "alarmState", SEG_ALARM_STATE, "Fault - Neutron", null, null, obj))
            mask |= BIT_FAULT_NEUTRON;
        if (evaluateCompiledComparison(ComparisonKind.EQ, true, "alarmState", SEG_ALARM_STATE, "Fault - Neutron High", null, null, obj))
            mask |= BIT_FAULT_NEUTRON_HIGH;
        if (evaluateCompiledComparison(ComparisonKind.EQ, true, "alarmState", SEG_ALARM_STATE, "Fault - Extended Occupancy", null, null, obj))
            mask |= BIT_FAULT_EXT_OCC;
        if (evaluateCompiledComparison(ComparisonKind.EQ, true, "tamperStatus", SEG_TAMPER_STATUS, Boolean.TRUE, null, null, obj))
            mask |= BIT_TAMPER_TRUE;
        if (matchesCompiledLike(obj, "derivedData.alarmCategory", SEG_ALARM_CATEGORY, RS350_GAMMA_PATTERN))
            mask |= BIT_RS350_GAMMA;
        if (matchesCompiledLike(obj, "derivedData.alarmCategory", SEG_ALARM_CATEGORY, RS350_NEUTRON_PATTERN))
            mask |= BIT_RS350_NEUTRON;
        if (evaluateCompiledComparison(ComparisonKind.EQ, true, "alarmCategoryCode", SEG_ALARM_CATEGORY_CODE, "Gamma", null, null, obj))
            mask |= BIT_ALARM_CAT_GAMMA;
        if (evaluateCompiledComparison(ComparisonKind.EQ, true, "alarmCategoryCode", SEG_ALARM_CATEGORY_CODE, "Neutron", null, null, obj))
            mask |= BIT_ALARM_CAT_NEUTRON;
        return mask;
    }

    private static boolean matchesCompiledLike(JsonObject json, String path, String[] segments, Pattern compiled) {
        JsonElement actual = resolveCompiled(json, path, segments);
        if (actual == null || actual.isJsonNull())
            return false;
        if (actual.isJsonArray()) {
            return StreamSupport.stream(actual.getAsJsonArray().spliterator(), false)
                    .anyMatch(el -> el != null && el.isJsonPrimitive() &&
                            compiled.matcher(el.getAsString()).matches());
        }
        if (!actual.isJsonPrimitive())
            return false;
        return compiled.matcher(actual.getAsString()).matches();
    }

    /**
     * Evaluates {@code filter} directly against a precomputed {@link #computeSummaryMask(String)}
     * when {@code filter} is composed solely of summary-mask predicates, or returns
     * {@link Optional#empty()} when full JSON evaluation is required.
     */
    public static Optional<Boolean> tryEvaluateSummaryMask(Filter filter, int mask) {
        if (filter == null || filter == Filter.INCLUDE)
            return Optional.of(Boolean.TRUE);
        if (filter == Filter.EXCLUDE)
            return Optional.of(Boolean.FALSE);
        if (filter instanceof And and) {
            if (and.getChildren().isEmpty())
                return Optional.empty();
            boolean allTrue = true;
            for (Filter child : and.getChildren()) {
                Optional<Boolean> sub = tryEvaluateSummaryMask(child, mask);
                if (sub.isEmpty())
                    return Optional.empty();
                if (!sub.get())
                    allTrue = false;
            }
            return Optional.of(allTrue);
        }
        if (filter instanceof Or or) {
            if (or.getChildren().isEmpty())
                return Optional.empty();
            boolean anyTrue = false;
            for (Filter child : or.getChildren()) {
                Optional<Boolean> sub = tryEvaluateSummaryMask(child, mask);
                if (sub.isEmpty())
                    return Optional.empty();
                if (sub.get())
                    anyTrue = true;
            }
            return Optional.of(anyTrue);
        }
        int bit = leafSummaryBit(filter);
        if (bit == 0)
            return Optional.empty();
        if ((bit & V2_SUMMARY_BITS_MASK) != 0 && (mask & BIT_MASK_V2) == 0)
            return Optional.empty();
        return Optional.of((mask & bit) != 0);
    }

    /**
     * Checks whether {@code filter} can be evaluated solely from a 4-byte
     * {@link #computeSummaryMask(String)} without reading JSON payloads.
     */
    public static boolean supportsSummaryMaskEvaluation(Filter filter) {
        return tryEvaluateSummaryMask(filter, BIT_MASK_V2).isPresent();
    }

    /**
     * Returns {@code true} when any predicate in {@code conjuncts} requires
     * v2 extended summary-mask bits (marked by {@link #BIT_MASK_V2}).
     */
    public static boolean requiresSummaryMaskV2(List<Filter> conjuncts) {
        if (conjuncts == null || conjuncts.isEmpty())
            return false;
        for (Filter filter : conjuncts) {
            if (requiresSummaryMaskV2(filter))
                return true;
        }
        return false;
    }

    private static boolean requiresSummaryMaskV2(Filter filter) {
        if (filter == null || filter == Filter.INCLUDE || filter == Filter.EXCLUDE)
            return false;
        if (filter instanceof And and) {
            for (Filter child : and.getChildren()) {
                if (requiresSummaryMaskV2(child))
                    return true;
            }
            return false;
        }
        if (filter instanceof Or or) {
            for (Filter child : or.getChildren()) {
                if (requiresSummaryMaskV2(child))
                    return true;
            }
            return false;
        }
        return (leafSummaryBit(filter) & V2_SUMMARY_BITS_MASK) != 0;
    }

    /**
     * Extracts canonical summary-mask conjuncts for {@code filter} when {@code filter}
     * is directly evaluable via {@link #tryEvaluateSummaryMask(Filter, int)}, or when
     * {@code timeContainedEligible} is {@code true} and {@link #timeContainedSummarySpec(Filter)}
     * proves that {@code startTime}/{@code endTime}/{@code samplingTime} range bounds are
     * already satisfied for the target time-contained generations. Returns {@code null}
     * when full JSON payload evaluation is required.
     */
    public static List<Filter> extractSummaryMaskConjuncts(Filter filter, boolean timeContainedEligible) {
        if (filter == null || filter == Filter.INCLUDE)
            return List.of();
        if (supportsSummaryMaskEvaluation(filter)) {
            List<Filter> flat = new ArrayList<>();
            flattenAndChildren(filter, flat);
            return List.copyOf(flat);
        }
        if (!timeContainedEligible || timeContainedSummarySpec(filter).isEmpty())
            return null;
        List<Filter> conjuncts = new ArrayList<>();
        flattenAndChildren(filter, conjuncts);
        List<Filter> nonTimeConjuncts = new ArrayList<>();
        for (Filter child : conjuncts) {
            if ((child instanceof PropertyIsLessThan ||
                    child instanceof PropertyIsLessThanOrEqualTo ||
                    child instanceof PropertyIsGreaterThan ||
                    child instanceof PropertyIsGreaterThanOrEqualTo) &&
                    ((BinaryComparisonOperator) child).getExpression1() instanceof PropertyName prop &&
                    ((BinaryComparisonOperator) child).getExpression2() instanceof Literal lit &&
                    lit.getValue() instanceof String) {
                String name = prop.getPropertyName();
                if ("startTime".equals(name) || "endTime".equals(name) || "samplingTime".equals(name))
                    continue;
            }
            if (!supportsSummaryMaskEvaluation(child))
                return null;
            nonTimeConjuncts.add(child);
        }
        return List.copyOf(nonTimeConjuncts);
    }

    public static Optional<Boolean> tryEvaluateSummaryMaskConjuncts(List<Filter> conjuncts, int mask) {
        if (conjuncts == null)
            return Optional.empty();
        boolean allTrue = true;
        for (Filter child : conjuncts) {
            Optional<Boolean> sub = tryEvaluateSummaryMask(child, mask);
            if (sub.isEmpty())
                return Optional.empty();
            if (!sub.get())
                allTrue = false;
        }
        return Optional.of(allTrue);
    }

    private static int leafSummaryBit(Filter filter) {
        if (filter instanceof PropertyIsEqualTo eq) {
            if (!(eq.getExpression1() instanceof PropertyName prop) ||
                    !(eq.getExpression2() instanceof Literal lit) ||
                    lit.getValue() == null)
                return 0;
            String name = prop.getPropertyName();
            Object val = lit.getValue();
            if (val instanceof Boolean boolVal) {
                if ("gammaAlarm".equals(name))
                    return boolVal ? BIT_GAMMA_TRUE : BIT_GAMMA_FALSE;
                if ("neutronAlarm".equals(name))
                    return boolVal ? BIT_NEUTRON_TRUE : BIT_NEUTRON_FALSE;
                if ("tamperStatus".equals(name) && boolVal)
                    return BIT_TAMPER_TRUE;
                return 0;
            }
            if ("adjudicatedIdsCount".equals(name) && val instanceof Number && isZeroNumber(val))
                return BIT_ADJ_ZERO;
            if (eq.isMatchingCase() && "alarmState".equals(name) && val instanceof String text) {
                return switch (text) {
                    case "Fault - Gamma High" -> BIT_FAULT_GAMMA_HIGH;
                    case "Fault - Gamma Low" -> BIT_FAULT_GAMMA_LOW;
                    case "Fault - Neutron" -> BIT_FAULT_NEUTRON;
                    case "Fault - Neutron High" -> BIT_FAULT_NEUTRON_HIGH;
                    case "Fault - Extended Occupancy" -> BIT_FAULT_EXT_OCC;
                    default -> 0;
                };
            }
            if (eq.isMatchingCase() && "alarmCategoryCode".equals(name) && val instanceof String text) {
                return switch (text) {
                    case "Gamma" -> BIT_ALARM_CAT_GAMMA;
                    case "Neutron" -> BIT_ALARM_CAT_NEUTRON;
                    default -> 0;
                };
            }
            return 0;
        }
        if (filter instanceof PropertyIsGreaterThan gt &&
                gt.getExpression1() instanceof PropertyName prop &&
                gt.getExpression2() instanceof Literal lit &&
                "adjudicatedIdsCount".equals(prop.getPropertyName()) &&
                (lit.getValue() instanceof Number || lit.getValue() instanceof String) &&
                isZeroNumber(lit.getValue())) {
            return BIT_ADJ_GT_ZERO;
        }
        if (filter instanceof PropertyIsLike like &&
                like.isMatchingCase() &&
                "%".equals(like.getWildCard()) &&
                "_".equals(like.getSingleChar()) &&
                "\\".equals(like.getEscape()) &&
                like.getExpression() instanceof PropertyName prop &&
                "derivedData.alarmCategory".equals(prop.getPropertyName())) {
            if ("%Gamma%".equals(like.getLiteral()))
                return BIT_RS350_GAMMA;
            if ("%Neutron%".equals(like.getLiteral()))
                return BIT_RS350_NEUTRON;
        }
        return 0;
    }

    /**
     * Conservatively proves whether boolean algebra over {@code filter} and
     * {@code summary} strictly guarantees zero matching base rows in the
     * generation without opening DuckDB. Never infers zero matches from inside
     * {@link Not} or {@link PropertyIsNotEqualTo} predicates or unpopulated
     * summaries.
     */
    public static boolean isGuaranteedZeroMatchBySummary(
            Filter filter, ParquetArchiveReader.GenerationSummary summary) {
        if (filter == Filter.EXCLUDE)
            return true;
        if (summary == null || !summary.populated())
            return false;
        if (summary.rowCount() <= 0L)
            return true;
        return isZeroMatchNode(filter, summary);
    }

    private static boolean isZeroMatchNode(
            Filter filter, ParquetArchiveReader.GenerationSummary summary) {
        if (filter == Filter.EXCLUDE)
            return true;
        if (filter == null || filter == Filter.INCLUDE)
            return summary.rowCount() <= 0L;
        if (filter instanceof Or or) {
            if (or.getChildren().isEmpty())
                return false;
            for (Filter child : or.getChildren()) {
                if (!isZeroMatchNode(child, summary))
                    return false;
            }
            return true;
        }
        if (filter instanceof And and) {
            List<Filter> conjuncts = new ArrayList<>();
            flattenAndChildren(and, conjuncts);
            if (conjuncts.isEmpty())
                return false;
            Boolean gamma = null;
            Boolean neutron = null;
            for (Filter child : conjuncts) {
                if (isZeroMatchNode(child, summary))
                    return true;
                if (child instanceof PropertyIsEqualTo eq &&
                        eq.getExpression1() instanceof PropertyName prop &&
                        eq.getExpression2() instanceof Literal lit &&
                        lit.getValue() instanceof Boolean boolVal) {
                    String name = prop.getPropertyName();
                    if ("gammaAlarm".equals(name)) {
                        if (gamma != null && !gamma.equals(boolVal))
                            return true;
                        gamma = boolVal;
                    } else if ("neutronAlarm".equals(name)) {
                        if (neutron != null && !neutron.equals(boolVal))
                            return true;
                        neutron = boolVal;
                    }
                }
            }
            if (gamma != null && neutron != null) {
                if (gamma && !neutron)
                    return summary.gammaAlarmCount() <= 0L;
                if (!gamma && neutron)
                    return summary.neutronAlarmCount() <= 0L;
                if (gamma)
                    return summary.gammaNeutronAlarmCount() <= 0L;
                return false;
            }
            if (Boolean.TRUE.equals(gamma))
                return summary.gammaTrueCount() <= 0L;
            if (Boolean.TRUE.equals(neutron))
                return summary.neutronTrueCount() <= 0L;
            return false;
        }
        if (filter instanceof PropertyIsEqualTo eq &&
                eq.getExpression1() instanceof PropertyName prop &&
                eq.getExpression2() instanceof Literal lit &&
                Boolean.TRUE.equals(lit.getValue())) {
            String name = prop.getPropertyName();
            if ("gammaAlarm".equals(name))
                return summary.gammaTrueCount() <= 0L;
            if ("neutronAlarm".equals(name))
                return summary.neutronTrueCount() <= 0L;
        }
        return false;
    }
}
