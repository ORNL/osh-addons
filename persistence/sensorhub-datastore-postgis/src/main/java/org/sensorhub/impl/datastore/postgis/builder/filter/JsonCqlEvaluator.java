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

import java.io.StringReader;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

/** Evaluates the supported OSCAR CQL subset directly against archived JSON. */
public final class JsonCqlEvaluator {
    private static final Pattern SAFE_JSON_KEY = Pattern.compile("^[A-Za-z0-9_-]+$");
    private static final Pattern SAFE_JSON_STRING_LITERAL = Pattern.compile("^[A-Za-z0-9 _.:-]+$");
    private static final Pattern RFC3339_INSTANT = Pattern.compile(
            "^[0-9]{4}-(0[1-9]|1[0-2])-([0-2][0-9]|3[01])T" +
            "([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](\\.[0-9]{1,9})?" +
            "(Z|[+-]((0[0-9]|1[0-7]):[0-5][0-9]|18:00))$");

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
        PrefilterClause clause = buildDuckDbPrefilter(filter, column);
        if (clause == null)
            return false;
        if ("FALSE".equals(clause.sql())) {
            sql.append(" AND FALSE");
        } else {
            sql.append(" AND ((").append(clause.sql())
                    .append(") OR strpos(").append(column).append(", '\\u') > 0)");
        }
        parameters.addAll(clause.parameters());
        return true;
    }

    /**
     * Reject only explicit nonzero numeric adjudication counts. Arrays, missing
     * fields, nulls and other types remain candidates for exact Java evaluation.
     * The caller supplies a trusted PostgreSQL jsonb column expression.
     */
    public static boolean appendPostgresOverlayPrefilter(
            Filter filter, String column, StringBuilder sql) {
        if (!isPostgresOverlayPrefilterSafe(filter) || !requiresUnadjudicated(filter))
            return false;
        sql.append(" AND NOT COALESCE((jsonb_typeof(").append(column)
                .append("->'adjudicatedIdsCount') = 'number' AND ")
                .append(column).append("->'adjudicatedIdsCount' <> '0'::jsonb), false)");
        return true;
    }

    private static boolean requiresUnadjudicated(Filter filter) {
        if (filter instanceof And and)
            return and.getChildren().stream().anyMatch(JsonCqlEvaluator::requiresUnadjudicated);
        if (filter instanceof Or or)
            return !or.getChildren().isEmpty() &&
                    or.getChildren().stream().allMatch(JsonCqlEvaluator::requiresUnadjudicated);
        return filter instanceof PropertyIsEqualTo eq &&
                eq.getExpression1() instanceof PropertyName property &&
                "adjudicatedIdsCount".equals(property.getPropertyName()) &&
                eq.getExpression2() instanceof Literal literal && isExactNumericZero(literal.getValue());
    }

    private static boolean isPostgresOverlayPrefilterSafe(Filter filter) {
        if (filter == Filter.EXCLUDE)
            return true;
        if (filter instanceof And and)
            return !and.getChildren().isEmpty() &&
                    and.getChildren().stream().allMatch(JsonCqlEvaluator::isPostgresOverlayPrefilterSafe);
        if (filter instanceof Or or)
            return !or.getChildren().isEmpty() &&
                    or.getChildren().stream().allMatch(JsonCqlEvaluator::isPostgresOverlayPrefilterSafe);
        if (!(filter instanceof PropertyIsEqualTo eq) ||
                !(eq.getExpression1() instanceof PropertyName property) ||
                !(eq.getExpression2() instanceof Literal literal))
            return false;
        String name = property.getPropertyName();
        Object value = literal.getValue();
        if (("gammaAlarm".equals(name) || "neutronAlarm".equals(name)) && value instanceof Boolean)
            return true;
        return "adjudicatedIdsCount".equals(name) && isExactNumericZero(value);
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

    public static Optional<SummaryCounterSpec> summaryCounterSpec(Filter filter) {
        if (filter == null || filter == Filter.INCLUDE) {
            String expr = "COALESCE(s.row_count, 0)";
            return Optional.of(new SummaryCounterSpec(expr, expr, "s.summary_populated"));
        }
        if (filter == Filter.EXCLUDE) {
            return Optional.of(new SummaryCounterSpec("0::bigint", "0::bigint", "true"));
        }
        if (filter instanceof PropertyIsEqualTo eq) {
            return singleEqualitySummaryCounter(eq)
                    .map(expr -> new SummaryCounterSpec(expr, expr, "s.summary_populated"));
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
            Set<String> faultColumns = new LinkedHashSet<>();
            for (Filter child : or.getChildren()) {
                if (!(child instanceof PropertyIsEqualTo eq)) {
                    faultColumns.clear();
                    break;
                }
                Optional<String> col = singleFaultSummaryColumn(eq);
                if (col.isEmpty()) {
                    faultColumns.clear();
                    break;
                }
                faultColumns.add(col.get());
            }
            if (!faultColumns.isEmpty()) {
                String expr = "(" + String.join(" + ", faultColumns) + ")";
                return Optional.of(new SummaryCounterSpec(expr, expr, "s.summary_populated"));
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

    private static Optional<SummaryCounterSpec> andSummaryCounterSpec(And rootAnd) {
        List<Filter> conjuncts = new ArrayList<>();
        flattenAndChildren(rootAnd, conjuncts);
        if (conjuncts.isEmpty())
            return Optional.empty();

        Boolean gamma = null;
        Boolean neutron = null;
        boolean requireAnyAlarm = false;
        Boolean adjudicated = null; // false = unadjudicated (count=0), true = adjudicated (count>0)
        boolean hasExtraConjuncts = false;

        for (Filter child : conjuncts) {
            if (child instanceof Or or && isAnyAlarmOr(or)) {
                requireAnyAlarm = true;
                continue;
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

        Optional<SummaryCounterSpec> baseSpec = resolveBaseAndSummarySpec(gamma, neutron, requireAnyAlarm, adjudicated);
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
                        Instant safeStart = parsed.plus(safetyMargin);
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
        for (Filter child : conjuncts) {
            if (child instanceof Or or && isAnyAlarmOr(or)) {
                requireAnyAlarm = true;
                continue;
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
                return Optional.empty();
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

    private static Optional<String> singleEqualitySummaryCounter(PropertyIsEqualTo eq) {
        if (!(eq.getExpression1() instanceof PropertyName prop) ||
                !(eq.getExpression2() instanceof Literal lit) ||
                lit.getValue() == null)
            return Optional.empty();
        String name = prop.getPropertyName();
        Object val = lit.getValue();
        if (val instanceof Boolean boolVal) {
            if ("gammaAlarm".equals(name) && boolVal)
                return Optional.of("COALESCE(s.gamma_true_count, 0)");
            if ("neutronAlarm".equals(name) && boolVal)
                return Optional.of("COALESCE(s.neutron_true_count, 0)");
            if ("tamperStatus".equals(name) && boolVal)
                return Optional.of("COALESCE(s.tamper_true_count, 0)");
            return Optional.empty();
        }
        return singleFaultSummaryColumn(eq);
    }

    private static Optional<String> singleFaultSummaryColumn(PropertyIsEqualTo eq) {
        if (!eq.isMatchingCase() ||
                !(eq.getExpression1() instanceof PropertyName prop) ||
                !(eq.getExpression2() instanceof Literal lit) ||
                !(lit.getValue() instanceof String text))
            return Optional.empty();
        if (!"alarmState".equals(prop.getPropertyName()))
            return Optional.empty();
        return switch (text) {
            case "Fault - Gamma High" -> Optional.of("COALESCE(s.gamma_high_fault_count, 0)");
            case "Fault - Gamma Low" -> Optional.of("COALESCE(s.gamma_low_fault_count, 0)");
            case "Fault - Neutron" -> Optional.of("COALESCE(s.neutron_fault_count, 0)");
            case "Fault - Extended Occupancy" -> Optional.of("COALESCE(s.extended_occupancy_fault_count, 0)");
            default -> Optional.empty();
        };
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
            if (like.getExpression() instanceof PropertyName property)
                return leafKeyPrefilter(property.getPropertyName(), column);
            return null;
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
            }
            return leafKey == null ? null : leafKeyPrefilter(property.getPropertyName(), column);
        }
        return null;
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
}
