package org.sensorhub.impl.datastore.postgis.builder.filter;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
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

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
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
     * Appends a sound (zero-false-negative) DuckDB SQL prefilter over {@code column} when possible.
     * Callers must still run {@link #matches(Filter, String)} on surviving candidate rows.
     */
    public static boolean appendDuckDbPrefilter(
            Filter filter, String column, StringBuilder sql, List<Object> parameters) {
        PrefilterClause clause = buildDuckDbPrefilter(filter, column);
        if (clause == null)
            return false;
        sql.append(" AND (").append(clause.sql()).append(')');
        parameters.addAll(clause.parameters());
        return true;
    }

    public static boolean hasSelectiveDuckDbPrefilter(Filter filter) {
        PrefilterClause clause = buildDuckDbPrefilter(filter, "result_json");
        return clause != null && clause.selective();
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
                    StringBuilder expr = new StringBuilder("(");
                    if (leafKey != null) {
                        expr.append("strpos(").append(column).append(", ?) > 0 AND ");
                        params.add(leafKey);
                    }
                    if (comparison.isMatchingCase()) {
                        expr.append("strpos(").append(column).append(", ?) > 0");
                        params.add(text);
                    } else {
                        expr.append("strpos(lower(").append(column).append("), ?) > 0");
                        params.add(text.toLowerCase(Locale.ROOT));
                    }
                    expr.append(") OR strpos(").append(column).append(", '\\u') > 0");
                    return new PrefilterClause(expr.toString(), params, true);
                }
                if (expected instanceof Boolean boolVal) {
                    List<Object> params = new ArrayList<>();
                    StringBuilder expr = new StringBuilder();
                    if (leafKey != null) {
                        expr.append("(strpos(").append(column).append(", ?) > 0 OR strpos(")
                                .append(column).append(", '\\u') > 0) AND ");
                        params.add(leafKey);
                    }
                    expr.append("strpos(lower(").append(column).append("), ?) > 0");
                    params.add(boolVal ? "true" : "false");
                    return new PrefilterClause(expr.toString(), params, false);
                }
            }
            return leafKey == null ? null : leafKeyPrefilter(property.getPropertyName(), column);
        }
        return null;
    }

    private static PrefilterClause leafKeyPrefilter(String propertyPath, String column) {
        String leafKey = extractSafeLeafKey(propertyPath);
        if (leafKey == null)
            return null;
        return new PrefilterClause(
                "strpos(" + column + ", ?) > 0 OR strpos(" + column + ", '\\u') > 0",
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
}
