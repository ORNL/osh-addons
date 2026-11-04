package org.sensorhub.impl.datastore.postgis.builder.filter;

import org.geotools.api.filter.Filter;
import org.geotools.api.filter.PropertyIsEqualTo;
import org.geotools.api.filter.PropertyIsLike;
import org.geotools.api.filter.PropertyIsGreaterThan;
import org.geotools.api.filter.PropertyIsLessThan;
import org.geotools.api.filter.PropertyIsGreaterThanOrEqualTo;
import org.geotools.api.filter.PropertyIsLessThanOrEqualTo;
import org.geotools.api.filter.PropertyIsNotEqualTo;
import org.geotools.api.filter.And;
import org.geotools.api.filter.Or;
import org.geotools.api.filter.Not;
import org.geotools.api.filter.expression.PropertyName;
import org.geotools.api.filter.expression.Literal;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

public class CQLFilterHandler {
    private static final String RFC3339_INSTANT_PATTERN =
            "^[0-9]{4}-(0[1-9]|1[0-2])-([0-2][0-9]|3[01])T" +
            "([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](\\.[0-9]{1,9})?" +
            "(Z|[+-]((0[0-9]|1[0-7]):[0-5][0-9]|18:00))$";

    private StringBuilder whereClause;
    private List<Object> parameters;

    public CQLFilterHandler() {
        this.whereClause = new StringBuilder();
        this.parameters = new ArrayList<>();
    }

    public String buildWhereClause(Filter filter) {
        whereClause.setLength(0);
        parameters.clear();

        if (filter != null && filter != Filter.INCLUDE) {
            processFilter(filter);
        }

        return whereClause.toString();
    }

    public List<Object> getParameters() {
        return List.copyOf(parameters);
    }

    private void processFilter(Filter filter) {
        if (filter == Filter.INCLUDE) {
            whereClause.append("TRUE");
        } else if (filter == Filter.EXCLUDE) {
            whereClause.append("FALSE");
        } else if (filter instanceof And) {
            handleAnd((And) filter);
        } else if (filter instanceof Or) {
            handleOr((Or) filter);
        } else if (filter instanceof Not) {
            handleNot((Not) filter);
        } else if (filter instanceof PropertyIsEqualTo) {
            handleEquals((PropertyIsEqualTo) filter);
        } else if (filter instanceof PropertyIsNotEqualTo) {
            handleNotEquals((PropertyIsNotEqualTo) filter);
        } else if (filter instanceof PropertyIsGreaterThan) {
            handleGreaterThan((PropertyIsGreaterThan) filter);
        } else if (filter instanceof PropertyIsGreaterThanOrEqualTo) {
            handleGreaterThanOrEqual((PropertyIsGreaterThanOrEqualTo) filter);
        } else if (filter instanceof PropertyIsLessThan) {
            handleLessThan((PropertyIsLessThan) filter);
        } else if (filter instanceof PropertyIsLessThanOrEqualTo) {
            handleLessThanOrEqual((PropertyIsLessThanOrEqualTo) filter);
        } else if (filter instanceof PropertyIsLike) {
            handleLike((PropertyIsLike) filter);
        }
    }

    private void handleAnd(And filter) {
        whereClause.append("(");
        List<Filter> children = filter.getChildren();
        for (int i = 0; i < children.size(); i++) {
            if (i > 0) {
                whereClause.append(" AND ");
            }
            processFilter(children.get(i));
        }
        whereClause.append(")");
    }

    private void handleOr(Or filter) {
        whereClause.append("(");
        List<Filter> children = filter.getChildren();
        for (int i = 0; i < children.size(); i++) {
            if (i > 0) {
                whereClause.append(" OR ");
            }
            processFilter(children.get(i));
        }
        whereClause.append(")");
    }

    private void handleNot(Not filter) {
        whereClause.append("NOT (");
        processFilter(filter.getFilter());
        whereClause.append(")");
    }

    private void handleEquals(PropertyIsEqualTo filter) {
        String propertyName = getPropertyName(filter.getExpression1());
        Object value = getLiteralValue(filter.getExpression2());

        if (value == null) {
            // NULL checks can't use containment
            whereClause.append(buildJsonPathExtract(propertyName)).append(" IS NULL");
        } else if (value instanceof Boolean || value instanceof Number || value instanceof String) {
            // Use containment operator for GIN index optimization
            whereClause.append("result @> ?::jsonb");
            parameters.add(buildNestedJsonObject(propertyName, value));
        } else {
            // Fallback for other types
            whereClause.append(buildJsonPathExtract(propertyName)).append(" = ?");
            parameters.add(value.toString());
        }
    }

    private void handleNotEquals(PropertyIsNotEqualTo filter) {
        String propertyName = getPropertyName(filter.getExpression1());
        Object value = getLiteralValue(filter.getExpression2());

        if (value == null) {
            whereClause.append(buildJsonPathExtract(propertyName)).append(" IS NOT NULL");
        } else if (value instanceof Boolean || value instanceof Number || value instanceof String) {
            // Guard with jsonb_typeof to ensure the field exists before negating containment,
            // otherwise NOT @> matches all rows where the field is missing
            String[] parts = propertyName.split("\\.");
            StringBuilder jsonbPath = new StringBuilder("result");
            for (String part : parts) {
                jsonbPath.append("->'").append(escapeJsonKey(part)).append("'");
            }
            String expectedType = value instanceof Boolean ? "boolean"
                    : value instanceof Number ? "number" : "string";
            whereClause.append("(jsonb_typeof(").append(jsonbPath).append(") = '")
                    .append(expectedType).append("' AND NOT (result @> ")
                    .append("?::jsonb").append("))");
            parameters.add(buildNestedJsonObject(propertyName, value));
        } else {
            whereClause.append(buildJsonPathExtract(propertyName)).append(" != ?");
            parameters.add(value.toString());
        }
    }

    private void handleGreaterThan(PropertyIsGreaterThan filter) {
        String propertyName = getPropertyName(filter.getExpression1());
        Object value = getLiteralValue(filter.getExpression2());
        appendOrderedComparison(propertyName, ">", value);
    }

    private void handleGreaterThanOrEqual(PropertyIsGreaterThanOrEqualTo filter) {
        String propertyName = getPropertyName(filter.getExpression1());
        Object value = getLiteralValue(filter.getExpression2());
        appendOrderedComparison(propertyName, ">=", value);
    }

    private void handleLessThan(PropertyIsLessThan filter) {
        String propertyName = getPropertyName(filter.getExpression1());
        Object value = getLiteralValue(filter.getExpression2());
        appendOrderedComparison(propertyName, "<", value);
    }

    private void handleLessThanOrEqual(PropertyIsLessThanOrEqualTo filter) {
        String propertyName = getPropertyName(filter.getExpression1());
        Object value = getLiteralValue(filter.getExpression2());
        appendOrderedComparison(propertyName, "<=", value);
    }

    private void appendOrderedComparison(String propertyName, String operator, Object value) {
        if (value instanceof Number || value instanceof String text && isDecimal(text)) {
            appendNumericComparison(propertyName, operator, value);
        } else if (value instanceof String text) {
            Instant instant = parseRfc3339Instant(text);
            if (instant != null)
                appendInstantComparison(propertyName, operator, instant);
            else
                whereClause.append("FALSE");
        } else {
            // Ordered string comparison is intentionally limited to valid instants. In
            // particular, do not turn malformed dates or arbitrary strings into a broad
            // lexical comparison over JSON values.
            whereClause.append("FALSE");
        }
    }

    /**
     * Builds a safe numeric comparison by guarding the ::numeric cast with a jsonb_typeof check.
     * This prevents PostgreSQL cast errors when rows contain missing, null, or non-numeric values.
     */
    private void appendNumericComparison(String propertyName, String operator, Object value) {
        String[] parts = propertyName.split("\\.");
        StringBuilder jsonbPath = new StringBuilder("result");
        for (String part : parts) {
            jsonbPath.append("->'").append(escapeJsonKey(part)).append("'");
        }

        whereClause.append("(jsonb_typeof(").append(jsonbPath).append(") = 'number' AND (")
                .append(buildJsonPathExtract(propertyName)).append(")::numeric ")
                .append(operator).append(" ?::numeric)");
        parameters.add(value);
    }

    /** Compares ISO-8601 strings as instants without casting invalid row values. */
    private void appendInstantComparison(String propertyName, String operator, Instant value) {
        String[] parts = propertyName.split("\\.");
        StringBuilder jsonbPath = new StringBuilder("result");
        for (String part : parts) {
            jsonbPath.append("->'").append(escapeJsonKey(part)).append("'");
        }
        String extracted = buildJsonPathExtract(propertyName);
        whereClause.append("CASE WHEN jsonb_typeof(").append(jsonbPath)
                .append(") = 'string' AND ").append(extracted).append(" ~ '")
                .append(RFC3339_INSTANT_PATTERN).append("' AND pg_input_is_valid(")
                .append(extracted).append(", 'timestamp with time zone') AND ")
                .append("pg_input_is_valid(CAST(? AS text), 'timestamp with time zone') ")
                .append("THEN CASE WHEN (").append(extracted)
                .append(")::timestamptz >= '0001-01-01T00:00:00Z'::timestamptz AND (")
                .append(extracted).append(")::timestamptz < '10000-01-01T00:00:00Z'::timestamptz ")
                .append("THEN (").append(extracted).append(")::timestamptz ")
                .append(operator).append(" CAST(? AS timestamptz) ELSE FALSE END ELSE FALSE END");
        String canonical = value.toString();
        parameters.add(canonical);
        parameters.add(canonical);
    }

    private Instant parseRfc3339Instant(String value) {
        if (!value.matches(RFC3339_INSTANT_PATTERN))
            return null;
        try {
            Instant instant = Instant.parse(value);
            int utcYear = instant.atOffset(ZoneOffset.UTC).getYear();
            return utcYear >= 1 && utcYear <= 9999 ? instant : null;
        } catch (DateTimeParseException error) {
            return null;
        }
    }

    private boolean isDecimal(String value) {
        try {
            new BigDecimal(value);
            return true;
        } catch (NumberFormatException error) {
            return false;
        }
    }

    private void handleLike(PropertyIsLike filter) {
        String propertyName = ((PropertyName) filter.getExpression()).getPropertyName();
        String pattern = filter.getLiteral();

        // LIKE queries can't use containment, must use extraction
        pattern = pattern.replace("\\", "\\\\")
                .replace("'", "''")
                .replace("%", "\\%")
                .replace("_", "\\_")
                .replace("*", "%")
                .replace("?", "_");

        whereClause.append(buildJsonPathExtract(propertyName)).append(" LIKE ?");
        parameters.add(pattern);
    }

    /**
     * Builds a nested JSON object string for containment queries.
     * Handles dot-notation paths like "foo.bar.baz" -> {"foo": {"bar": {"baz": value}}}
     *
     * @param propertyPath Dot-separated property path
     * @param value        The value to match
     * @return JSON string for containment check
     */
    private String buildNestedJsonObject(String propertyPath, Object value) {
        String[] parts = propertyPath.split("\\.");
        StringBuilder json = new StringBuilder();

        // Build opening braces and keys
        for (int i = 0; i < parts.length; i++) {
            json.append("{\"").append(escapeJsonKey(parts[i])).append("\": ");
        }

        // Add the value
        json.append(formatJsonValue(value));

        // Close all braces
        for (int i = 0; i < parts.length; i++) {
            json.append("}");
        }

        return json.toString();
    }

    /**
     * Builds a JSONB path extraction expression for nested properties.
     * Handles dot-notation paths like "foo.bar.baz" -> result->'foo'->'bar'->>'baz'
     *
     * @param propertyPath Dot-separated property path
     * @return SQL extraction expression
     */
    private String buildJsonPathExtract(String propertyPath) {
        String[] parts = propertyPath.split("\\.");

        if (parts.length == 1) {
            return "result->>'" + escapeJsonKey(parts[0]) + "'";
        }

        StringBuilder extraction = new StringBuilder("result");

        // Use -> for all but the last key
        for (int i = 0; i < parts.length - 1; i++) {
            extraction.append("->'").append(escapeJsonKey(parts[i])).append("'");
        }

        // Use ->> for the last key to get text
        extraction.append("->>'").append(escapeJsonKey(parts[parts.length - 1])).append("'");

        return extraction.toString();
    }

    /**
     * Formats a value for JSON representation.
     */
    private String formatJsonValue(Object value) {
        if (value == null) {
            return "null";
        } else if (value instanceof Boolean) {
            return value.toString();
        } else if (value instanceof Number) {
            return value.toString();
        } else {
            // String value - escape for JSON
            return "\"" + escapeJsonString(value.toString()) + "\"";
        }
    }

    /**
     * Escapes a string for use in JSON.
     */
    private String escapeJsonString(String value) {
        if (value == null) {
            return "";
        }
        return value
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    /**
     * Escapes a key for use in JSON/SQL.
     */
    private String escapeJsonKey(String key) {
        return key
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("'", "''");
    }

    private String getPropertyName(org.geotools.api.filter.expression.Expression expr) {
        if (expr instanceof PropertyName) {
            return ((PropertyName) expr).getPropertyName();
        }
        throw new IllegalArgumentException("Expected PropertyName expression");
    }

    private Object getLiteralValue(org.geotools.api.filter.expression.Expression expr) {
        if (expr instanceof Literal) {
            return ((Literal) expr).getValue();
        }
        throw new IllegalArgumentException("Expected Literal expression");
    }

}
