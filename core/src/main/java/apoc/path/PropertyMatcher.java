package apoc.path;

import apoc.convert.Convert;
import apoc.meta.Meta;
import apoc.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.Entity;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TimeValue;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static apoc.path.LabelRelMatcherUtil.PROPS_REGEX;

public class PropertyMatcher {

    // regex for nameLabel{propertyPartOptional}
    public static final Pattern LABEL_TYPE_PATTERN = Pattern.compile("(?<labelOrType>.[^{]*)(\\{(?<props>.+)\\})?");
    // regex for prop1 = value1 / prop1 != value1 and so on
    public static final Pattern FIELD_PATTERN = Pattern.compile("(?<prop>.[^!><]+)(?<operator>=|>=|<=|<|>|!=)(?<value>.+)");
    private static final String PIPE_IN_BRACKETS = "\\s*\\|\\s*";
    private static final String INCLUDE_PFX = "+";
    private static final String EXCLUDE_PFX = "-";

    public static boolean matchesProperties(Entity entity, String propertyString) {
        return matchesPropertiesCommon(propertyString, andItem -> matchProperty(andItem, entity));
    }
    
    public static boolean matchesPropsSchema(List<String> propsList, String propertyString) {
        return matchesPropertiesCommon(propertyString, andItem -> {
            final String itemSubstring = andItem.substring(1);
            if (andItem.startsWith(INCLUDE_PFX)) {
                return propsList.contains(itemSubstring);
            } else if (andItem.startsWith(EXCLUDE_PFX)) {
                return !propsList.contains(itemSubstring);
            } else {
                // we consider only + and - to filter schema
                return true;
            }
        });
    }

    private static boolean matchesPropertiesCommon(String propertyString, Predicate<String> stringPredicate) {
        // when property part or relPropFilter / nodePropFilter node is not present
        if (StringUtils.isBlank(propertyString)) {
            return true;
        }
        
        final String[] splitOrs = propertyString.split(PIPE_IN_BRACKETS);

        return Arrays.stream(splitOrs).anyMatch(orItem -> {
            final String[] splitAnds = orItem.split("\\s*&\\s*");
            return Arrays.stream(splitAnds).allMatch(stringPredicate);
        });
    }

    private static boolean matchProperty(String orItem, Entity entity) {
        if (orItem.startsWith(INCLUDE_PFX)) {
            return entity.hasProperty(orItem.substring(1));
        } 
        if(orItem.startsWith(EXCLUDE_PFX)) {
            return !entity.hasProperty(orItem.substring(1));
        }
        
        final Matcher matcher = FIELD_PATTERN.matcher(orItem);
        if (matcher.matches()) {
            final String propName = matcher.group("prop");
            final String value = matcher.group("value");
            final String operator = matcher.group("operator");
            Object nodeProperty = entity.getProperty(propName, null);
            // when property doesn't exists
            if (nodeProperty == null) {
                return false;
            }
            final boolean isComparable = nodeProperty instanceof Comparable;
            final Object valueConverted = convertValue(value, nodeProperty.getClass());
            if (nodeProperty.getClass().isArray()) {
                nodeProperty = (List<Object>) Convert.convertToList(nodeProperty);
            }
            switch (operator) {
                case ">":
                    return isComparable && ((Comparable) nodeProperty).compareTo(valueConverted) > 0;
                case ">=":
                    return isComparable && ((Comparable) nodeProperty).compareTo(valueConverted) >= 0;
                case "<":
                    return isComparable && ((Comparable) nodeProperty).compareTo(valueConverted) < 0;
                case "<=":
                    return isComparable && ((Comparable) nodeProperty).compareTo(valueConverted) <= 0;
                case "!=":
                    return !nodeProperty.equals(valueConverted);
                default: // '=' case:
                    return nodeProperty.equals(valueConverted);
            }
        }
        
        return false;
    }

    private static Object convertValue(String value, Class<?> nodeProperty) {
        // todo - evaluate reuse of Mapping.java from vga91:issue-1471  
        final Meta.Types metaType = Meta.Types.of(nodeProperty);
        switch (metaType) {
            case POINT:
                return PointValue.parse(value);
            case LOCAL_DATE_TIME:
                return LocalDateTimeValue.parse(value).asObjectCopy();
            case LOCAL_TIME:
                return LocalTimeValue.parse(value).asObjectCopy();
            case DATE_TIME:
                return DateTimeValue.parse(value, ZoneId::systemDefault).asObjectCopy();
            case TIME:
                return TimeValue.parse(value, ZoneId::systemDefault).asObjectCopy();
            case DATE:
                return DateValue.parse(value).asObjectCopy();
            case DURATION:
                return DurationValue.parse(value);
            case INTEGER: 
                return Util.toLong(value);
            case FLOAT: 
                return Util.toDouble(value);
            case BOOLEAN: 
                return Util.toBoolean(value);
            case LIST:
                return Arrays.stream(value.split(","))
                        .map(item -> convertValue(item, nodeProperty.getComponentType()))
                        .collect(Collectors.toList());
            default:
                return value;
        }
    }

    public static String getPropsMatched(Matcher matcher, String props) {
        final String propsMatched = matcher.group(PROPS_REGEX);
        if (propsMatched != null) {
            props = propsMatched;
        }
        return props;
    }
}
