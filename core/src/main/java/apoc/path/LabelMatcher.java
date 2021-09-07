package apoc.path;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A generic label matcher which evaluates whether or not a node has at least one of the labels added on the matcher.
 * String labels can be added on the matcher. The label can optionally be be prefixed with `:`.
 * Also handles compound labels (multiple labels separated by `:`), and a node will be matched if it has all of the labels
 * in a compound label (order does not matter).
 * If the node only has a subset of the compound label, it will only be matched if that subset is in the matcher.
 * For example, a LabelMatcher with only `Person:Manager` will only match on nodes with both :Person and :Manager, not just one or the other.
 * Any other labels on the matched node would not be relevant and would not affect the match.
 * If the LabelMatcher only had `Person:Manager` and `Person:Boss`, then only nodes with both :Person and :Manager, or :Person and :Boss, would match.
 * Some nodes that would not match would be: :Person, :Boss, :Manager, :Boss:Manager, but :Boss:Person:HeadHoncho would match fine.
 * Also accepts a special `*` label, indicating that the matcher will always return a positive match.
 * LabelMatchers hold no context about what a match means, and do not handle labels prefixed with filter symbols (+, -, /, &gt;).
 * Please strip these symbols from the start of each label before adding to the matcher.
 */
public class LabelMatcher {

    private static LabelMatcher ACCEPTS_ALL_LABEL_MATCHER = new LabelMatcher() {
        @Override
        public boolean matchesLabels(Set<String> nodeLabels) {
            return true;
        }

    private enum CheckType {BLACKLIST_ALL, BLACKLIST_ANY, WHITELIST_ALL, WHITELIST_ANY}

    private List<Pair<String, Map<String, Object>>> labels = new ArrayList<>();
    private List<Pair<Set<String>, Map<String, Object>>> compoundLabels = new ArrayList<>();

    public static LabelMatcher acceptsAllLabelMatcher() {
        return ACCEPTS_ALL_LABEL_MATCHER;
    }

    public LabelMatcher addLabel(String label, String props, boolean checkFirstChar) {
        props = props == null ? StringUtils.EMPTY : props;
        
        if (!checkFirstChar && "*".equals(label)) {
            labels = Collections.singletonList(Pair.of("*", Map.of(PROPS, props, CHECK_TYPE, CheckType.WHITELIST_ANY)));
            return this;
        }
        
        CheckType checkType = CheckType.WHITELIST_ANY;
        if (checkFirstChar) { // for export-cypher case
            final Matcher regExMatcher = FILTER_TYPE_PATTERN.matcher(label);
            if (regExMatcher.matches()) {
                String operator = regExMatcher.group("typeFilter");
                switch (operator) {
                    case "++":
                        checkType = CheckType.WHITELIST_ALL;
                        break;
                    case "--":
                        checkType = CheckType.BLACKLIST_ALL;
                        break;
                    case "-":
                        checkType = CheckType.BLACKLIST_ANY;
                        break;
                    case "+":
                        break;
                    default:
                        throw new RuntimeException("e"); // todo - forse non serve
                }
            } else {
                // todo - testare questo...
                throw new RuntimeException("Invalid type filter. Valid types are '++', '--', '+' and '-'");
            }
            label = regExMatcher.group(LABEL_TYPE_REGEX);
        }


        if (label.charAt(0) == ':') {
            label = label.substring(1);
        }

        String[] elements = label.split(":");
        if (elements.length == 1) {
            labels.add(label);
        } else if (elements.length > 1) {
            compoundLabels.add(Pair.of(Set.copyOf(Arrays.asList(elements)), Map.of(PROPS, props, "checkType", checkType)));
        }

        return this;
    }

    public boolean matchesLabels(Entity entity) {
        return matchesLabels(entity, false);
    }
    
    public boolean matchesLabels(Entity entity, boolean allowEmptyLabels) {

        if (!allowEmptyLabels) {
            if (labels.size() == 1 && labels.get(0).first().equals("*")) { // todo - valutare questa prima parte...
                return matchesProperties(entity, (String) labels.get(0).other().get(PROPS));
            }
        }

        return matchCommon(entity, allowEmptyLabels);
    }

    boolean matchCommon(Entity entity, boolean allowEmptyLabels) {
        final Set<String> nodeLabels = new HashSet<>();
        if (entity instanceof Node) {
            ((Node) entity).getLabels().forEach(label -> nodeLabels.add(label.name()));
        } else {
            final Relationship relationship = (Relationship) entity;
            nodeLabels.add(relationship.getType().name());
        }

        // with export cypher we consider all labels / rel-types, if label / rel-type filter is empty
        if (allowEmptyLabels && labels.isEmpty() && compoundLabels.isEmpty()) {
            return true;
        }

        for (Pair<String, Map<String, Object>> labelPair : labels) {
            final String label = labelPair.first();
            final Map<String, Object> other = labelPair.other();
            if (isContains(nodeLabels, label, (CheckType) other.get(CHECK_TYPE))) {
                return matchesProperties(entity, (String) other.get(PROPS));
            }
        }

        if (compoundLabels != null) {
            for (Pair<Set<String>, Map<String, Object>> compoundLabelPair : compoundLabels) {
                final Set<String> compoundLabel = compoundLabelPair.first();
                final Map<String, Object> other = compoundLabelPair.other();
                if (isContainsAll(nodeLabels, compoundLabel, (CheckType) other.get(CHECK_TYPE))) {
                    return matchesProperties(entity, (String) other.get(PROPS));
                }
            }
        }

        return false;
    }

    
    public boolean isMatchedSchema(List<String> props, Set<String> nodeLabels) {
        if (labels.isEmpty() && compoundLabels.isEmpty()) {
            return true; // todo - questa parte non dovrebbe valere per pathExpander credo...
        }
        
        for (Pair<String, Map<String, Object>> labelPair : labels) {
            final String label = labelPair.first();
            final Map<String, Object> other = labelPair.other();
            if (isContainsSchema(nodeLabels, Set.of(label), (CheckType) other.get(CHECK_TYPE))) { // todo - fare discorso come sopra, nodeLabels.contains(label), !nodeLabels.contains(label), etc..
                return matchesPropsSchema(props, (String) other.get(PROPS));
            }
        }

        if (compoundLabels != null) {
            for (Pair<Set<String>, Map<String, Object>> compoundLabelPair : compoundLabels) {
                final Set<String> compoundLabel = compoundLabelPair.first();
                final Map<String, Object> other = compoundLabelPair.other();
                if (isContainsSchema(nodeLabels, compoundLabel, (CheckType) other.get(CHECK_TYPE))) { // todo - fare discorso come sopra, nodeLabels.contains(label), !nodeLabels.contains(label), etc..
                    return matchesPropsSchema(props, (String) other.get(PROPS));
                }
            }
        }
        return false;
    }
    
    private boolean isContainsAll(Set<String> nodeLabels, Set<String> compoundLabel, CheckType checkType) {
        if (compoundLabel.equals(Set.of("*"))) {
            return true;
        }
        switch (checkType) {
            case BLACKLIST_ALL:
                return !nodeLabels.equals(compoundLabel);
            case BLACKLIST_ANY:
                return !nodeLabels.containsAll(compoundLabel);
            case WHITELIST_ALL:
                return nodeLabels.equals(compoundLabel);
            default:
                return nodeLabels.containsAll(compoundLabel);
        }
    }

    private boolean isContains(Set<String> nodeLabels, String label, CheckType checkType) {
        if (label.equals("*")) {
            return true;
        }
        switch (checkType) {
            case BLACKLIST_ALL:
                return !nodeLabels.equals(Set.of(label));
            case BLACKLIST_ANY:
                return !nodeLabels.contains(label);
            case WHITELIST_ALL:
                return nodeLabels.equals(Set.of(label));
            default:
                return nodeLabels.contains(label);
        }
    }

    private boolean isContainsSchema(Set<String> nodeLabels, Set<String> label, CheckType checkType) {
        if (label.equals(Set.of("*"))) {
            return true;
        }
        switch (checkType) {
            case BLACKLIST_ANY:
                return !nodeLabels.containsAll(label);
            case WHITELIST_ANY:
                return nodeLabels.containsAll(label);
            default:
                return true; // with BLACKLIST_ALL and WHITELIST_ALL i can have other labels
        }
    }

    public boolean isEmpty() {
        return labels.isEmpty() && (compoundLabels == null || compoundLabels.isEmpty());
    }
}