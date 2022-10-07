package apoc.export.util;

import apoc.meta.Types;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ClassUtils;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResultTransformer;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static apoc.gephi.GephiFormatUtils.getCaption;
import static apoc.meta.tablesforlabels.PropertyTracker.typeMappings;
import static apoc.util.collection.Iterables.stream;
import static java.util.Arrays.asList;

/**
 * @author mh
 * @since 19.01.14
 */
public class MetaInformation {
    
    public static final Map<String, String> REVERSED_TYPE_MAP = MapUtils.invertMap(typeMappings);
    
    public static Map<String, Class> collectPropTypesForNodes(SubGraph graph, GraphDatabaseService db, ExportConfig config) {
        if (!config.isSampling()) {
            Map<String,Class> propTypes = new LinkedHashMap<>();
            for (Node node : graph.getNodes()) {
                updateKeyTypes(propTypes, node);
            }
            return propTypes;
        }

        return db.executeTransactionally("CALL apoc.meta.nodeTypeProperties($conf)",
                Map.of("conf", getConfWithIncludeLabels(graph, config)), 
                getMapResultTransformer());
    }

    public static Map<String, Object> getConfWithIncludeLabels(SubGraph graph, ExportConfig config) {
        final Map<String, Object> conf = config.getSamplingConfig();
        conf.putIfAbsent("includeLabels", stream(graph.getAllLabelsInUse()).map(Label::name).collect(Collectors.toList()));
        return conf;
    }
    
    public static Map<String, Class> collectPropTypesForRelationships(SubGraph graph, GraphDatabaseService db, ExportConfig config) {
        if (!config.isSampling()) {
            Map<String,Class> propTypes = new LinkedHashMap<>();
            for (Relationship relationship : graph.getRelationships()) {
                updateKeyTypes(propTypes, relationship);
            }
            return propTypes;
        }

        return db.executeTransactionally("CALL apoc.meta.relTypeProperties($conf)",
                Map.of("conf", getConfWithIncludeRels(graph, config)), 
                getMapResultTransformer());
    }

    public static Map<String, Object> getConfWithIncludeRels(SubGraph graph, ExportConfig config) {
        final Map<String, Object> conf = config.getSamplingConfig();
        conf.putIfAbsent("includeRels", stream(graph.getAllRelationshipTypesInUse()).map(RelationshipType::name).collect(Collectors.toList()));
        return conf;
    }

    private static ResultTransformer<Map<String, Class>> getMapResultTransformer() {
        return result -> result.stream()
                .filter(map -> map.get("propertyName") != null)
                .collect(Collectors.toMap(map -> (String) map.get("propertyName"),
                        map -> {
                            final String propertyTypes = ((List<String>) map.get("propertyTypes")).get(0);
                            return getClassFromMetaType(propertyTypes);
                        }, (e1, e2) -> e2));
    }

    public static void updateKeyTypes(Map<String, Class> keyTypes, Entity pc) {
        for (String prop : pc.getPropertyKeys()) {
            Object value = pc.getProperty(prop);
            Class storedClass = keyTypes.get(prop);
            if (storedClass == null) {
                keyTypes.put(prop, value.getClass());
                continue;
            }
            if (storedClass == void.class || storedClass.equals(value.getClass())) continue;
            keyTypes.put(prop, void.class);
        }
    }

    public static void updateKeyTypesGraphMl(Map<String, Map<String, Class>> keyTypes, Entity pc) {
        for (String prop : pc.getPropertyKeys()) {
            Object value = pc.getProperty(prop);
            Map<String, Class> storedClasses = keyTypes.get(prop);
            if (storedClasses==null || storedClasses.isEmpty()) {
                initPropKey(keyTypes, prop, value.getClass());
            } else {
                Class storedClass = storedClasses.values().iterator().next();
                if (storedClass == void.class || storedClass.equals(value.getClass())) continue;
                initPropKey(keyTypes, prop, void.class);
            }
        }
    }

    public static void initPropKey(Map<String, Map<String, Class>> keyTypes, String prop, Class<?> value) {
        // currently, without sampling config we don't differentiate multiple types in case of equivalent property names   
        keyTypes.put(prop, Map.of("",  value));
    }

    public final static Set<String> GRAPHML_ALLOWED = new HashSet<>(asList("boolean", "int", "long", "float", "double", "string"));

    public static String typeFor(Class value, Set<String> allowed) {
        if (value == void.class) return null; // Is this necessary?
        Types type = Types.of(value);
        String name = (value.isArray() ? value.getComponentType() : value).getSimpleName().toLowerCase();
        boolean isAllowed = allowed != null && allowed.contains(name);
        switch (type) {
            case NULL:
                return null;
            case INTEGER: case FLOAT:
                return "integer".equals(name) || !isAllowed ? "int" : name;
            default:
                return isAllowed ? name : "string"; // We manage all other data types as strings
        }
    }

    public static String getLabelsString(Node node) {
        if (!node.getLabels().iterator().hasNext()) return "";
        String delimiter = ":";
        return delimiter + FormatUtils.joinLabels(node, delimiter);
    }

    public static String getLabelsStringGephi(ExportConfig config, Node node) {
        return getCaption(node, config.getCaption());
    }
    
    public static Class getClassFromMetaType(String propertyTypes) {
        // take the className from the result, inversely to the meta.relTypeProperties/nodeTypeProperties procedures
        String className = REVERSED_TYPE_MAP.get(propertyTypes);
        try {
            return ClassUtils.getClass(className);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
