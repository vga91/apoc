package apoc.export.cypher.formatter;

import apoc.export.util.ExportConfig;
import apoc.export.util.Reporter;
import apoc.path.LabelMatcher;
import apoc.path.RelMatcher;
import org.neo4j.graphdb.*;

import java.io.PrintWriter;
import java.util.Map;
import java.util.Set;

/**
 * @author AgileLARUS
 *
 * @since 16-06-2017
 */
public interface CypherFormatter {

	String statementForNode(Node node, Map<String, Set<String>> uniqueConstraints, Set<String> indexedProperties, Set<String> indexNames);

	String statementForRelationship(Relationship relationship, Map<String, Set<String>> uniqueConstraints, Set<String> indexedProperties, ExportConfig exportConfig);

	String statementForNodeIndex(String indexType, String label, Iterable<String> keys, boolean ifNotExist, String idxName);

	String statementForIndexRelationship(String indexType, String type, Iterable<String> keys, boolean ifNotExist, String idxName);

	String statementForNodeFullTextIndex(String name, Iterable<Label> labels, Iterable<String> keys);

	String statementForRelationshipFullTextIndex(String name, Iterable<RelationshipType> types, Iterable<String> keys);

	String statementForCreateConstraint(String name, String label, Iterable<String> keys, boolean ifNotExist);

	String statementForDropConstraint(String name);

	String statementForCleanUp(int batchSize);

	void statementForNodes(Iterable<Node> node, Map<String, Set<String>> uniqueConstraints, ExportConfig exportConfig, PrintWriter out, Reporter reporter, GraphDatabaseService db, LabelMatcher labelMatcher);

	void statementForRelationships(Iterable<Relationship> relationship, Map<String, Set<String>> uniqueConstraints, ExportConfig exportConfig, PrintWriter out, Reporter reporter, GraphDatabaseService db, RelMatcher relMatcher);

}
