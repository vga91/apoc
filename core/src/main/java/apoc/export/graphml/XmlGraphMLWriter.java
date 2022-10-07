package apoc.export.graphml;

import apoc.export.util.*;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.Writer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static apoc.export.util.MetaInformation.*;
import static apoc.meta.tablesforlabels.PropertyTracker.typeMappings;

/**
 * @author mh
 * @since 21.01.14
 */
public class XmlGraphMLWriter {
    private final Map<String, Map<String, Class>> totalKeyTypes = new HashMap<>();
    private final Transaction tx;

    public XmlGraphMLWriter(Transaction tx) {
        this.tx = tx;
    }
    
    public void write(SubGraph graph, Writer writer, Reporter reporter, ExportConfig config) throws Exception {
        XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();
        XMLStreamWriter xmlWriter = xmlOutputFactory.createXMLStreamWriter(writer);
        writeHeader(xmlWriter);
        writeKey(xmlWriter, graph, config);
        writeGraph(xmlWriter);
        for (Node node : graph.getNodes()) {
            int props = writeNode(xmlWriter, node, config);
            reporter.update(1, 0, props);
        }
        for (Relationship rel : graph.getRelationships()) {
            int props = writeRelationship(xmlWriter, rel, config);
            reporter.update(0, 1, props);
        }
        writeFooter(xmlWriter);
        reporter.done();
    }
    
    public static String addSuffix(String propMap) {
        if (propMap == null || propMap.isEmpty()) {
            return propMap;
        }
        return "__" + propMap;
    }

    private void writeKey(XMLStreamWriter writer, SubGraph ops, ExportConfig config) throws Exception {
        // we'll create a Map<propName, <suffixes, classes>>
        Map<String, Map<String, Class>> keyTypes = new HashMap<>();
        boolean useTypes = config.useTypes();

        if (ops.getAllLabelsInUse().iterator().hasNext()) {
            if (config.getFormat() == ExportFormat.TINKERPOP) {
                initPropKey(keyTypes, "labelV", String.class);
            } else {
                initPropKey(keyTypes, "labels", String.class);
            }
        }
        
        if (config.isSampling()) {
            final Result result = tx.execute("CALL apoc.meta.nodeTypeProperties($conf)",
                    Map.of("conf", getConfWithIncludeLabels(ops, config)));

            final Map<String, Map<String, Class>> mapResultTransformer = getMapResultTransformer(result);
            keyTypes.putAll(mapResultTransformer);
        } else {
            for (Node node : ops.getNodes()) {
                updateKeyTypesGraphMl(keyTypes, node);
            }
        }
        
        ExportFormat format = config.getFormat();
        if (format == ExportFormat.GEPHI) {
            initPropKey(keyTypes, "TYPE", String.class);
        }
        writeKey(writer, keyTypes, "node", useTypes);
        // adding nodes keyTypes
        totalKeyTypes.putAll(keyTypes);
        keyTypes.clear();
        if (ops.getAllRelationshipTypesInUse().iterator().hasNext()) {
            if (config.getFormat() == ExportFormat.TINKERPOP) {
                initPropKey(keyTypes, "labelE", String.class);
            } else {
                initPropKey(keyTypes, "label", String.class);
            }
        }
        if (config.isSampling()) {
            final Result result = tx.execute("CALL apoc.meta.relTypeProperties($conf)",
                    Map.of("conf", getConfWithIncludeRels(ops, config)));
            keyTypes.putAll(getMapResultTransformer(result));
        } else {
            for (Relationship rel : ops.getRelationships()) {
                updateKeyTypesGraphMl(keyTypes, rel);
            }
        }
        if (format == ExportFormat.GEPHI) {
            initPropKey(keyTypes, "TYPE", String.class);
        }
        writeKey(writer, keyTypes, "edge", useTypes);
        // adding rels keyTypes
        totalKeyTypes.putAll(keyTypes);
    }

    private Map<String, Map<String, Class>> getMapResultTransformer(Result result) {
        return result.stream()
                .filter(map -> map.get("propertyName") != null)
                .collect(Collectors.toMap(map -> (String) map.get("propertyName"),
                        map -> ((List<String>) map.get("propertyTypes"))
                                .stream()
                                .collect(Collectors.toMap(type -> type,
                                        MetaInformation::getClassFromMetaType)), 
                        (e1, e2) -> {
                            e1.putAll(e2);
                            return e1;
                        }
                ));
    }




    private void writeKey(XMLStreamWriter writer, Map<String, Map<String, Class>> keyTypes, String forType, boolean useTypes) throws XMLStreamException {
        for (Map.Entry<String, Map<String, Class>> entry : keyTypes.entrySet()) {
            final Map<String, Class> entryValue = entry.getValue();
            for (Map.Entry<String, Class> subEntry : entryValue.entrySet()) {
                final Class typeClass = subEntry.getValue();
                String type = MetaInformation.typeFor(typeClass, MetaInformation.GRAPHML_ALLOWED);
                if (type == null) continue;
                writer.writeEmptyElement("key");
                // append uuid suffix if necessary
                writer.writeAttribute("id", entry.getKey() + addSuffix(subEntry.getKey()));
                writer.writeAttribute("for", forType);
                writer.writeAttribute("attr.name", entry.getKey());
                if (useTypes) {
                    if (typeClass.isArray()) {
                        writer.writeAttribute("attr.type", "string");
                        writer.writeAttribute("attr.list", type);
                    } else {
                        writer.writeAttribute("attr.type", type);
                    }
                }
                newLine(writer);
            }
        }
    }

    private int writeNode(XMLStreamWriter writer, Node node, ExportConfig config) throws XMLStreamException {
        writer.writeStartElement("node");
        writer.writeAttribute("id", id(node));
        if (config.getFormat() != ExportFormat.TINKERPOP) {
            writeLabels(writer, node);
        }
        writeLabelsAsData(writer, node, config);
        int props = writeProps(writer, node, config);
        endElement(writer);
        return props;
    }

    private String id(Node node) {
        return "n" + node.getId();
    }

    private void writeLabels(XMLStreamWriter writer, Node node) throws XMLStreamException {
        String labelsString = getLabelsString(node);
        if (!labelsString.isEmpty()) writer.writeAttribute("labels", labelsString);
    }

    private void writeLabelsAsData(XMLStreamWriter writer, Node node, ExportConfig config) throws XMLStreamException {
        String labelsString = getLabelsString(node);
        if (labelsString.isEmpty()) return;
        String delimiter = ":";
        if (config.getFormat() == ExportFormat.GEPHI) {
            writeData(writer, "TYPE", delimiter + FormatUtils.joinLabels(node, delimiter));
            writeData(writer, "label", getLabelsStringGephi(config, node));
        } else if (config.getFormat() == ExportFormat.TINKERPOP){
            writeData(writer, "labelV", FormatUtils.joinLabels(node, delimiter));
        } else {
            writeData(writer, "labels", labelsString);
        }
    }

    private int writeRelationship(XMLStreamWriter writer, Relationship rel, ExportConfig config) throws XMLStreamException {
        writer.writeStartElement("edge");
        writer.writeAttribute("id", id(rel));
        getNodeAttribute(writer, XmlNodeExport.NodeType.SOURCE, config, rel);
        getNodeAttribute(writer, XmlNodeExport.NodeType.TARGET, config, rel);
        if (config.getFormat() == ExportFormat.TINKERPOP) {
            writeData(writer, "labelE", rel.getType().name());
        } else {
            writer.writeAttribute("label", rel.getType().name());
            writeData(writer, "label", rel.getType().name());
        }
        if (config.getFormat() == ExportFormat.GEPHI) {
            writeData(writer, "TYPE", rel.getType().name());
        }
        int props = writeProps(writer, rel, config);
        endElement(writer);
        return props;
    }

    private void getNodeAttribute(XMLStreamWriter writer, XmlNodeExport.NodeType nodeType, ExportConfig config, Relationship rel) throws XMLStreamException {

        final XmlNodeExport.ExportNode xmlNodeInterface = nodeType.get();
        final Node node = xmlNodeInterface.getNode(rel);
        final String name = nodeType.getName();
        final ExportConfig.NodeConfig nodeConfig = xmlNodeInterface.getNodeConfig(config);
        // without config the source/target configs, we leverage the internal node id
        if (StringUtils.isBlank(nodeConfig.id)) {
            writer.writeAttribute(name, id(node));
            return;
        }
        // with source/target with an id configured 
        // we put a source with the property value and a sourceType with the prop type of node
        try {
            final Object nodeProperty = node.getProperty(nodeConfig.id);
            writer.writeAttribute(name, nodeProperty.toString());
            writer.writeAttribute(nodeType.getNameType(), MetaInformation.typeFor(nodeProperty.getClass(), MetaInformation.GRAPHML_ALLOWED));
        } catch (NotFoundException e) {
            throw new RuntimeException(
                    "The config source and/or target cannot be used because the node with id " + node.getId() + " doesn't have property " + nodeConfig.id);
        }
    }

    private String id(Relationship rel) {
        return "e" + rel.getId();
    }

    private void endElement(XMLStreamWriter writer) throws XMLStreamException {
        writer.writeEndElement();
        newLine(writer);
    }

    private int writeProps(XMLStreamWriter writer, Entity node, ExportConfig config) throws XMLStreamException {
        int count = 0;
        for (String prop : node.getPropertyKeys()) {
            Object value = node.getProperty(prop);
            if (config.useTypes()) {
                if (config.isSampling()) {
                    // when we use sampling, if we found a metaType from totalKeyTypes
                    // e.g. "IntArray", we add the suffix, that is: <data key="nameProp__TYPE".... >
                    // if a meta is not found via apoc.meta.*, we don't add suffix, that is, we coerce value to string
                    final Map<String, Class> propKeyTypes = totalKeyTypes.getOrDefault(prop, Collections.emptyMap());
                    final String metaType = typeMappings.get(ClassUtils.getCanonicalName(value.getClass()));
                    boolean isMultiTypeProp = propKeyTypes.size() > 1 && propKeyTypes.containsKey(metaType);
                    if (isMultiTypeProp) {
                        String suffix = addSuffix(metaType);
                        prop += suffix;
                    }
                }
            }

            writeData(writer, prop, value);
            count++;
        }
        return count;
    }

    private void writeData(XMLStreamWriter writer, String prop, Object value) throws XMLStreamException {
        writer.writeStartElement("data");
        writer.writeAttribute("key", prop);
        if (value != null) {
            writer.writeCharacters(FormatUtils.toXmlString(value));
        }
        writer.writeEndElement();
    }

    private void writeFooter(XMLStreamWriter writer) throws XMLStreamException {
        endElement(writer);
        endElement(writer);
        writer.writeEndDocument();
    }

    private void writeHeader(XMLStreamWriter writer) throws XMLStreamException {
        writer.writeStartDocument("UTF-8", "1.0");
        newLine(writer);
        writer.writeStartElement("graphml"); // todo properties
        writer.writeNamespace("xmlns", "http://graphml.graphdrawing.org/xmlns");
        writer.writeAttribute("xmlns", "http://graphml.graphdrawing.org/xmlns", "xsi", "http://www.w3.org/2001/XMLSchema-instance");
        writer.writeAttribute("xsi", "", "schemaLocation", "http://graphml.graphdrawing.org/xmlns http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd");
        newLine(writer);
    }

    private void writeGraph(XMLStreamWriter writer) throws XMLStreamException {
        writer.writeStartElement("graph");
        writer.writeAttribute("id", "G");
        writer.writeAttribute("edgedefault", "directed");
        newLine(writer);
    }

    private void newLine(XMLStreamWriter writer) throws XMLStreamException {
        writer.writeCharacters(System.getProperty("line.separator"));
    }
}
