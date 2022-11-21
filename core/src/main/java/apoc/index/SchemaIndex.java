package apoc.index;

import apoc.result.ListResult;
import apoc.util.QueueBasedSpliterator;
import apoc.util.Util;
import apoc.util.collection.Iterables;
import apoc.util.collection.Iterators;
import org.neo4j.common.EntityType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.kernel.api.*;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.TerminationGuard;
import org.neo4j.procedure.builtin.FulltextProcedures;
import org.neo4j.values.storable.Value;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author mh
 * @since 23.05.16
 */
public class SchemaIndex {

    private static final PropertyValueCount POISON = new PropertyValueCount("poison", "poison", "poison", -1);

    @Context
    public GraphDatabaseAPI db;

    @Context
    public Transaction tx;

    @Context
    public TerminationGuard terminationGuard;


    @Procedure("apoc.schema.properties.distinct")
    @Description("Returns all distinct node property values for the given key.")
    public Stream<ListResult> distinct(@Name("label") String label, @Name("key")  String key) {
        List<Object> values = distinctCount(label, key).map(propertyValueCount -> propertyValueCount.value).collect(Collectors.toList());
        return Stream.of(new ListResult(values));
    }

    @Procedure("apoc.schema.properties.distinctCount")
    @Description("Returns all distinct property values and counts for the given key.")
    public Stream<PropertyValueCount> distinctCount(@Name(value = "label", defaultValue = "") String labelName, @Name(value = "key", defaultValue = "") String keyName) {

        BlockingQueue<PropertyValueCount> queue = new LinkedBlockingDeque<>(100);
        Iterable<IndexDefinition> indexDefinitions = (labelName.isEmpty()) ? tx.schema().getIndexes() : tx.schema().getIndexes(Label.label(labelName));

        Util.newDaemonThread(() ->
                StreamSupport.stream(indexDefinitions.spliterator(), true)
                        .filter(indexDefinition -> isIndexCoveringProperty(indexDefinition, keyName))
                        .map(indexDefinition -> scanIndexDefinitionForKeys(indexDefinition, keyName, queue))
                        .collect(new QueuePoisoningCollector(queue, POISON))
        ).start();

        return StreamSupport.stream(new QueueBasedSpliterator<>(queue, POISON, terminationGuard, Integer.MAX_VALUE),false);
    }

    private Object scanIndexDefinitionForKeys(IndexDefinition indexDefinition, @Name(value = "key", defaultValue = "") String keyName,  BlockingQueue<PropertyValueCount> queue) {
        try (Transaction threadTx = db.beginTx()) {
            KernelTransaction ktx = ((InternalTransaction)threadTx).kernelTransaction();
            Iterable<String> keys = keyName.isEmpty() ? indexDefinition.getPropertyKeys() : Collections.singletonList(keyName);
            for (String key : keys) {
                try (KernelStatement ignored = (KernelStatement) ktx.acquireStatement()) {
                    SchemaRead schemaRead = ktx.schemaRead();
                    TokenRead tokenRead = ktx.tokenRead();
                    Read read = ktx.dataRead();
                    CursorFactory cursors = ktx.cursors();

                    int[] propertyKeyIds = StreamSupport.stream(indexDefinition.getPropertyKeys().spliterator(), false)
                            .mapToInt(tokenRead::propertyKey)
                            .toArray();
                    
                    SchemaDescriptor schema;
                    if (isFullText(indexDefinition)) {
                        int[] labelIds = Iterables.stream(indexDefinition.getLabels())
                                .mapToInt(lbl -> tokenRead.nodeLabel(lbl.name()))
                                .toArray();
                        schema = SchemaDescriptors.fulltext(EntityType.NODE, labelIds, propertyKeyIds);
                    } else {
                        String label = Iterables.single(indexDefinition.getLabels()).name();
                        schema = SchemaDescriptors.forLabel(tokenRead.nodeLabel(label), propertyKeyIds);
                    }

                    IndexDescriptor indexDescriptor = Iterators.singleOrNull(schemaRead.index(schema));
                    if (indexDescriptor == null) {
                        return null;
                    }
                    scanIndex(queue, indexDefinition, key, read, cursors, indexDescriptor, ktx);
                }
            }
            threadTx.commit();
            return null;
        }
    }

    private void scanIndex(BlockingQueue<PropertyValueCount> queue, IndexDefinition indexDefinition, String key, Read read, CursorFactory cursors, IndexDescriptor indexDescriptor, KernelTransaction ktx) {
        try (NodeValueIndexCursor cursor = cursors.allocateNodeValueIndexCursor( ktx.cursorContext(), ktx.memoryTracker())) {
            // we need to using IndexOrder.NONE here to prevent an exception
            // however the index guarantees to be scanned in order unless
            // there are writes done in the same tx beforehand - which we don't do.
            
            final IndexReadSession indexSession;
            try {
                indexSession = read.indexReadSession( indexDescriptor );
            }catch (Exception e) {
                // we skip indexScan if it's still populating
                if (e.getMessage().contains("Index is still populating")) {
                    return;
                }
                throw e;
            }
            if (isFullText(indexDefinition)) {
                read.nodeIndexSeek(ktx.queryContext(), indexSession, cursor, IndexQueryConstraints.unconstrained(), PropertyIndexQuery.fulltextSearch("*"));
            } else {
                read.nodeIndexScan(indexSession, cursor, IndexQueryConstraints.unorderedValues());
            }

            Value previousValue = null;
            long count = 0;
            Map<Object, Integer> fullTextMap = new HashMap<>();
            while (cursor.next()) {
                if (isFullText(indexDefinition)) {
                    long nodeReference = cursor.nodeReference();
                    FulltextProcedures.NodeOutput nodeOutput = FulltextProcedures.NodeOutput.forExistingEntityOrNull(tx, nodeReference, cursor.score());
                    final Node node = nodeOutput.node;
                    indexDefinition.getPropertyKeys()
                            .forEach(propKey -> {
                                final Object property = node.getProperty(propKey, null);
                                if (property != null) {
                                    fullTextMap.merge(property, 1, Integer::sum);
                                }
                            });
                } else {
                    for (int i = 0; i < cursor.numberOfProperties(); i++) {
                        Value v = cursor.propertyValue(i);
                        if (Objects.equals(v, previousValue)) { //  nullsafe equals
                            count++;
                        } else {
                            if (previousValue != null) {
                                putIntoQueue(queue, indexDefinition, key, previousValue.asObject(), count);
                            }
                            previousValue = v;
                            count = 1;
                        }
                    }
                }
            }
            if (isFullText(indexDefinition)) {
                fullTextMap.forEach((k,v) -> {
                    putIntoQueue(queue, indexDefinition, key, k, v);
                });
            } else {
                putIntoQueue(queue, indexDefinition, key, previousValue, count);
            }
        } catch (KernelException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean isFullText(IndexDefinition indexDefinition) {
        return indexDefinition.getIndexType().equals(IndexType.FULLTEXT);
    }

    private void putIntoQueue(BlockingQueue<PropertyValueCount> queue, IndexDefinition indexDefinition, String key, Object value, long count) {
        // if no value returned, like in testDistinctWithNoPreviousNodesShouldNotHangs
        if (value == null) {
            return;
        }
        try {
            for (Label label: indexDefinition.getLabels()) {
                queue.put(new PropertyValueCount(label.name(), key, value, count));
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isIndexCoveringProperty(IndexDefinition indexDefinition, String properttyKeyName) {
        try (Transaction threadTx = db.beginTx()) {
            threadTx.commit();
            return properttyKeyName.isEmpty() || contains(indexDefinition.getPropertyKeys(), properttyKeyName);
        }
    }

    private boolean contains(Iterable<String> list, String search) {
        for (String element: list) {
            if (element.equals(search)) {
                return true;
            }
        }
        return false;
    }

    public static class PropertyValueCount {
        public String label;
        public String key;
        public Object value;
        public long count;

        public PropertyValueCount(String label, String key, Object value, long count) {
            this.label = label;
            this.key = key;
            this.value = value;
            this.count = count;
        }

        @Override
        public String toString() {
            return "PropertyValueCount{" +
                    "label='" + label + '\'' +
                    ", key='" + key + '\'' +
                    ", value='" + value + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
