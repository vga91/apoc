package apoc.path;

import apoc.util.collection.Iterables;
import apoc.util.collection.Iterators;
import apoc.util.collection.NestingResourceIterator;
import apoc.util.collection.ResourceClosingIterator;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.internal.helpers.collection.NestingIterator;
//import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.memory.MemoryTracker;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.traversal.BranchState;

/**
 * An expander for repeating sequences of relationships. The sequence provided should be a string consisting of
 * relationship type/direction patterns (exactly the same as the `relationshipFilter`), separated by commas.
 * Each comma-separated pattern represents the relationships that will be expanded with each step of expansion, which
 * repeats indefinitely (unless otherwise stopped by `maxLevel`, `limit`, or terminator filtering from the other expander config options).
 * The exception is if `beginSequenceAtStart` is false. This indicates that the sequence should not begin from the start node,
 * but from one node distant. In this case, we may still need a restriction on the relationship used to reach the start node
 * of the sequence, so when `beginSequenceAtStart` is false, then the first relationship step in the sequence given will not
 * actually be used as part of the sequence, but will only be used once to reach the starting node of the sequence.
 * The remaining relationship steps will be used as the repeating relationship sequence.
 */
public class RelationshipSequenceExpander implements PathExpander {

    private final List<List<Pair<RelationshipType, Direction>>> relSequences = new ArrayList<>();
    private List<Pair<RelationshipType, Direction>> initialRels = null;
    private final MemoryTracker memoryTracker;

    public RelationshipSequenceExpander(String relSequenceString, boolean beginSequenceAtStart, MemoryTracker memoryTracker) {
        this.memoryTracker = memoryTracker;

        int index = 0;

        for (String sequenceStep : relSequenceString.split(",")) {
            sequenceStep = sequenceStep.trim();
            Iterable<Pair<RelationshipType, Direction>> relDirIterable = RelationshipTypeAndDirections.parse(sequenceStep);

            List<Pair<RelationshipType, Direction>> stepRels = new ArrayList<>();
            for (Pair<RelationshipType, Direction> pair : relDirIterable) {
                stepRels.add(pair);
            }

            if (!beginSequenceAtStart && index == 0) {
                initialRels = stepRels;
            } else {
                relSequences.add(stepRels);
            }

            index++;
        }
    }

    public RelationshipSequenceExpander(List<String> relSequenceList, boolean beginSequenceAtStart, MemoryTracker memoryTracker) {
        this.memoryTracker = memoryTracker;

        int index = 0;

        for (String sequenceStep : relSequenceList) {
            sequenceStep = sequenceStep.trim();
            List<org.apache.commons.lang3.tuple.Pair<RelationshipType, Direction>> relDirIterable = RelationshipTypeAndDirections.parse(sequenceStep);

            List<org.apache.commons.lang3.tuple.Pair<RelationshipType, Direction>> stepRels = new ArrayList<>();
            for (org.apache.commons.lang3.tuple.Pair<RelationshipType, Direction> pair : relDirIterable) {
                stepRels.add(pair);
            }

            if (!beginSequenceAtStart && index == 0) {
                initialRels = stepRels;
            } else {
                relSequences.add(stepRels);
            }

            index++;
        }
    }

    @Override
    public ResourceIterable<Relationship> expand( Path path, BranchState state ) {
        final Node node = path.endNode();
        final int depth = path.length();
        final List<Pair<RelationshipType, Direction>> stepRels;

        if (depth == 0 && initialRels != null) {
            stepRels = initialRels;
        } else {
            stepRels = relSequences.get((initialRels == null ? depth : depth - 1) % relSequences.size());
        }

        final List<Relationship> relationships = Iterators.asList(
            new NestingIterator<>(
                    stepRels.iterator()) {
                @Override
                protected Iterator<Relationship> createNestedIterator(
                        Pair<RelationshipType, Direction> entry) {
                    RelationshipType type = entry.getLeft();
                    Direction dir = entry.getRight();
                    if (type != null) {
                        return ((dir == Direction.BOTH) ? node.getRelationships(type) :
                                node.getRelationships(dir, type)).iterator();
                    } else {
                        return ((dir == Direction.BOTH) ? node.getRelationships() :
                                node.getRelationships(dir)).iterator();
                    }
                }
            });

        // calculated through HeapEstimator.sizeOfCollection(..)
        memoryTracker.allocateHeap(HeapEstimator.sizeOf(relationships));
        
        return Iterables.asResourceIterable(relationships);
    }

    @Override
    public PathExpander reverse() {
        throw new RuntimeException("Not implemented");
    }
}

