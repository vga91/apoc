package apoc.path;

import org.neo4j.graphdb.Relationship;

public class RelMatcher extends LabelMatcher {
    
    private final LabelMatcher nodeLabelMatcher;

    public RelMatcher(LabelMatcher nodeLabelMatcher) {
        this.nodeLabelMatcher = nodeLabelMatcher;
    }

    public boolean matchesRels(Relationship relationship) {
        return nodeLabelMatcher.matchesLabels(relationship.getStartNode(), true)
                && nodeLabelMatcher.matchesLabels(relationship.getEndNode(), true)
                && matchCommon(relationship, true);
    }
}
