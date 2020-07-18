package br.ufsc.lapesd.riefederator.rel.mappings.r2rml.enh;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.stream.Stream;

public interface ReferencingObjectMap extends ObjectMap {
    @Nonnull TriplesMap getParentTriplesMap();
    @Nonnull Iterable<JoinCondition> getJoinConditions();
    @Nonnull Iterator<JoinCondition> getJoinConditionsIterator();
    @Nonnull Stream<JoinCondition> streamJoinConditions();
}
