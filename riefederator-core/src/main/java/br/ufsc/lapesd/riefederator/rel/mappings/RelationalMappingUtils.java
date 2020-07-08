package br.ufsc.lapesd.riefederator.rel.mappings;

import br.ufsc.lapesd.riefederator.description.molecules.Molecule;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.rel.mappings.tags.ColumnTag;

import javax.annotation.Nonnull;
import java.util.Collection;

import static java.util.stream.Collectors.toSet;

public class RelationalMappingUtils {
    /**
     * Get the set of {@link Column} instances that yield the given predicate in the molecule.
     *
     * This relies on annotations inserted on the molecule by implementations of
     * {@link RelationalMapping#createMolecule()} and related overloads. If the molecule is did
     * not originate from such method, then this function will return an empty set always.
     *
     * @param molecule the molecule from where to get the reverse mapping annotations
     * @param predicate the predicate to look up
     * @return A non-null distinct collection of Column instances, without nulls.
     */
    public static @Nonnull Collection<Column> predicate2column(@Nonnull Molecule molecule,
                                                               @Nonnull Term predicate) {
        return molecule.getIndex().stream(null, predicate, null)
                .flatMap(t -> t.getEdgeTags().stream())
                .filter(ColumnTag.class::isInstance)
                .map(t -> ((ColumnTag) t).getColumn())
                .collect(toSet());
    }
}
