package br.ufsc.lapesd.riefederator.reason.tbox;

import br.ufsc.lapesd.riefederator.model.term.Term;

import javax.annotation.Nonnull;
import java.util.stream.Stream;

public interface TBoxReasoner extends AutoCloseable {
    /**
     * Loads the sources into the reasoner.
     *
     * This method dicards any previously loaded TBox. This may also cause pre-materialization
     * by the reasoner. What is pre-materialized (and if something is prematerialized) is up to
     * the {@link TBoxReasoner} implementation.
     *
     * @param sources source TBox
     */
    void load(@Nonnull TBoxSpec sources);

    /**
     * Get all subclasses of the given term, not including term itself.
     *
     * @param term parent class
     * @return A non-null and distinct stream of subclasses
     */
    @Nonnull Stream<Term> subClasses(@Nonnull Term term);

    /**
     * Get all subproperties of the given term, not including term itself.
     *
     * @param term parent class
     * @return A non-null and distinct stream of subproperties
     */
    @Nonnull Stream<Term> subProperties(@Nonnull Term term);
}
