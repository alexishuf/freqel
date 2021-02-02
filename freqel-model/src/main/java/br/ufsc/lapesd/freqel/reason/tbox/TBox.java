package br.ufsc.lapesd.freqel.reason.tbox;

import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.stream.Stream;

public interface TBox extends AutoCloseable {
    /**
     * An endpoint containing all the entailed triples in this TBox.
     *
     * @return a {@link TPEndpoint} that will live until {@link TBox#close()} containing all
     *         triples entailed by this TBox, or null if the implementation does not support
     *         exposing triples as an endpoint
     */
    @Nullable TPEndpoint getEndpoint();

    /**
     * Get all subclasses of the given term, not including term itself.
     *
     * @param term parent class
     * @return A non-null and distinct stream of subclasses
     */
    @Nonnull Stream<Term> subClasses(@Nonnull Term term);

    /**
     * Same as {@link #subClasses(Term)}, but includes the term in the stream.
     */
    @Nonnull Stream<Term> withSubClasses(@Nonnull Term term);

    /**
     * Get all subproperties of the given term, not including term itself.
     *
     * @param term parent class
     * @return A non-null and distinct stream of subproperties
     */
    @Nonnull Stream<Term> subProperties(@Nonnull Term term);

    /**
     * Same as {@link #subProperties(Term)}, but includes <code>term</code> in the results
     */
    @Nonnull Stream<Term> withSubProperties(@Nonnull Term term);

    /**
     * Checks whether subProperty is a rdfs:subPropertyOf superProperty directly or implicitly.
     *
     * @return true iff <code>?subProperty rdfs:subPropertyOf ?superProperty</code>
     *         is entailed by the TBox. Note that rfds:subPropertyOf is reflexive.
     */
    boolean isSubProperty(@Nonnull Term subProperty, @Nonnull Term superProperty);

    /**
     * Checks whether subProperty is a rdfs:subPropertyOf superProperty directly or implicitly.
     *
     * @return true iff <code>?subProperty rdfs:subPropertyOf ?superProperty</code>
     *         is entailed by the TBox. Note that rdfs:subPropertyOf is reflexive
     */
    default boolean isSuperProperty(@Nonnull Term superProperty, @Nonnull Term subProperty) {
        return isSubProperty(subProperty, superProperty);
    }

    /**
     * Checks whether subClass is a subclass of superClass or is equivalent to superClass.
     *
     * @return true iff <code>?subClass rdfs:subClassOf ?superClass</code> is entailed
     *         by the TBox.
     */
    boolean isSubClass(@Nonnull Term subClass, @Nonnull Term superClass);


    /**
     * Checks whether superClass is a superclass of subClass.
     *
     * @return true iff <code>?subClass rdfs:subClassOf ?superClass</code> is entailed by this
     *         TBox. Note that rdfs:subClassOf is relfexive
     */
    default boolean isSuperClass(@Nonnull Term superClass, @Nonnull Term subClass) {
        return isSubClass(subClass, superClass);
    }

    @Override void close();
}
