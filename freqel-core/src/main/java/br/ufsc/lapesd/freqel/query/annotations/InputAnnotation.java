package br.ufsc.lapesd.freqel.query.annotations;


import br.ufsc.lapesd.freqel.model.term.Term;
import com.google.errorprone.annotations.Immutable;

/**
 * Denotes that certain term is to be considered an input. This impllies usage of bind joins
 * instead of hash joins, since the term must be bound before the query can be executed.
 *
 * The mere presence of a instance of this interface denotes the annotated term is an input
 */
@Immutable
public interface InputAnnotation extends TermAnnotation {
    /** Indicates whether the annotated {@link Term} is an input. */
    boolean isInput();

    /** Not all inputs are required. */
    boolean isRequired();

    /**
     * If true, the result may miss the term annotaed with this {@link InputAnnotation}.
     */
    boolean isMissingInResult();
}
