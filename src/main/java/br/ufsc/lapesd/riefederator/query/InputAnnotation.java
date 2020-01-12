package br.ufsc.lapesd.riefederator.query;


import br.ufsc.lapesd.riefederator.model.term.Term;
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
}
