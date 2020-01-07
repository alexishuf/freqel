package br.ufsc.lapesd.riefederator.query;


import com.google.errorprone.annotations.Immutable;

/**
 * Denotes that certain term is to be considered an input. This impllies usage of bind joins
 * instead of hash joins, since the term must be bound before the query can be executed.
 *
 * The mere presence of a instance of this interface denotes the annotated term is an input
 */
@Immutable
public interface InputAnnotation extends TermAnnotation {
    /**
     * Not all inputs are required.
     */
    boolean isRequired();
}
