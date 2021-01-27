package br.ufsc.lapesd.freqel.model.term;

import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@Immutable
public interface Blank extends Res {
    /**
     * Get an unique node identifier. This allows {@link Blank}s to be compared for identity.
     *
     * Uniqueness of identifiers must be ensured at least up to JVM level. Note that two
     * labeled blank nodes with same label (e.g., _:name in Turtle syntax) are distinct if
     * they originate from different graphs/documents. Therefore such same-name blank nodes
     * must have distinct ids.
     */
    @Nonnull Object getId();

    /**
     * An optional name for the blank node.
     *
     * Some syntaxes, such as Turtle allow this (e.g., _:name would return "name" here).
     * Retaining the name after parsing/processing and returning non-null here is not mandatory.
     */
    @Nullable String getName();
}
