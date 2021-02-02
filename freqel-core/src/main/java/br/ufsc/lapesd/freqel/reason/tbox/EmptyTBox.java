package br.ufsc.lapesd.freqel.reason.tbox;

import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.impl.EmptyEndpoint;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.stream.Stream;

public class EmptyTBox implements TBox {
    private @Nullable TPEndpoint endpoint;

    @Override public @Nonnull TPEndpoint getEndpoint() {
        if (endpoint == null)
            endpoint = new EmptyEndpoint();
        return endpoint;
    }

    @Override public @Nonnull Stream<Term> subClasses(@Nonnull Term term) {
        return Stream.empty();
    }

    @Override public @Nonnull Stream<Term> withSubClasses(@Nonnull Term term) {
        return Stream.of(term);
    }

    @Override public @Nonnull Stream<Term> subProperties(@Nonnull Term term) {
        return Stream.empty();
    }

    @Override public @Nonnull Stream<Term> withSubProperties(@Nonnull Term term) {
        return Stream.of(term);
    }

    @Override public boolean isSubClass(@Nonnull Term subClass, @Nonnull Term superClass) {
        return subClass.equals(superClass);
    }

    @Override public boolean isSubProperty(@Nonnull Term subProperty, @Nonnull Term superProperty) {
        return subProperty.equals(superProperty);
    }

    @Override public void close() {
        /* do nothing */
    }

    @Override public @Nonnull String toString() {
        return String.format("EmptyTBox@%x", System.identityHashCode(this));
    }
}
