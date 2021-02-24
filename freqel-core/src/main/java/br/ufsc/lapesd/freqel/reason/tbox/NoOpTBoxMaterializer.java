package br.ufsc.lapesd.freqel.reason.tbox;

import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.stream.Stream;

public class NoOpTBoxMaterializer implements TBoxMaterializer {
    @Inject public NoOpTBoxMaterializer() { }

    @Override public void load(@Nonnull TBoxSpec ignored) { }
    @Override public @Nullable TPEndpoint getEndpoint() { return null; }

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

    @Override public boolean isSubProperty(@Nonnull Term subProperty, @Nonnull Term superProperty) {
        return subProperty.equals(superProperty);
    }
    @Override public boolean isSubClass(@Nonnull Term subClass, @Nonnull Term superClass) {
        return subClass.equals(superClass);
    }

    @Override public void close() { }
}
