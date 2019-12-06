package br.ufsc.lapesd.riefederator.model;

import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.Var;
import com.google.common.base.Objects;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import java.util.function.Consumer;

@Immutable
public class Triple {
    private final @Nonnull Term subject, predicate, object;

    public Triple(@Nonnull Term subject, @Nonnull Term predicate, @Nonnull Term object) {
        this.subject = subject;
        this.predicate = predicate;
        this.object = object;
    }

    public @Nonnull Term getSubject() {
        return subject;
    }
    public @Nonnull Term getPredicate() {
        return predicate;
    }
    public @Nonnull Term getObject() {
        return object;
    }

    /**
     * A triple is bound iff it has no {@link Var} terms
     */
    public boolean isBound() {
        return !(subject instanceof Var) && !(predicate instanceof Var) && !(object instanceof Var);
    }

    public void forEach(@Nonnull Consumer<Term> consumer) {
        consumer.accept(subject);
        consumer.accept(predicate);
        consumer.accept(object);
    }

    @Override
    public @Nonnull String toString() {
        return String.format("(%s %s %s)", getSubject(), getPredicate(), getObject());
    }

    public @Nonnull String toString(@Nonnull PrefixDict dict) {
        return String.format("(%s %s %s)", getSubject().toString(dict),
                getPredicate().toString(dict), getObject().toString(dict));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Triple)) return false;
        Triple triple = (Triple) o;
        return Objects.equal(subject, triple.subject) &&
                Objects.equal(predicate, triple.predicate) &&
                Objects.equal(object, triple.object);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(subject, predicate, object);
    }
}
