package br.ufsc.lapesd.riefederator.model;

import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.Var;
import com.google.errorprone.annotations.Immutable;
import com.google.errorprone.annotations.concurrent.LazyInit;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

@Immutable
public class Triple {
    private final @Nonnull Term subject, predicate, object;
    private @LazyInit int hash = 0;

    public enum Position {
        SUBJ, PRED, OBJ;

        public static List<Position> VALUES_LIST = Arrays.asList(values());
    }
    public Triple(@Nonnull Term subject, @Nonnull Term predicate, @Nonnull Term object) {
        this.subject = subject;
        this.predicate = predicate;
        this.object = object;
    }

    public @Nonnull Triple withSubject(@Nonnull Term term) {
        return new Triple(term, getPredicate(), getObject());
    }
    public @Nonnull Triple withPredicate(@Nonnull Term term) {
        return new Triple(getSubject(), term, getObject());
    }
    public @Nonnull Triple withObject(@Nonnull Term term) {
        return new Triple(getSubject(), getPredicate(), term);
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
    public @Nonnull Term get(@Nonnull Position position) {
        switch (position) {
            case SUBJ: return subject;
            case PRED: return predicate;
            case  OBJ: return object;
        }
        throw new UnsupportedOperationException("Cannot handle get("+position+")");
    }

    /**
     * A triple is bound iff it has no {@link Var} terms
     */
    public boolean isBound() {
        return !(subject instanceof Var) && !(predicate instanceof Var) && !(object instanceof Var);
    }

    /** Executes consumer for each member of the {@link Triple} */
    public void forEach(@Nonnull Consumer<Term> consumer) {
        consumer.accept(subject);
        consumer.accept(predicate);
        consumer.accept(object);
    }

    /** Indicates whether term is a member (<code>equals()</code>) of this triple */
    public boolean contains(@Nonnull Term term) {
        return subject.equals(term) || predicate.equals(term) || object.equals(term);
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
        return Objects.equals(subject, triple.subject) &&
                Objects.equals(predicate, triple.predicate) &&
                Objects.equals(object, triple.object);
    }

    @Override
    public int hashCode() {
        if (hash == 0)
            hash = Objects.hash(subject, predicate, object);
        return hash;
    }
}
