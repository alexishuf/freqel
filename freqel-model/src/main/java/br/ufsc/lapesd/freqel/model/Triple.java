package br.ufsc.lapesd.freqel.model;

import br.ufsc.lapesd.freqel.model.prefix.PrefixDict;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.model.term.Var;
import com.google.errorprone.annotations.Immutable;
import com.google.errorprone.annotations.concurrent.LazyInit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Stream;

@Immutable
public class Triple {
    private final @Nonnull Term subject, predicate, object;
    private @LazyInit int hash = 0;

    public enum Position {
        SUBJ, PRED, OBJ;
        public Position opposite() {
            switch (this) {
                case SUBJ: return OBJ;
                case PRED: return PRED;
                case OBJ: return SUBJ;
            }
            throw new UnsupportedOperationException("Unkown Postion "+this);
        }
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
    public @Nonnull Triple with(@Nonnull Position position, @Nonnull Term term) {
        switch (position) {
            case SUBJ: return withSubject(term);
            case PRED: return withPredicate(term);
            case  OBJ: return withObject(term);
            default: throw new IllegalArgumentException("Bad position="+position);
        }
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

    public @Nullable Position where(@Nonnull Term term) {
        if      (  subject.equals(term)) return Position.SUBJ;
        else if (predicate.equals(term)) return Position.PRED;
        else if (   object.equals(term)) return Position.OBJ;
        else                             return null;
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

    public Stream<Term> stream() {
        return Stream.of(subject, predicate, object);
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
