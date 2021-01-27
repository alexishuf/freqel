package br.ufsc.lapesd.freqel.algebra.leaf;

import br.ufsc.lapesd.freqel.algebra.AbstractOp;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.query.modifiers.ModifiersSet;
import br.ufsc.lapesd.freqel.query.results.Solution;
import br.ufsc.lapesd.freqel.util.indexed.IndexSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;

import static br.ufsc.lapesd.freqel.util.CollectionUtils.unmodifiableSet;
import static java.util.Collections.emptySet;

public class EmptyOp extends AbstractOp {
    private @Nullable Op query;
    private @Nonnull Set<String> resultVars;
    private final @Nullable ModifiersSet modifiers;

    public EmptyOp(@Nonnull Collection<String> resultVars) {
        this.resultVars = unmodifiableSet(resultVars);
        this.modifiers = new ModifiersSet();
        assertAllInvariants();
    }

    public EmptyOp(@Nonnull Op query) {
        this(query.getResultVars());
        this.resultVars = unmodifiableSet(resultVars);
        this.query = query;
        assertAllInvariants();
    }

    @Override
    public void offerTriplesUniverse(@Nonnull IndexSet<Triple> universe) {
        if (query != null)
            query.offerTriplesUniverse(universe);
    }

    @Override public @Nullable IndexSet<Triple> getOfferedTriplesUniverse() {
        return query == null ? null : query.getOfferedTriplesUniverse();
    }

    @Override public void offerVarsUniverse(@Nonnull IndexSet<String> universe) {
        if (query != null)
            query.offerVarsUniverse(universe);
    }

    @Override public @Nullable IndexSet<String> getOfferedVarsUniverse() {
        return query == null ? null : query.getOfferedVarsUniverse();
    }

    @Override
    public @Nonnull Set<String> getAllVars() {
        cacheHit = true;
        return resultVars;
    }

    @Override
    public @Nonnull Set<String> getResultVars() {
        cacheHit = true;
        return resultVars;
    }

    @Override
    public @Nonnull Set<Triple> getMatchedTriples() {
        cacheHit = true;
        return query == null ? emptySet() : query.getMatchedTriples();
    }

    @Override
    public @Nonnull ModifiersSet modifiers() {
        return query != null ? query.modifiers() : Objects.requireNonNull(modifiers);
    }

    @Override
    public @Nonnull  Op createBound(@Nonnull Solution solution) {
        return query == null ? new EmptyOp(resultVars) : new EmptyOp(query);
    }

    @Override public @Nonnull Op flatCopy() {
        EmptyOp op = query != null ? new EmptyOp(query) : new EmptyOp(resultVars);
        op.copyCaches(this);
        return op;
    }

    @Override
    public @Nonnull StringBuilder toString(@Nonnull StringBuilder builder) {
        return builder.append("EMPTY").append(getVarNamesString());
    }

    @Override
    public @Nonnull StringBuilder prettyPrint(@Nonnull StringBuilder builder, @Nonnull String indent) {
        return builder.append(indent).append(toString()).append(' ').append(getName());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EmptyOp)) return false;
        if (!super.equals(o)) return false;
        EmptyOp emptyOp = (EmptyOp) o;
        return Objects.equals(query, emptyOp.query) &&
                getResultVars().equals(emptyOp.getResultVars());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), query, getResultVars());
    }
}
