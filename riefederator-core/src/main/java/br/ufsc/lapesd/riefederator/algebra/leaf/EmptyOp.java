package br.ufsc.lapesd.riefederator.algebra.leaf;

import br.ufsc.lapesd.riefederator.algebra.AbstractOp;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.ModifiersSet;
import br.ufsc.lapesd.riefederator.query.results.Solution;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Set;

import static br.ufsc.lapesd.riefederator.util.CollectionUtils.unmodifiableSet;
import static java.util.Collections.emptySet;

public class EmptyOp extends AbstractOp {
    private @Nullable CQuery query;
    private @Nonnull Set<String> resultVars;

    public EmptyOp(@Nonnull Collection<String> resultVars) {
        this.resultVars = unmodifiableSet(resultVars);
        assertAllInvariants();
    }

    public EmptyOp(@Nonnull CQuery query) {
        this(query.attr().publicVarNames());
        this.query = query;
        assertAllInvariants();
    }

    @Override
    public @Nonnull Set<String> getAllVars() {
        return resultVars;
    }

    @Override
    public @Nonnull Set<String> getResultVars() {
        return resultVars;
    }

    @Override
    public @Nonnull Set<Triple> getMatchedTriples() {
        return query == null ? emptySet() : query.attr().matchedTriples();
    }

    @Override
    public @Nonnull ModifiersSet modifiers() {
        return ModifiersSet.EMPTY;
    }

    @Override
    public @Nonnull  Op createBound(@Nonnull Solution solution) {
        return query == null ? new EmptyOp(resultVars) : new EmptyOp(query);
    }

    @Override
    public @Nonnull StringBuilder toString(@Nonnull StringBuilder builder) {
        return builder.append("EMPTY").append(getVarNamesString());
    }

    @Override
    public @Nonnull StringBuilder prettyPrint(@Nonnull StringBuilder builder, @Nonnull String indent) {
        return builder.append(indent).append(toString()).append(' ').append(getName());
    }
}
