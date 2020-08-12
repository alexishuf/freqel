package br.ufsc.lapesd.riefederator.algebra.leaf;

import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;

import javax.annotation.Nonnull;

public class QueryOp extends FreeQueryOp {
    private final @Nonnull TPEndpoint endpoint;

    public QueryOp(@Nonnull TPEndpoint endpoint, @Nonnull CQuery query) {
        super(query);
        this.endpoint = endpoint;
        assertAllInvariants();
    }

    public @Nonnull TPEndpoint getEndpoint() {
        return endpoint;
    }

    @Override
    public @Nonnull FreeQueryOp withQuery(@Nonnull CQuery query) {
        return new QueryOp(endpoint, query);
    }

    @Override
    protected @Nonnull FreeQueryOp createWith(@Nonnull CQuery query) {
        return new QueryOp(getEndpoint(), query);
    }

    @Override
    public @Nonnull StringBuilder toString(@Nonnull StringBuilder builder) {
        if (isProjected())
            builder.append(getPiWithNames()).append('(');
        builder.append("Q(").append(getEndpoint()).append(", ").append(getQuery()).append(')');
        if (isProjected())
            builder.append(')');
        return builder;
    }

    @Nonnull @Override protected StringBuilder prettyPrintQArgs(@Nonnull StringBuilder b) {
        return b.append(' ').append(getEndpoint());
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && getEndpoint().equals(((QueryOp)obj).getEndpoint());
    }

    @Override
    public int hashCode() {
        return 37*super.hashCode() + getEndpoint().hashCode();
    }
}
