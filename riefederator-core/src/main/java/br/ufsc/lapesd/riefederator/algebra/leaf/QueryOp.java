package br.ufsc.lapesd.riefederator.algebra.leaf;

import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.riefederator.query.results.Solution;

import javax.annotation.Nonnull;

public class QueryOp extends UnassignedQueryOp {
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
    public @Nonnull UnassignedQueryOp createBound(@Nonnull Solution s) {
        return new QueryOp(endpoint, bindQuery(s));
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

    @Override
    public  @Nonnull StringBuilder prettyPrint(@Nonnull StringBuilder builder,
                                               @Nonnull String indent) {
        String indent2 = indent + "  ";
        builder.append(indent);
        if (isProjected())
            builder.append(getPiWithNames()).append('(');
        builder.append("Q(").append(getCardinality()).append(' ').append(getEndpoint())
                .append(isProjected() ? "))" : ")"+getVarNamesString())
                .append(' ').append(getName()).append('\n')
                .append(indent2)
                .append(getQuery().toString().replace("\n", "\n"+indent2));
        printFilters(builder, indent2);
        return builder;
    }
}
