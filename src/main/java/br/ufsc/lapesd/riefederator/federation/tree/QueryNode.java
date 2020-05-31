package br.ufsc.lapesd.riefederator.federation.tree;

import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.riefederator.query.results.Solution;

import javax.annotation.Nonnull;
import java.util.Set;

public class QueryNode extends ComponentNode {
    private final @Nonnull TPEndpoint endpoint;

    public QueryNode(@Nonnull TPEndpoint endpoint, @Nonnull CQuery query) {
        this(endpoint, query, Cardinality.UNSUPPORTED);
    }

    public QueryNode(@Nonnull TPEndpoint endpoint, @Nonnull CQuery query,
                     @Nonnull Cardinality cardinality) {
        super(query, null, cardinality);
        this.endpoint = endpoint;
        assertAllInvariants();
    }

    public QueryNode(@Nonnull TPEndpoint endpoint, @Nonnull CQuery query,
                     @Nonnull Set<String> projection) {
        this(endpoint, query, projection, Cardinality.UNSUPPORTED);
    }

    public QueryNode(@Nonnull TPEndpoint endpoint, @Nonnull CQuery q,
                     @Nonnull Set<String> projection,
                     @Nonnull Cardinality cardinality) {
        super(q, projection, cardinality);
        this.endpoint = endpoint;
        assertAllInvariants();
    }

    public @Nonnull TPEndpoint getEndpoint() {
        return endpoint;
    }

    @Override
    public @Nonnull QueryNode createBound(@Nonnull Solution s) {
        BindData d = createBindData(s);
        QueryNode boundNode = d.projection == null
                ? new QueryNode(endpoint, d.query, getCardinality())
                : new QueryNode(endpoint, d.query, d.projection, getCardinality());
        boundNode.addBoundFiltersFrom(getFilters(), s);
        return boundNode;
    }

    @Override
    public @Nonnull StringBuilder toString(@Nonnull StringBuilder builder) {
        if (isProjecting())
            builder.append(getPiWithNames()).append('(');
        builder.append("Q(").append(getEndpoint()).append(", ").append(getQuery()).append(')');
        if (isProjecting())
            builder.append(')');
        return builder;
    }

    @Override
    public  @Nonnull StringBuilder prettyPrint(@Nonnull StringBuilder builder,
                                               @Nonnull String indent) {
        String indent2 = indent + "  ";
        builder.append(indent);
        if (isProjecting())
            builder.append(getPiWithNames()).append('(');
        return builder.append("Q(").append(getCardinality()).append(' ').append(getEndpoint())
                .append(isProjecting() ? "))\n" : ")"+getVarNamesString()+"\n")
                .append(indent2)
                .append(getQuery().toString().replaceAll("\n", "\n"+indent2));
    }
}
