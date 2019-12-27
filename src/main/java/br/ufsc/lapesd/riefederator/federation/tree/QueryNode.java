package br.ufsc.lapesd.riefederator.federation.tree;

import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.TPEndpoint;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;

public class QueryNode extends PlanNode {
    private @Nonnull TPEndpoint endpoint;
    private @Nonnull CQuery query;

    public QueryNode(@Nonnull TPEndpoint endpoint, @Nonnull CQuery query) {
        this(endpoint, query, query.streamTerms(Var.class).map(Var::getName)
                                   .collect(Collectors.toList()), false);
    }

    public QueryNode(@Nonnull TPEndpoint endpoint, @Nonnull CQuery query,
                     @Nonnull Collection<String> varNames) {
        this(endpoint, query, varNames, true);
    }

    public QueryNode(@Nonnull TPEndpoint endpoint, @Nonnull CQuery query,
                     @Nonnull Collection<String> varNames, boolean projecting) {
        super(varNames, projecting, Collections.emptyList());
        if (QueryNode.class.desiredAssertionStatus()) { //expensive checks
            Set<String> all = query.streamTerms(Var.class).map(Var::getName).collect(toSet());
            Preconditions.checkArgument(all.containsAll(varNames), "There are extra varNames");
            Preconditions.checkArgument(projecting == !varNames.containsAll(all),
                    "Mismatch between projecting and varNames");
        }
        this.endpoint = endpoint;
        this.query = query;
    }

    public @Nonnull TPEndpoint getEndpoint() {
        return endpoint;
    }

    public @Nonnull CQuery getQuery() {
        return query;
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
}
