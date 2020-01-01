package br.ufsc.lapesd.riefederator.federation.tree;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.Solution;
import br.ufsc.lapesd.riefederator.query.TPEndpoint;
import br.ufsc.lapesd.riefederator.query.modifiers.Modifier;
import br.ufsc.lapesd.riefederator.query.modifiers.Projection;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import java.util.*;
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
    public @Nonnull PlanNode createBound(@Nonnull Solution solution) {
        Map<Term, Term> map = new HashMap<>();
        solution.forEach((n, t) -> map.put(new StdVar(n), t));

        CQuery q = getQuery();
        boolean change = false;
        ArrayList<Triple> bound = new ArrayList<>(q.size());
        for (Triple t : q) {
            Term s = map.get(t.getSubject());
            Term p = map.get(t.getPredicate());
            Term o = map.get(t.getObject());
            if (s == null && p == null && o == null) {
                bound.add(t);
            } else {
                change = true;
                bound.add(new Triple(s == null ? t.getSubject()   : s,
                                     p == null ? t.getPredicate() : p,
                                     o == null ? t.getObject()    : o));
            }
        }
        if (change) {
            CQuery.WithBuilder builder = CQuery.with(bound);
            for (Modifier m : q.getModifiers()) {
                switch (m.getCapability()) {
                    case PROJECTION:
                        for (String n : ((Projection) m).getVarNames()) {
                            if (!solution.has(n)) builder.project(n);
                        }
                        break;
                    case ASK:
                        builder.ask(m.isRequired());
                        break;
                    case DISTINCT:
                        builder.distinct(m.isRequired());
                        break;
                    default:
                        throw new UnsupportedOperationException("Can't handle "+m.getCapability());
                }
            }
            q = builder.build();
        }

        Set<String> all = q.streamTerms(Var.class).map(Var::getName).collect(toSet());
        Set<String> projection = new HashSet<>(all);
        projection.retainAll(getResultVars());
        boolean projecting = projection.size() < all.size();

        return new QueryNode(getEndpoint(), q, projection, projecting);
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
