package br.ufsc.lapesd.riefederator.federation.tree;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.query.*;
import br.ufsc.lapesd.riefederator.query.modifiers.Modifier;
import br.ufsc.lapesd.riefederator.query.modifiers.Projection;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;

public class QueryNode extends AbstractPlanNode {
    private final @Nonnull TPEndpoint endpoint;
    private final @Nonnull CQuery query;

    protected static @Nonnull Set<String> getInputVars(@Nonnull CQuery query) {
        if (!query.hasTermAnnotations())
            return Collections.emptySet();
        ImmutableSet.Builder<String> b = ImmutableSet.builder();
        boolean got = query.forEachTermAnnotation(InputAnnotation.class, (t, a) -> {
            if (t.isVar() && a.isInput()) b.add(t.asVar().getName());
        });
        return got ? b.build() : Collections.emptySet();
    }

    public QueryNode(@Nonnull TPEndpoint endpoint, @Nonnull CQuery query) {
        this(endpoint, query, Cardinality.UNSUPPORTED);
    }

    public QueryNode(@Nonnull TPEndpoint endpoint, @Nonnull CQuery query,
                     @Nonnull Cardinality cardinality) {
        this(endpoint, query, query.streamTerms(Var.class).map(Var::getName)
                        .collect(Collectors.toList()), false, cardinality);
    }

    public QueryNode(@Nonnull TPEndpoint endpoint, @Nonnull CQuery query,
                     @Nonnull Collection<String> varNames) {
        this(endpoint, query, varNames, true, Cardinality.UNSUPPORTED);
    }

    public QueryNode(@Nonnull TPEndpoint endpoint, @Nonnull CQuery query,
                     @Nonnull Collection<String> varNames, boolean projecting,
                     @Nonnull Cardinality cardinality) {
        super(varNames, projecting, getInputVars(query), Collections.emptyList(), cardinality);
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

    @Contract("_, _, !null -> !null")
    private Term bind(@Nonnull Term term, @Nonnull Solution solution, Term fallback) {
        return term.isVar() ? solution.get(term.asVar().getName(), fallback) : fallback;
    }

    private @Nonnull Triple bind(@Nonnull Triple t, @Nonnull Solution solution,
                                 @Nullable AtomicBoolean changed) {
        Term s = bind(t.getSubject(), solution, null);
        Term p = bind(t.getPredicate(), solution, null);
        Term o = bind(t.getObject(), solution, null);
        if (s == null && p == null && o == null)
            return t;
        if (changed != null)
            changed.set(true);
        return new Triple(s == null ? t.getSubject()   : s,
                          p == null ? t.getPredicate() : p,
                          o == null ? t.getObject()    : o);
    }

    private @Nonnull Triple bind(@Nonnull Triple t, @Nonnull Solution solution) {
        return bind(t, solution, null);
    }

    @Override
    public @Nonnull QueryNode createBound(@Nonnull Solution solution) {
        CQuery q = getQuery();
        ArrayList<Triple> bound = new ArrayList<>(q.size());
        AtomicBoolean changed = new AtomicBoolean(false);
        for (Triple t : q)
            bound.add(bind(t, solution, changed));

        if (changed.get()) {
            CQuery.WithBuilder builder = CQuery.with(bound);
            q.forEachTermAnnotation((t, a) -> builder.annotate(bind(t, solution, t), a));
            q.forEachTripleAnnotation((t, a) -> builder.annotate(bind(t, solution), a));
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

        return new QueryNode(getEndpoint(), q, projection, projecting, getCardinality());
    }

    @Override
    public @Nonnull QueryNode replacingChildren(@Nonnull Map<PlanNode, PlanNode> map)
            throws IllegalArgumentException {
        Preconditions.checkArgument(map.isEmpty());
        return this;
    }

    @Override
    public @Nonnull Set<Triple> getMatchedTriples() {
        return getQuery().getMatchedTriples();
    }

    @Override
    public <T extends TermAnnotation>
    boolean forEachTermAnnotation(@Nonnull Class<T> cls, @Nonnull BiConsumer<Term, T> consumer) {
        return getQuery().forEachTermAnnotation(cls, consumer);
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
