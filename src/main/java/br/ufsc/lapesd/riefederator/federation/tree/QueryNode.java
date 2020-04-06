package br.ufsc.lapesd.riefederator.federation.tree;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.TermAnnotation;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.riefederator.query.modifiers.Modifier;
import br.ufsc.lapesd.riefederator.query.modifiers.Projection;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.webapis.description.AtomInputAnnotation;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.function.BiConsumer;

import static br.ufsc.lapesd.riefederator.federation.tree.TreeUtils.setMinus;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toSet;

public class QueryNode extends AbstractPlanNode {
    private final @Nonnull
    TPEndpoint endpoint;
    private final @Nonnull CQuery query;
    private @Nullable Set<String> allVars, resultVars, reqInputs, optInputs;

    public QueryNode(@Nonnull TPEndpoint endpoint, @Nonnull CQuery query) {
        this(endpoint, query, Cardinality.UNSUPPORTED);
    }

    public QueryNode(@Nonnull TPEndpoint endpoint, @Nonnull CQuery query,
                     @Nonnull Cardinality cardinality) {
        super(cardinality, null);
        this.endpoint = endpoint;
        this.query = query;
        assertAllInvariants();
    }

    public QueryNode(@Nonnull TPEndpoint endpoint, @Nonnull CQuery query,
                     @Nonnull Set<String> projection) {
        this(endpoint, query, projection, Cardinality.UNSUPPORTED);
    }

    public QueryNode(@Nonnull TPEndpoint endpoint, @Nonnull CQuery q,
                     @Nonnull Set<String> projection,
                     @Nonnull Cardinality cardinality) {
        super(cardinality, projection);

        Set<Var> termVars = q.getTermVars();
        checkArgument(projection.size() <= termVars.size(),
                      "Some projected variables are not result variables");
        assert termVars.stream().map(Var::getName).collect(toSet()).containsAll(projection);
        assert projection.size() < termVars.size() : "Projecting all result variables is useless";

        this.endpoint = endpoint;
        this.query = q;
        assertAllInvariants();
    }

    public @Nonnull TPEndpoint getEndpoint() {
        return endpoint;
    }

    public @Nonnull CQuery getQuery() {
        return query;
    }

    @Override
    public @Nonnull Set<String> getAllVars() {
        if (allVars == null)
            allVars = query.getVars().stream().map(Var::getName).collect(toSet());
        return allVars;
    }

    @Override
    public @Nonnull Set<String> getResultVars() {
        if (projection != null)
            return projection;
        if (resultVars == null)
            resultVars = query.getTermVars().stream().map(Var::getName).collect(toSet());
        return resultVars;
    }

    private Set<String> scanInputs(boolean required) {
        if (query.hasTermAnnotations()) {
            Set<String> set = new HashSet<>(getResultVars().size());
            boolean has = query.forEachTermAnnotation(AtomInputAnnotation.class, (t, a) -> {
                if (t.isVar() && a.isRequired() == required)
                    set.add(t.asVar().getName());
            });
            return has ? set : Collections.emptySet();
        }
        return Collections.emptySet();
    }

    @Override
    public @Nonnull Set<String> getRequiredInputVars() {
        if (reqInputs == null)
            reqInputs = scanInputs(true);
        return reqInputs;
    }

    @Override
    public @Nonnull Set<String> getOptionalInputVars() {
        if (optInputs == null)
            optInputs = scanInputs(false);
        assert TreeUtils.intersect(optInputs, getRequiredInputVars()).isEmpty();
        return optInputs;
    }

    @Contract("_, _, !null -> !null")
    private Term bind(@Nonnull Term term, @Nonnull Solution solution, Term fallback) {
        return term.isVar() ? solution.get(term.asVar().getName(), fallback) : fallback;
    }

    private @Nonnull Triple bind(@Nonnull Triple t, @Nonnull Solution solution) {
        Term s = bind(t.getSubject(), solution, null);
        Term p = bind(t.getPredicate(), solution, null);
        Term o = bind(t.getObject(), solution, null);
        if (s == null && p == null && o == null)
            return t;
        return new Triple(s == null ? t.getSubject()   : s,
                          p == null ? t.getPredicate() : p,
                          o == null ? t.getObject()    : o);
    }

    @Override
    public @Nonnull QueryNode createBound(@Nonnull Solution s) {
        CQuery q = getQuery();
        ArrayList<Triple> bound = new ArrayList<>(q.size());
        for (Triple t : q)
            bound.add(bind(t, s));

        CQuery.WithBuilder builder = CQuery.with(bound);
        q.forEachTermAnnotation((t, a) -> {
            if (!t.isVar() || !s.has(t.asVar().getName()))
                builder.annotate(bind(t, s, t), a);
        });
        q.forEachTripleAnnotation((t, a) -> builder.annotate(bind(t, s), a));
        for (Modifier m : q.getModifiers()) {
            switch (m.getCapability()) {
                case PROJECTION:
                    for (String n : ((Projection) m).getVarNames()) {
                        if (!s.has(n)) builder.project(n);
                    }
                    break;
                case SPARQL_FILTER:
                    builder.modifier(((SPARQLFilter)m).bind(s));
                    break;
                default:
                    builder.modifier(m);
            }
        }
        q = builder.build();


        Cardinality card = getCardinality();
        Set<String> projection = this.projection;
        if (this.projection != null) {
            projection = setMinus(this.projection, s.getVarNames());
            if (q.getTermVars().size() == projection.size())
                projection = null;
        }
        QueryNode boundNode = projection == null ? new QueryNode(endpoint, q, card)
                                                 : new QueryNode(endpoint, q, projection, card);
        boundNode.addBoundFiltersFrom(getFilers(), s);
        return boundNode;
    }

    @Override
    public @Nonnull QueryNode replacingChildren(@Nonnull Map<PlanNode, PlanNode> map)
            throws IllegalArgumentException {
        checkArgument(map.isEmpty());
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
