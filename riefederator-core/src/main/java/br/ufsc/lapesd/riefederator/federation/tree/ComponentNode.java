package br.ufsc.lapesd.riefederator.federation.tree;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.annotations.TermAnnotation;
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

public class ComponentNode extends AbstractPlanNode {
    protected final @Nonnull CQuery query;
    protected @Nullable Set<String> allVars, resultVars, reqInputs, optInputs;

    public ComponentNode(@Nonnull CQuery query, @Nullable Set<String> projection,
                         @Nonnull Cardinality cardinality) {
        super(cardinality, projection);
        this.query = query;
        if (projection != null) {
            Set<Var> termVars = query.getTermVars();
            checkArgument(projection.size() <= termVars.size(),
                    "Some projected variables are not result variables");
            assert termVars.stream().map(Var::getName).collect(toSet()).containsAll(projection);
            assert projection.size() < termVars.size() : "Projecting all result variables is useless";
        }
        assertAllInvariants();
    }

    public ComponentNode(@Nonnull CQuery query) {
        this(query, null, Cardinality.UNSUPPORTED );
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
                if (t.isVar() && a.isRequired() == required && !a.isOverride())
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

    protected static class BindData {
        public final @Nonnull CQuery query;
        public final @Nullable Set<String> projection;

        public BindData(@Nonnull CQuery query, @Nullable Set<String> projection) {
            this.query = query;
            this.projection = projection;
        }
    }

    @Contract("_, _, !null -> !null")
    protected Term bind(@Nonnull Term term, @Nonnull Solution solution, Term fallback) {
        return term.isVar() ? solution.get(term.asVar().getName(), fallback) : fallback;
    }

    protected @Nonnull Triple bind(@Nonnull Triple t, @Nonnull Solution solution) {
        Term s = bind(t.getSubject(), solution, null);
        Term p = bind(t.getPredicate(), solution, null);
        Term o = bind(t.getObject(), solution, null);
        if (s == null && p == null && o == null)
            return t;
        return new Triple(s == null ? t.getSubject()   : s,
                p == null ? t.getPredicate() : p,
                o == null ? t.getObject()    : o);
    }

    protected @Nonnull BindData createBindData(@Nonnull Solution s) {
        CQuery q = getQuery();
        ArrayList<Triple> bound = new ArrayList<>(q.size());
        for (Triple t : q)
            bound.add(bind(t, s));

        CQuery.WithBuilder builder = CQuery.with(bound);
        Set<Term> nonFilterTerms = q.stream().flatMap(Triple::stream).collect(toSet());
        Set<Term> filterTerms = q.getModifiers().stream().filter(SPARQLFilter.class::isInstance)
                .flatMap(m -> ((SPARQLFilter) m).getTerms().stream()).collect(toSet());
        q.forEachTermAnnotation((t, a) -> {
            Term boundTerm = bind(t, s, t);
            if (filterTerms.contains(t))
                builder.annotate(SPARQLFilter.generalizeAsBind(boundTerm), a);
            if (nonFilterTerms.contains(t))
                builder.annotate(boundTerm, a);
            assert filterTerms.contains(t) || nonFilterTerms.contains(t);
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


        Set<String> projection = this.projection;
        if (this.projection != null) {
            projection = setMinus(this.projection, s.getVarNames());
            if (q.getTermVars().size() == projection.size())
                projection = null;
        }
        return new BindData(q, projection);
    }


    @Override
    public @Nonnull ComponentNode createBound(@Nonnull Solution s) {
        BindData d = createBindData(s);
        ComponentNode boundNode = d.projection == null
                ? new ComponentNode(d.query)
                : new ComponentNode(d.query, d.projection, getCardinality());
        boundNode.addBoundFiltersFrom(getFilters(), s);
        return boundNode;
    }

    @Override
    public @Nonnull ComponentNode replacingChildren(@Nonnull Map<PlanNode, PlanNode> map)
            throws IllegalArgumentException {
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
        builder.append("Q(").append(getQuery()).append(')');
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
        builder.append("Q(")
                .append(isProjecting() ? ")) " : ")"+getVarNamesString()+" ")
                .append(getName()).append('\n').append(indent2)
                .append(getQuery().toString().replace("\n", "\n"+indent2));
        printFilters(builder, indent2);
        return builder;
    }
}
