package br.ufsc.lapesd.riefederator.federation.tree;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.MutableCQuery;
import br.ufsc.lapesd.riefederator.query.annotations.TermAnnotation;
import br.ufsc.lapesd.riefederator.query.modifiers.Modifier;
import br.ufsc.lapesd.riefederator.query.modifiers.Projection;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.util.CollectionUtils;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.webapis.description.AtomInputAnnotation;
import com.google.common.collect.ImmutableSet;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import static br.ufsc.lapesd.riefederator.util.CollectionUtils.setMinus;
import static com.google.common.base.Preconditions.checkArgument;

public class ComponentNode extends AbstractPlanNode {
    protected final @Nonnull CQuery query;
    protected @Nullable Set<String> reqInputs, optInputs;

    public ComponentNode(@Nonnull CQuery query, @Nullable Set<String> projection,
                         @Nonnull Cardinality cardinality) {
        super(cardinality, projection);
        this.query = query;
        if (projection != null) {
            IndexedSet<String> vars = query.attr().tripleVarNames();
            checkArgument(projection.size() <= vars.size(),
                    "Some projected variables are not result variables");
            assert vars.containsAll(projection);
            assert projection.size() < vars.size() : "Projecting all result variables is useless";
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
        return query.attr().allVarNames();
    }

    @Override
    public @Nonnull Set<String> getResultVars() {
        if (projection != null)
            return projection;
        return query.attr().tripleVarNames();
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
        assert CollectionUtils.intersect(optInputs, getRequiredInputVars()).isEmpty();
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
        MutableCQuery b = new MutableCQuery();
        for (Triple triple : q) {
            Triple bound = bind(triple, s);
            b.add(bound);
            q.getTripleAnnotations(triple).forEach(a -> b.annotate(bound, a));
        }
        for (Modifier modifier : q.getModifiers()) {
            if (modifier instanceof Projection) {
                ImmutableSet.Builder<String> set = ImmutableSet.builder();
                ((Projection) modifier).getVarNames().forEach(n -> {if (!s.has(n)) set.add(n);});
                modifier = new Projection(set.build(), modifier.isRequired());
            } else if (modifier instanceof SPARQLFilter) {
                modifier = ((SPARQLFilter)modifier).bind(s);
            }
            b.addModifier(modifier);
        }
        IndexedSet<Term> tripleTerms = q.attr().tripleTerms();
        q.forEachTermAnnotation((t, a) -> {
            Term boundTerm = bind(t, s, t);
            if (tripleTerms.contains(t)) b.annotate(boundTerm, a);
            else                         b.annotate(SPARQLFilter.generalizeAsBind(boundTerm), a);
        });

        Set<String> projection = this.projection;
        if (this.projection != null) {
            projection = setMinus(this.projection, s.getVarNames());
            if (b.attr().tripleVars().size() == projection.size()) {
                assert b.attr().tripleVarNames().equals(projection);
                projection = null;
            }
        }
        return new BindData(b, projection);
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
        return getQuery().attr().matchedTriples();
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
