package br.ufsc.lapesd.riefederator.federation.tree;

import br.ufsc.lapesd.riefederator.model.FastSPARQLString;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.riefederator.query.modifiers.Modifier;
import br.ufsc.lapesd.riefederator.query.modifiers.ModifierUtils;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.modifiers.ValuesModifier;
import br.ufsc.lapesd.riefederator.query.results.Solution;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static br.ufsc.lapesd.riefederator.query.endpoint.Capability.DISTINCT;

public class SPARQLValuesTemplateNode extends AbstractPlanNode {
    private static final Pattern DISTINCT_RX = Pattern.compile("(?i)^\\s*SELECT\\s*DISTINCT");

    private final @Nonnull TPEndpoint endpoint;
    private final @Nonnull String template;
    private final @Nonnull Set<String> vars;
    private final boolean ask, distinct;
    private @Nullable ValuesModifier values;
    private @Nullable Collection<String> varNames;
    private @Nullable Collection<Solution> assignments;

    public SPARQLValuesTemplateNode(@Nonnull TPEndpoint endpoint, @Nonnull String template,
                                    @Nonnull Set<String> vars) {
        super(Cardinality.UNSUPPORTED, null);
        this.endpoint = endpoint;
        this.template = template;
        this.vars = vars;
        this.ask = vars.isEmpty();
        this.distinct = DISTINCT_RX.matcher(template).find();
    }

    public SPARQLValuesTemplateNode(@Nonnull TPEndpoint endpoint, @Nonnull CQuery query,
                                    @Nonnull List<SPARQLFilter> filters) {
        super(Cardinality.UNSUPPORTED, null);
        this.endpoint = endpoint;
        StringBuilder b = new StringBuilder(query.size()*60);
        distinct = ModifierUtils.getFirst(DISTINCT, query.getModifiers()) != null;
        ask = query.attr().isAsk();
        vars = query.attr().publicTripleVarNames();
        FastSPARQLString.writeHeader(b, ask, distinct, vars);
        FastSPARQLString.writeTriples(b, query);

        for (Modifier m : query.getModifiers()) {
            if (!(m instanceof SPARQLFilter)) continue;
            SPARQLFilter filter = (SPARQLFilter) m;
            b.append(filter.getSparqlFilter()).append(' ');
        }
        for (SPARQLFilter filter : filters)
            b.append(filter.getSparqlFilter()).append(' ');

        template = b.append('}').toString();
    }

    public @Nonnull SPARQLValuesTemplateNode withEndpoint(@Nonnull TPEndpoint endpoint) {
        SPARQLValuesTemplateNode copy = new SPARQLValuesTemplateNode(endpoint, template, vars);
        copy.values = values;
        return copy;
    }

    public void setValues(@Nullable ValuesModifier values) {
        this.values = values;
    }
    public void setValues(@Nonnull Collection<String> varNames,
                          @Nonnull Collection<Solution> assignments) {
        this.values = null;
        this.varNames = varNames;
        this.assignments = assignments;
    }

    public @Nullable ValuesModifier getValues() {
        return this.values;
    }

    public @Nonnull String createSPARQL() {
        if (values != null)
            return createSPARQL(values);
        else if (varNames != null && assignments != null)
            return createSPARQL(varNames, assignments);
        throw new IllegalStateException();
    }

    public @Nonnull String createSPARQL(@Nonnull ValuesModifier values) {
        return createSPARQL(values.getVarNames(), values.getAssignments());
    }

    public @Nonnull String createSPARQL(@Nonnull Collection<String> varNames,
                                        @Nonnull Collection<Solution> assignments) {
        int idx = template.lastIndexOf('}');
        StringBuilder b = new StringBuilder(template.length() +
                                            assignments.size() * 40);
        b.append(template, 0, idx);
        FastSPARQLString.writeValues(b, varNames, assignments);
        b.append(template, idx, template.length());
        return b.toString();
    }

    public @Nonnull TPEndpoint getEndpoint() {
        return endpoint;
    }

    public boolean isAsk() {
        return ask;
    }

    public boolean isDistinct() {
        return distinct;
    }

    @Override
    public @Nonnull Set<String> getResultVars() {
        return vars;
    }

    @Override
    public @Nonnull Set<String> getAllVars() {
        return vars;
    }

    @Override
    public @Nonnull Set<Triple> getMatchedTriples() {
        throw new UnsupportedOperationException();
    }

    @Override
    public @Nonnull PlanNode createBound(@Nonnull Solution solution) {
        throw new UnsupportedOperationException();
    }

    @Override
    public @Nonnull PlanNode replacingChildren(@Nonnull Map<PlanNode, PlanNode> map) throws IllegalArgumentException {
        return this;
    }

    @Override
    public @Nonnull StringBuilder toString(@Nonnull StringBuilder builder) {
        return builder.append("VTPL(").append(template).append(')');
    }

    @Override
    public @Nonnull StringBuilder prettyPrint(@Nonnull StringBuilder builder, @Nonnull String indent) {
        String indent2 = indent + "  ";
        return builder.append(indent).append("VTPL\n").append(template.replace("\n", "\n"+indent2));
    }
}
