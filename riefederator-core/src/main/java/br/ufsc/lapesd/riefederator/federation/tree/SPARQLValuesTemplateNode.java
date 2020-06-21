package br.ufsc.lapesd.riefederator.federation.tree;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.riefederator.query.modifiers.*;
import br.ufsc.lapesd.riefederator.query.results.Solution;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

import static br.ufsc.lapesd.riefederator.model.SPARQLString.term2SPARQL;
import static br.ufsc.lapesd.riefederator.query.endpoint.Capability.DISTINCT;
import static java.util.stream.Collectors.joining;

public class SPARQLValuesTemplateNode extends AbstractPlanNode {
    private final @Nonnull TPEndpoint endpoint;
    private final @Nonnull String template;
    private final @Nonnull Set<String> vars;
    private final boolean ask;
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
    }

    public SPARQLValuesTemplateNode(@Nonnull TPEndpoint endpoint, @Nonnull CQuery query,
                                    @Nonnull List<SPARQLFilter> filters) {
        super(Cardinality.UNSUPPORTED, null);
        this.endpoint = endpoint;
        StringBuilder b = new StringBuilder(query.size()*60);
        ask = query.isAsk();
        b.append(ask ? "ASK " : "SELECT ");
        if (ModifierUtils.getFirst(DISTINCT, query.getModifiers()) != null)
            b.append("DISTINCT ");
        if (ask) {
            this.vars = Collections.emptySet();
        } else {
            this.vars = addVars(query, b);
        }
        b.append("WHERE { ");

        PrefixDict dict = StdPrefixDict.EMPTY;
        for (Triple triple : query) {
            b.append(term2SPARQL(triple.getSubject(), dict)).append(' ');
            b.append(term2SPARQL(triple.getPredicate(), dict)).append(' ');
            b.append(term2SPARQL(triple.getObject(), dict)).append(" . ");
        }
        for (Modifier m : query.getModifiers()) {
            if (!(m instanceof SPARQLFilter)) continue;
            SPARQLFilter filter = (SPARQLFilter) m;
            b.append(filter.getSparqlFilter()).append(' ');
        }
        for (SPARQLFilter filter : filters)
            b.append(filter.getSparqlFilter()).append(' ');
        template = b.append('}').toString();
    }

    private static @Nonnull Set<String> addVars(@Nonnull CQuery query, StringBuilder b) {
        Set<String> vars = new HashSet<>();
        Projection prj = ModifierUtils.getFirst(Projection.class, query.getModifiers());
        if (prj != null) {
            for (String name : prj.getVarNames()) {
                b.append('?').append(name).append(' ');
                vars.add(name);
            }
        } else {
            for (Var v : query.getTermVars()) {
                String name = v.getName();
                b.append('?').append(name).append(' ');
                vars.add(name);
            }
        }
        return vars;
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
        String varList = varNames.stream().map(n -> "?" + n).collect(joining(" "));
        b.append("VALUES ( ").append(varList).append(" ) {");
        for (Solution assignment : assignments) {
            b.append(' ').append("( ");
            for (String var : varNames) {
                Term term = assignment.get(var);
                b.append(term == null ? "UNDEF" : term2SPARQL(term, StdPrefixDict.EMPTY));
                b.append(' ');
            }
            b.append(')');
        }
        b.append('}').append(template, idx, template.length());
        return b.toString();
    }

    public @Nonnull TPEndpoint getEndpoint() {
        return endpoint;
    }

    public boolean isAsk() {
        return ask;
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
