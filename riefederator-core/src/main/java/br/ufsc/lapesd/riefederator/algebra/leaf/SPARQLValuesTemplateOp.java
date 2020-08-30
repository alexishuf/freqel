package br.ufsc.lapesd.riefederator.algebra.leaf;

import br.ufsc.lapesd.riefederator.algebra.AbstractOp;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.model.FastSPARQLString;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.riefederator.query.modifiers.ModifiersSet;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.modifiers.ValuesModifier;
import br.ufsc.lapesd.riefederator.query.results.Solution;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

public class SPARQLValuesTemplateOp extends AbstractOp {
    private static final Pattern DISTINCT_RX = Pattern.compile("(?i)^\\s*SELECT\\s*DISTINCT");

    private final @Nonnull TPEndpoint endpoint;
    private final @Nonnull String template;
    private final @Nonnull Set<String> vars;
    private final boolean ask, distinct;
    private @Nullable ValuesModifier values;
    private @Nullable Collection<String> varNames;
    private @Nullable Collection<Solution> assignments;

    public SPARQLValuesTemplateOp(@Nonnull TPEndpoint endpoint, @Nonnull String template,
                                  @Nonnull Set<String> vars) {
        this.endpoint = endpoint;
        this.template = template;
        this.vars = vars;
        this.ask = vars.isEmpty();
        this.distinct = DISTINCT_RX.matcher(template).find();
        assertAllInvariants();
    }

    public SPARQLValuesTemplateOp(@Nonnull TPEndpoint endpoint, @Nonnull CQuery query) {
        this.endpoint = endpoint;
        StringBuilder b = new StringBuilder(query.size()*60);
        distinct = query.getModifiers().distinct() != null;
        ask = query.attr().isAsk();
        vars = query.attr().publicTripleVarNames();
        FastSPARQLString.writeHeader(b, ask, distinct, vars);
        FastSPARQLString.writeTriples(b, query);

        for (SPARQLFilter filter : query.getModifiers().filters())
            b.append(filter.getSparqlFilter()).append(' ');

        template = b.append('}').toString();
        assertAllInvariants();
    }

    public @Nonnull SPARQLValuesTemplateOp withEndpoint(@Nonnull TPEndpoint endpoint) {
        SPARQLValuesTemplateOp copy = new SPARQLValuesTemplateOp(endpoint, template, vars);
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
    public @Nonnull ModifiersSet modifiers() {
        return ModifiersSet.EMPTY;
    }

    @Override
    public @Nonnull Set<String> getResultVars() {
        cacheHit = true;
        return vars;
    }

    @Override
    public @Nonnull Set<String> getAllVars() {
        cacheHit = true;
        return vars;
    }

    @Override
    public @Nonnull Set<Triple> getMatchedTriples() {
        cacheHit = true;
        throw new UnsupportedOperationException();
    }

    @Override
    public @Nonnull Op createBound(@Nonnull Solution solution) {
        throw new UnsupportedOperationException();
    }

    @Override
    public @Nonnull Op flatCopy() {
        throw new UnsupportedOperationException();
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

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) return false;
        SPARQLValuesTemplateOp rhs = (SPARQLValuesTemplateOp) obj;
        return getEndpoint().equals(rhs.getEndpoint())
                && template.equals(rhs.template)
                && Objects.equals(values, rhs.values)
                && Objects.equals(varNames, rhs.varNames)
                && Objects.equals(assignments, rhs.assignments);
    }

    @Override
    public int hashCode() {
        int code = super.hashCode();
        code = 37*code + getEndpoint().hashCode();
        code = 37*code + template.hashCode();
        code = 37*code + Objects.hashCode(values);
        code = 37*code + Objects.hashCode(varNames);
        return 37*code + Objects.hashCode(assignments);
    }
}
