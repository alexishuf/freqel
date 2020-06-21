package br.ufsc.lapesd.riefederator.model;

import br.ufsc.lapesd.riefederator.jena.JenaWrappers;
import br.ufsc.lapesd.riefederator.model.term.Lit;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.URI;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.InputAnnotation;
import br.ufsc.lapesd.riefederator.query.TermAnnotation;
import br.ufsc.lapesd.riefederator.query.modifiers.*;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.webapis.description.PureDescriptive;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.XSD;

import javax.annotation.Nonnull;
import java.util.*;

import static java.util.stream.Collectors.joining;

public class FastSPARQLString {
    private static final @Nonnull URI rdfType = JenaWrappers.fromURIResource(RDF.type);
    private static final @Nonnull URI xsdString = JenaWrappers.fromURIResource(XSD.xstring);

    private final boolean distinct, ask;
    private final @Nonnull Set<String> varNames;
    private final @Nonnull String sparql;

    public FastSPARQLString(boolean distinct, boolean ask, @Nonnull Set<String> varNames,
                            @Nonnull String sparql) {
        assert !distinct || !ask : "If DISTINCT, cannot be ASK";
        assert !ask || varNames.isEmpty() : "If ASK, cannot have vars";
        assert !varNames.isEmpty() || ask : "If no vars, should be ASK";
        this.distinct = distinct;
        this.ask = ask;
        this.varNames = varNames;
        this.sparql = sparql;
    }

    public FastSPARQLString(@Nonnull CQuery query) {
        assert canWrite(query);
        boolean distinct = false;
        Set<String> varNames = null;
        List<SPARQLFilter> filters = new ArrayList<>();
        List<ValuesModifier> valuesList = new ArrayList<>();
        for (Modifier m : query.getModifiers()) {
            if (m instanceof Distinct) distinct = true;
            else if (m instanceof Projection) varNames = ((Projection)m).getVarNames();
            else if (m instanceof SPARQLFilter) filters.add((SPARQLFilter)m);
            else if (m instanceof ValuesModifier) valuesList.add((ValuesModifier) m);
        }
        ask = query.isAsk();
        this.distinct = distinct;
        if (ask) {
            varNames = Collections.emptySet();
        } else if (varNames == null) {
            Collection<Var> vars = query.getTermVars();
            varNames = new HashSet<>((int)Math.ceil(vars.size()/0.75)+1);
            for (Var var : vars) varNames.add(var.getName());
        }

        StringBuilder b = new StringBuilder(query.size()*60);
        this.varNames = varNames;
        writeHeader(b, ask, distinct, varNames);
        writeTriples(b, query);
        writeFilters(b, filters);
        for (ValuesModifier values : valuesList)
            writeValues(b, values.getVarNames(), values.getAssignments());
        b.append('}'); // ends SELECT/ASK
        sparql = b.toString();
    }

    public boolean isDistinct() {
        return distinct;
    }

    public boolean isAsk() {
        return ask;
    }

    public @Nonnull Set<String> getVarNames() {
        return varNames;
    }

    public @Nonnull String getSparql() {
        return sparql;
    }

    public static void writeHeader(@Nonnull StringBuilder b, boolean ask, boolean distinct,
                                   @Nonnull Collection<String> varNames) {
        assert !ask || varNames.isEmpty() : "If ASK, cannot have vars";
        assert !varNames.isEmpty() || ask : "If has no vars, should be ASK";
        assert !distinct || !ask : "If DISTINCT, cannot be ASk";
        b.append(ask ? "ASK " : "SELECT ");
        if (distinct) b.append("DISTINCT ");
        for (String n : varNames)
            b.append('?').append(n).append(' ');
        b.append("WHERE {");
    }

    public static void writeTriples(@Nonnull StringBuilder b,
                                     @Nonnull Collection<Triple> triples) {
        assert !triples.isEmpty();
        for (Triple triple : triples) {
            b.append(term2SPARQL(triple.getSubject())).append(' ');
            b.append(term2SPARQL(triple.getPredicate())).append(' ');
            b.append(term2SPARQL(triple.getObject())).append(" . ");
        }
    }

    public static void writeFilters(@Nonnull StringBuilder b,
                                     @Nonnull Collection<SPARQLFilter> filters) {
        for (SPARQLFilter filter : filters)
            b.append(filter.getSparqlFilter()).append(' ');
    }

    public static void writeValues(@Nonnull StringBuilder b,
                                   @Nonnull Collection<String> varNames,
                                   @Nonnull Collection<Solution> assignments) {
        String varList = varNames.stream().map(n -> "?" + n).collect(joining(" "));
        b.append("VALUES ( ").append(varList).append(" ) {");
        for (Solution assignment : assignments) {
            b.append(' ').append("( ");
            for (String var : varNames) {
                Term term = assignment.get(var);
                b.append(term == null ? "UNDEF" : term2SPARQL(term));
                b.append(' ');
            }
            b.append(')');
        }
        b.append('}');
    }

    public static @Nonnull String term2SPARQL(@Nonnull Term t) {
        if (t.isBlank()) {
            String name = t.asBlank().getName();
            return name != null ? "_:"+name : "[]";
        } else if (t.isVar()) {
            String name = t.asVar().getName();
            return "?"+name;
        } else if (t.isLiteral()) {
            Lit lit = t.asLiteral();
            if (lit.getDatatype().equals(xsdString))
                return "\""+RDFUtils.escapeLexicalForm(lit.getLexicalForm())+"\"";
            return RDFUtils.toNT(lit);
        } else if (t.isURI()) {
            if (t.equals(rdfType)) return "a";
            return RDFUtils.toNT(t.asURI());
        }
        throw new IllegalArgumentException("Cannot represent "+t+" in SPARQL");
    }

    public static boolean canWrite(@Nonnull CQuery query) {
        boolean bad = query.stream().flatMap(t -> query.getTripleAnnotations(t).stream())
                           .anyMatch(PureDescriptive.class::isInstance);
        if (bad)
            return false;
        for (Term term : query.getTerms()) {
            for (TermAnnotation ann : query.getTermAnnotations(term)) {
                if (ann instanceof InputAnnotation && ((InputAnnotation) ann).isMissingInResult())
                    return false;
            }
        }
        return true;
    }

}