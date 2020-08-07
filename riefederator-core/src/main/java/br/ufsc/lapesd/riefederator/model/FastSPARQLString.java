package br.ufsc.lapesd.riefederator.model;

import br.ufsc.lapesd.riefederator.jena.JenaWrappers;
import br.ufsc.lapesd.riefederator.model.term.Lit;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.URI;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.annotations.InputAnnotation;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.modifiers.ValuesModifier;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.webapis.description.PureDescriptive;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.XSD;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.Set;

import static java.util.stream.Collectors.joining;

public class FastSPARQLString {
    private static final @Nonnull URI rdfType = JenaWrappers.fromURIResource(RDF.type);
    private static final @Nonnull URI xsdString = JenaWrappers.fromURIResource(XSD.xstring);

    private final boolean distinct, ask;
    private final int limit;
    private final @Nonnull Set<String> varNames;
    private final @Nonnull String sparql;

    public FastSPARQLString(boolean distinct, boolean ask, int limit, @Nonnull Set<String> varNames,
                            @Nonnull String sparql) {
        assert !distinct || !ask : "If DISTINCT, cannot be ASK";
        assert !ask || varNames.isEmpty() : "If ASK, cannot have vars";
        assert !varNames.isEmpty() || ask : "If no vars, should be ASK";
        this.distinct = distinct;
        this.ask = ask;
        this.limit = limit;
        this.varNames = varNames;
        this.sparql = sparql;
    }

    public FastSPARQLString(@Nonnull CQuery query) {
        assert canWrite(query);
        ask = query.attr().isAsk();
        distinct = query.getModifiers().distinct() != null;
        limit = query.attr().limit();
        varNames = query.attr().publicTripleVarNames();

        StringBuilder b = new StringBuilder(query.size()*60);
        writeHeader(b, ask, distinct, varNames);
        writeTriples(b, query);
        writeFilters(b, query.getModifiers().filters());
        ValuesModifier values = query.getModifiers().valueModifier();
        if (values != null)
            writeValues(b, values.getVarNames(), values.getAssignments());
        b.append('}'); // ends SELECT/ASK
        if (limit > 0)
            b.append(" LIMIT ").append(limit);
        sparql = b.toString();
    }

    public boolean isDistinct() {
        return distinct;
    }

    public boolean isAsk() {
        return ask;
    }

    public boolean hasLimit() {
        return limit > 0;
    }

    public int getLimit() {
        if (limit <= 0) throw new NoSuchElementException("No LIMIT defined");
        return limit;
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
        boolean[] ok = {!query.hasTripleAnnotations(PureDescriptive.class)};
        if (ok[0]) {
            query.forEachTermAnnotation(InputAnnotation.class, (t, a) -> {
                if (a.isMissingInResult())
                    ok[0] = false;
            });
        }
        return ok[0];
    }

}
