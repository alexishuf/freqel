package br.ufsc.lapesd.riefederator.model;

import br.ufsc.lapesd.riefederator.jena.JenaWrappers;
import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.riefederator.model.term.Lit;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.URI;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.annotations.InputAnnotation;
import br.ufsc.lapesd.riefederator.query.annotations.TermAnnotation;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.modifiers.ValuesModifier;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.util.IndexedSubset;
import br.ufsc.lapesd.riefederator.webapis.description.PureDescriptive;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.XSD;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

import static java.util.stream.Collectors.joining;

public class SPARQLString {
    private static final @Nonnull URI rdfType = JenaWrappers.fromURIResource(RDF.type);
    private static final @Nonnull URI xsdString = JenaWrappers.fromURIResource(XSD.xstring);

    private final boolean distinct, ask;
    private final int limit;
    private final @Nonnull Set<String> varNames;
    private final @Nonnull String sparql;

    public SPARQLString(boolean distinct, boolean ask, int limit, @Nonnull Set<String> varNames,
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

    public SPARQLString(@Nonnull CQuery query) {
        ask = query.attr().isAsk();
        distinct = query.getModifiers().distinct() != null;
        limit = query.attr().limit();
        PrefixDict dict = query.getPrefixDict(StdPrefixDict.STANDARD);
        IndexedSet<String> varNamesIndexedSet = query.attr().publicTripleVarNames();

        StringBuilder b = new StringBuilder(query.size()*60);
        // write body to discover which variables in publicTripleVarNames should be removed
        IndexedSubset<String> missing = varNamesIndexedSet.fullSubset();
        writeTriples(b, query, dict, missing);
        if (!missing.isEmpty()) {
            Set<String> actualVars = varNamesIndexedSet.fullSubset();
            actualVars.removeAll(missing);
            this.varNames = Collections.unmodifiableSet(actualVars);
        } else {
            this.varNames = varNamesIndexedSet;
        }
        String body = b.toString();

        b.setLength(0); //start over
        writePrefixes(b, dict);
        writeHeader(b, ask, distinct, varNames);
        b.append(body); //add the body
        writeFilters(b, query.getModifiers().filters());
        ValuesModifier values = query.getModifiers().valueModifier();
        if (values != null)
            writeValues(b, values.getVarNames(), values.getAssignments(), dict);
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

    public static void writePrefixes(@Nonnull StringBuilder b, @Nonnull PrefixDict dict) {
        for (Map.Entry<String, String> e : dict.entries())
            b.append("PREFIX ").append(e.getKey()).append(": <").append(e.getValue()).append(">\n");
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
        b.append(ask ? "{" : "WHERE {");
    }

    public static void writeTriples(@Nonnull StringBuilder b, @Nonnull CQuery query,
                                    @Nonnull PrefixDict dict, @Nullable Set<String> missing) {
        assert !query.isEmpty();
        for (Triple triple : query) {
            if (omitTriple(triple, query))
                continue;
            removeVarName(triple.getSubject(), missing);
            removeVarName(triple.getPredicate(), missing);
            removeVarName(triple.getObject(), missing);
            b.append(term2SPARQL(triple.getSubject(), dict)).append(' ');
            b.append(term2SPARQL(triple.getPredicate(), dict)).append(' ');
            b.append(term2SPARQL(triple.getObject(), dict)).append(" . ");
        }
    }

    private static void removeVarName(@Nonnull Term term, @Nullable Set<String> varNames) {
        if (varNames != null && term.isVar()) varNames.remove(term.asVar().getName());
    }

    public static boolean omitTriple(@Nonnull Triple triple, @Nonnull CQuery query) {
        if (query.getTripleAnnotations(triple).contains(PureDescriptive.INSTANCE))
            return true;
        return omitTerm(triple.getSubject(), query) || omitTerm(triple.getPredicate(), query)
                                                    || omitTerm(triple.getObject(), query);
    }

    private static boolean omitTerm(@Nonnull Term term, @Nonnull CQuery query) {
        for (TermAnnotation ann : query.getTermAnnotations(term)) {
            if (ann instanceof InputAnnotation && ((InputAnnotation)ann).isMissingInResult())
                return true;
        }
        return false;
    }

    public static void writeFilters(@Nonnull StringBuilder b,
                                     @Nonnull Collection<SPARQLFilter> filters) {
        for (SPARQLFilter filter : filters)
            b.append(filter.getSparqlFilter()).append(' ');
    }

    public static void writeValues(@Nonnull StringBuilder b,
                                   @Nonnull Collection<String> varNames,
                                   @Nonnull Collection<Solution> assignments,
                                   @Nonnull PrefixDict dict) {
        String varList = varNames.stream().map(n -> "?" + n).collect(joining(" "));
        b.append("VALUES ( ").append(varList).append(" ) {");
        for (Solution assignment : assignments) {
            b.append(' ').append("( ");
            for (String var : varNames) {
                Term term = assignment.get(var);
                b.append(term == null ? "UNDEF" : term2SPARQL(term, dict));
                b.append(' ');
            }
            b.append(')');
        }
        b.append('}');
    }

    public static @Nonnull String term2SPARQL(@Nonnull Term t, @Nonnull PrefixDict dict) {
        if (t.isBlank()) {
            String name = t.asBlank().getName();
            return name != null ? "_:"+name : "[]";
        } else if (t.isVar()) {
            String name = t.asVar().getName();
            if (name.isEmpty() || name.charAt(0) == '?')
                throw new IllegalArgumentException("Bad var name: '"+name+"'");
            return "?"+name;
        } else if (t.isLiteral()) {
            Lit lit = t.asLiteral();
            if (lit.getDatatype().equals(xsdString))
                return "\""+RDFUtils.escapeLexicalForm(lit.getLexicalForm())+"\"";
            return RDFUtils.toTurtle(lit, dict);
        } else if (t.isURI()) {
            if (t.equals(rdfType)) return "a";
            return RDFUtils.toTurtle(t.asURI(), dict);
        }
        throw new IllegalArgumentException("Cannot represent "+t+" in SPARQL");
    }
}
