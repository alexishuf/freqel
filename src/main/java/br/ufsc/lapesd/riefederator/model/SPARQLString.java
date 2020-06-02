package br.ufsc.lapesd.riefederator.model;

import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.InputAnnotation;
import br.ufsc.lapesd.riefederator.query.endpoint.Capability;
import br.ufsc.lapesd.riefederator.query.modifiers.Modifier;
import br.ufsc.lapesd.riefederator.query.modifiers.ModifierUtils;
import br.ufsc.lapesd.riefederator.query.modifiers.Projection;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.webapis.description.PureDescriptive;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.regex.Pattern;

import static br.ufsc.lapesd.riefederator.query.endpoint.Capability.ASK;
import static br.ufsc.lapesd.riefederator.query.endpoint.Capability.PROJECTION;

public class SPARQLString {
    public enum Type {
        ASK, SELECT
    }

    static final @Nonnull Pattern SPARQL_VAR_NAME = Pattern.compile("^[a-zA-Z_0-9\\-]+$");
    private final @Nonnull Type type;
    private final @Nonnull String string;
    private final int triplesCount;
    private final @Nonnull ImmutableSet<String> varNames;
    private final @Nonnull ImmutableSet<SPARQLFilter> filters;

    private static boolean keepTriple(@Nonnull Triple triple, @Nonnull CQuery query) {
        if (query.getTripleAnnotations(triple).contains(PureDescriptive.INSTANCE))
            return false;
        boolean missing = triple.stream().anyMatch(t
                -> query.getTermAnnotations(t).stream().anyMatch(a
                    -> a instanceof InputAnnotation && ((InputAnnotation) a).isMissingInResult()));
        return !missing;
    }

    private static @Nonnull Collection<Triple>
    removePureDescriptive(@Nonnull Collection<Triple> triples) {
        if (triples instanceof CQuery) {
            CQuery query = (CQuery) triples;
            List<Triple> list = new ArrayList<>(query.size());
            for (Triple triple : query) {
                if (keepTriple(triple, query))
                    list.add(triple);
            }
            if (list.size() != query.size())
                return list;
        }
        return triples;
    }

    public SPARQLString(@Nonnull Collection<Triple> triples, @Nonnull PrefixDict dict) {
        this(triples, dict,
             triples instanceof CQuery ? ((CQuery)triples).getModifiers() : ImmutableList.of());
    }

    public SPARQLString(@Nonnull Collection<Triple> triples, @Nonnull PrefixDict dict,
                        @Nonnull Collection<Modifier> modifiers) {
        triples = removePureDescriptive(triples);
        Preconditions.checkArgument(!triples.isEmpty(), "triples cannot be empty");
        triplesCount = triples.size();
        // find var names
        Set<String> varNames = new HashSet<>(triples.size() * 2);
        for (Triple triple : triples)
            triple.forEach(t -> {if (t.isVar()) varNames.add(t.asVar().getName());});
        this.varNames = ImmutableSet.copyOf(varNames);
        filters = getFilters(triples);

        // add prefixes
        StringBuilder b = new StringBuilder(triples.size()*32);
        dict.forEach((name, uri) -> {
            if (SPARQL_VAR_NAME.matcher(name).matches())
                b.append("PREFIX ").append(name).append(": <").append(uri).append("> \n");
        });
        if (b.length() > 0) b.append('\n');

        // add query command
        type = this.varNames.isEmpty() || ModifierUtils.getFirst(ASK, modifiers) != null
                ? Type.ASK : Type.SELECT;
        if (type == Type.ASK) {
            b.append("ASK {\n");
        } else {
            b.append("SELECT");
            if (ModifierUtils.getFirst(Capability.DISTINCT, modifiers) != null)
                b.append(" DISTINCT");
            Projection project = (Projection)ModifierUtils.getFirst(PROJECTION, modifiers);
            if (project != null) {
                for (String name : project.getVarNames()) {
                    if (this.varNames.contains(name))
                        b.append(" ?").append(name);
                }
            } else {
                for (String name : this.varNames) b.append(" ?").append(name);
            }
            b.append(" WHERE {\n");
        }

        // add BGP
        writeBGP(triples, dict, b);
        this.string = b.append("}\n").toString();
    }

    private void writeBGP(@Nonnull Collection<Triple> triples, @Nonnull PrefixDict dict,
                          @Nonnull StringBuilder b) {
        SetMultimap<Term, SPARQLFilter> term2filter = HashMultimap.create();
        for (SPARQLFilter filter : filters)
            filter.getTerms().forEach(t -> term2filter.put(t, filter));
        Map<SPARQLFilter, Integer> filter2triple = new HashMap<>();
        if (triples instanceof CQuery) {
            CQuery query = (CQuery)triples;
            int idx = -1;
            for (Triple triple : query) {
                int finalIdx = ++idx;
                triple.forEach(t -> {
                    for (SPARQLFilter filter : term2filter.get(t))
                        filter2triple.put(filter, finalIdx);
                });
            }
        }
        List<List<SPARQLFilter>> annotations = new ArrayList<>(triples.size());
        for (int i = 0; i < triples.size(); i++) annotations.add(new ArrayList<>());
        for (Map.Entry<SPARQLFilter, Integer> e : filter2triple.entrySet())
            annotations.get(e.getValue()).add(e.getKey());

        Iterator<List<SPARQLFilter>> aIt = annotations.iterator();
        for (Triple triple : triples) {
            assert  aIt.hasNext();
            triple.forEach(t -> b.append(term2SPARQL(t, dict)).append(" "));
            aIt.next().forEach(ann -> b.append(ann.getSparqlFilter()).append('\n'));
            b.append(".\n");
        }
    }

    static @Nonnull ImmutableSet<SPARQLFilter> getFilters(Collection<Triple> triples) {
        if (!(triples instanceof CQuery)) return ImmutableSet.of();
        Set<SPARQLFilter> set = new HashSet<>();
        for (Modifier modifier : ((CQuery) triples).getModifiers()) {
            if (modifier instanceof SPARQLFilter) set.add((SPARQLFilter)modifier);
        }
        return ImmutableSet.copyOf(set);
    }

    static @Nonnull String term2SPARQL(@Nonnull Term t, @Nonnull PrefixDict dict) {
        if (t.isBlank()) {
            String name = t.asBlank().getName();
            return name != null && SPARQL_VAR_NAME.matcher(name).matches() ? "_:"+name : "[]";
        } else if (t.isVar()) {
            String name = t.asVar().getName();
            Preconditions.checkArgument(SPARQL_VAR_NAME.matcher(name).matches(),
                    name+" cannot be used as a SPARQL variable name");
            return "?"+name;
        } else if (t.isLiteral()) {
            return RDFUtils.toTurtle(t.asLiteral(), dict);
        } else if (t.isURI()) {
            return RDFUtils.toTurtle(t.asURI(), dict);
        }
        throw new IllegalArgumentException("Cannot represent "+t+" in SPARQL");
    }

    public @Nonnull Type getType() {
        return type;
    }
    public @Nonnull String getString() {
        return string;
    }
    public int getTriplesCount() {
        return triplesCount;
    }
    public @Nonnull ImmutableSet<String> getVarNames() {
        return varNames;
    }
    public @Nonnull ImmutableSet<SPARQLFilter> getFilters() {
        return filters;
    }

    @Override
    public @Nonnull String toString() {
        return string;
    }
}
