package br.ufsc.lapesd.riefederator.rel.common;

import br.ufsc.lapesd.riefederator.description.molecules.tags.AtomTag;
import br.ufsc.lapesd.riefederator.description.molecules.tags.MoleculeLinkTag;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.annotations.TermAnnotation;
import br.ufsc.lapesd.riefederator.query.annotations.TripleAnnotation;
import br.ufsc.lapesd.riefederator.query.modifiers.ModifierUtils;
import br.ufsc.lapesd.riefederator.query.modifiers.Projection;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.rel.mappings.Column;
import br.ufsc.lapesd.riefederator.rel.mappings.tags.ColumnsTag;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.util.IndexedSubset;
import br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation;
import br.ufsc.lapesd.riefederator.webapis.description.MoleculeLinkAnnotation;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class StarVarIndex {
    private final @Nonnull CQuery query;
    private final @Nonnull IndexedSet<SPARQLFilter> allFilters;
    private final @Nonnull IndexedSubset<SPARQLFilter> crossStarFilters;
    private final @Nonnull List<StarSubQuery> stars;
    private final @Nonnull SortedSet<String> outerVars;
    private final @Nonnull List<SortedSet<String>> starProjected;
    private final @Nonnull Map<String, Integer> var2firstStar;
    private final @Nonnull List<Set<StarJoin>> star2joins;
    private final @Nonnull Map<String, Column> var2col;
    private final @Nonnull List<List<Selector>> star2selectors;
    private final @Nonnull List<IndexedSubset<SPARQLFilter>> star2pendingFilters;
    private final @Nonnull List<IndexedSubset<Triple>> star2pendingTriples;

    public StarVarIndex(@Nonnull CQuery query,
                        @Nonnull SelectorFactory selectorFactory) {
        this.query = query;
        this.allFilters = StarsHelper.getFilters(query);
        stars = orderJoinable(StarsHelper.findStars(query, allFilters));
        this.crossStarFilters = allFilters.fullSubset();
        stars.forEach(s -> crossStarFilters.removeAll(s.getFilters()));
        outerVars = new TreeSet<>();
        starProjected = new ArrayList<>(stars.size());
        var2firstStar = Maps.newHashMapWithExpectedSize(stars.size()*2);
        star2selectors = new ArrayList<>(stars.size());
        star2pendingFilters = new ArrayList<>(stars.size());
        star2pendingTriples = new ArrayList<>(stars.size());
        star2joins = new ArrayList<>(stars.size());
        if (stars.isEmpty()) {
            var2col = Collections.emptyMap();
            return;
        }
        Builder builder = new Builder();
        var2col = builder.var2col;
        builder.createSelectors(selectorFactory);
        builder.fillJoinVars();
        builder.fillOuterProjection(query);
        builder.fillStarProjections();
    }

    private class Builder {
        private final SetMultimap<String, String> sparqlVar2Var;
        private final Map<String, Column> var2col;
        private final List<Set<String>> star2vars;
        private final List<Map<Column, String>> star2col2var;
        private final IndexedSubset<Triple> hasNonDirectJoin;
        private final Set<String> nonDirectJoinSparqlVar;
        private int idVarNextId = 0;

        private class SelectorContext implements SelectorFactory.Context {
            private final int starIdx;

            public SelectorContext(int starIdx) {
                this.starIdx = starIdx;
            }
            @Override
            public @Nonnull CQuery getQuery() {
                return query;
            }
            @Override
            public @Nonnull Collection<Column> getColumns(@Nonnull Term term) {
                assert stars.get(starIdx).getTriples().stream().anyMatch(t -> t.contains(term))
                        : "Term is not in the current star";
                return stars.get(starIdx).getAllColumns(term);
            }
            @Override
            public @Nullable Column getDirectMapped(@Nonnull Term term, @Nullable Triple triple) {
                int encounters = 0;
                Column direct = null;
                String table = stars.get(starIdx).findTable();
                if (table == null)
                    return null;
                if (triple != null) {
                    boolean hadTag = false;
                    for (TripleAnnotation a : query.getTripleAnnotations(triple)) {
                        if (!(a instanceof MoleculeLinkAnnotation)) continue;
                        MoleculeLinkAnnotation mla = (MoleculeLinkAnnotation) a;
                        boolean isObject = term.equals(triple.getObject());
                        if (isObject && mla.isReversed()) continue;
                        if (term.equals(triple.getSubject()) && !mla.isReversed()) continue;
                        for (MoleculeLinkTag tag : mla.getLink().getTags()) {
                            if (tag instanceof ColumnsTag) {
                                hadTag = true;
                                ColumnsTag cTag = (ColumnsTag) tag;
                                assert !isObject || cTag.getTable().equals(table);
                                if (cTag.isDirect()) {
                                    ++encounters;
                                    direct = cTag.getColumns().get(0);
                                }
                            }
                        }
                    }
                    if (hadTag)
                        return encounters == 1 ? direct : null;
                }
                for (TermAnnotation a : query.getTermAnnotations(term)) {
                    if (a instanceof AtomAnnotation) {
                        for (AtomTag tag : ((AtomAnnotation) a).getAtom().getTags()) {
                            if (tag instanceof ColumnsTag) {
                                ColumnsTag cTag = (ColumnsTag) tag;
                                if (!cTag.getTable().equals(table))
                                    continue; //unrelated to this star
                                if (cTag.isDirect()) {
                                    ++encounters;
                                    direct = cTag.getColumns().get(0);
                                }
                            }
                        }
                    }
                }
                assert (encounters == 1) == (direct != null);
                return encounters == 1 ? direct : null;
            }
        }

        public Builder() {
            IndexedSet<String> sparqlVars = stars.get(0).getVarNames().getParent();
            sparqlVar2Var = HashMultimap.create(sparqlVars.size(), 2);
            var2col = Maps.newHashMapWithExpectedSize(sparqlVars.size()*2);
            star2vars = new ArrayList<>(stars.size());
            star2col2var = new ArrayList<>(stars.size());
            for (int i = 0, size = stars.size(); i < size; i++) {
                star2vars.add(new HashSet<>());
                star2col2var.add(new HashMap<>());
            }
            hasNonDirectJoin = query.attr().getSet().emptySubset();
            nonDirectJoinSparqlVar = new HashSet<>();
            createVars();
        }

        private void createVars() {
            List<String> termVars = new ArrayList<>();
            query.forEachTermAnnotation(AtomAnnotation.class, (t, a) -> {
                for (AtomTag tag : a.getAtom().getTags()) {
                    if (tag instanceof ColumnsTag) {
                        for (Column column : ((ColumnsTag) tag).getColumns()) {
                            String var = "v" + (idVarNextId++);
                            var2col.put(var, column);
                            termVars.add(var);
                        }
                    }
                }
                if (!termVars.isEmpty()) {
                    boolean isVar = t.isVar();
                    for (int i = 0, size = stars.size(); i < size; i++) {
                        StarSubQuery star = stars.get(i);
                        Map<Column, String> col2var = star2col2var.get(i);
                        String table = star.findTable();
                        assert table != null;
                        boolean has = isVar && star.getVarNames().contains(t.asVar().getName())
                                || star.getTriples().stream().anyMatch(tp -> tp.contains(t));
                        if (!has) continue;
                        for (String v : termVars) {
                            Column col = var2col.get(v);
                            if (!col.getTable().equals(table)) continue;
                            String oldVar = col2var.putIfAbsent(col, v);
                            if (oldVar == null) {
                                star2vars.get(i).add(v);
                                if (isVar)
                                    sparqlVar2Var.put(t.asVar().getName(), v);
                            } else if (isVar) {
                                sparqlVar2Var.put(t.asVar().getName(), oldVar);
                            }
                        }
                    }
                    termVars.clear();
                }
            });
        }

        public void createSelectors(@Nonnull SelectorFactory factory) {
            assert star2pendingFilters.isEmpty();
            for (int i = 0, size = stars.size(); i < size; i++) {
                StarSubQuery star = stars.get(i);
                SelectorContext ctx = new SelectorContext(i);
                List<Selector> selectors = new ArrayList<>();
                IndexedSubset<SPARQLFilter> pendingFilters
                        = star.getAllQueryFilters().emptySubset();
                for (SPARQLFilter filter : star.getFilters()) {
                    Selector selector = factory.create(ctx, filter);
                    if (selector == null) pendingFilters.add(filter);
                    else                  selectors.add(selector);
                }
                star2pendingFilters.add(pendingFilters);
                IndexedSubset<Triple> pendingTriples = star.getTriples().getParent().emptySubset();
                for (Triple triple : star.getTriples()) {
                    Selector selector = factory.create(ctx, triple);
                    if (selector == null) pendingTriples.add(triple);
                    else                  selectors.add(selector);
                }
                star2pendingTriples.add(pendingTriples);
                star2selectors.add(selectors);
            }
        }

        public void fillJoinVars() {
            assert star2joins.isEmpty();
            star2joins.add(Collections.emptySet());
            for (int i = 1, size = stars.size(); i < size; i++) {
                Set<StarJoin> joins = new HashSet<>();
                star2joins.add(joins);
                StarSubQuery star = stars.get(i);
                String table = star.findTable();
                assert table != null : "No table annotation for star";

                for (String sparqlVar : star.getVarNames()) {
                    StdVar svTerm = new StdVar(sparqlVar);
                    ColumnsTag myColumns = star.getColumnsTag(svTerm);
                    if (myColumns == null)
                        continue; //not annotated
                    List<String> mine = myColumns.stream().map(star2col2var.get(i)::get)
                                                 .collect(toList());

                    int prevStar = -1;
                    ColumnsTag theirColumns = null;
                    for (int j = 0; j < i; j++) {
                        StarSubQuery pStar = stars.get(j);
                        if (pStar.getVarNames().contains(sparqlVar)) {
                            ColumnsTag columns = pStar.getColumnsTag(svTerm);
                            if (columns == null)
                                continue; //not annotated
                            prevStar = j;
                            theirColumns = columns;
                            break;
                        }
                    }
                    if (prevStar < 0)
                        continue; //we are the first star, try again later
                    List<String> theirs = theirColumns.stream().map(star2col2var.get(prevStar)::get)
                                                      .collect(toList());
                    if (mine.size() != theirs.size()) {
                        throw new IllegalArgumentException("Cannot join SPARQL var "+sparqlVar+
                                " on star "+i+" with star "+prevStar+" since it expands to "+
                                mine.size()+" columns on "+i+" and to "+theirs.size()+"on "+
                                prevStar);
                    }
                    boolean direct = myColumns.isDirect() && theirColumns.isDirect();
                    for (int j = 0, varsSize = mine.size(); j < varsSize; j++) {
                        joins.add(new StarJoin(prevStar, i, theirs.get(j), mine.get(j),
                                           sparqlVar, direct));
                    }
                    if (!direct) {
                        nonDirectJoinSparqlVar.add(sparqlVar);
                        markTriplesWithNonDirectJoin(svTerm, prevStar);
                        markTriplesWithNonDirectJoin(svTerm, i);
                    }
                }
            }
            assert star2joins.size() == stars.size();
            assert star2joins.stream().noneMatch(Objects::isNull);
        }

        private void markTriplesWithNonDirectJoin(@Nonnull Term joinVar, int starIdx) {
            for (Triple triple : stars.get(starIdx).getTriples()) {
                if (triple.contains(joinVar))
                    hasNonDirectJoin.add(triple);
            }
        }

        public void fillOuterProjection(@Nonnull CQuery query) {
            assert !stars.isEmpty();

            Set<String> sparqlVars;
            Projection projection = ModifierUtils.getFirst(Projection.class, query.getModifiers());
            if (projection != null) {
                sparqlVars = new HashSet<>(projection.getVarNames());
                for (IndexedSubset<SPARQLFilter> filters : star2pendingFilters) {
                    for (SPARQLFilter f : filters) sparqlVars.addAll(f.getVarTermNames());
                }
            } else {
                IndexedSet<String> allVars = stars.get(0).getVarNames().getParent();
                sparqlVars = hasNonDirectJoin.isEmpty() ? allVars : new HashSet<>(allVars);
            }
            assert sparqlVars.stream().noneMatch(Objects::isNull);

            hasNonDirectJoin.stream().flatMap(Triple::stream).filter(Term::isVar)
                    .forEach(t -> sparqlVars.add(t.asVar().getName()));
            exposeSparqlVars(sparqlVars);
            exposeGroundTermVars();
            assert sparqlVars.stream().noneMatch(Objects::isNull);
            removeUselessPendingTriples(sparqlVars);
        }

        private void removeUselessPendingTriples(Set<String> sparqlVars) {
            IndexedSubset<String> met = stars.get(0).getVarNames().getParent().emptySubset();
            for (int i = 0, size = stars.size(); i < size; i++) {
                StarSubQuery star = stars.get(i);
                star2pendingTriples.get(i).removeIf(t -> {
                    if (hasNonDirectJoin.contains(t))
                        return false;
                    Term o = t.getObject();
                    if (!o.isVar()) return false;
                    String oName = o.asVar().getName();
                    if (met.add(oName) && sparqlVars.contains(oName))
                        return false; //triple is relevant since it yields a solution
                    Term p = t.getPredicate();
                    if (p.isVar()) {
                        String pName = p.asVar().getName();
                        return !met.add(pName) || !sparqlVars.contains(pName);
                    } else {
                        return star.getAllColumns(p).isEmpty();
                    }
                });
            }
        }

        private void exposeSparqlVars(Set<String> sparqlVars) {
            List<String> selected = new ArrayList<>();
            for (String sparqlVar : sparqlVars) {
                Set<String> vs = sparqlVar2Var.get(sparqlVar);
                selected.clear();
                for (int i = 0, size = stars.size(); i < size; i++) {
                    Set<String> starVars = star2vars.get(i);
                    Set<String> larger = starVars.size() < vs.size() ? vs : starVars;
                    for (String v : (starVars.size() < vs.size() ? starVars : vs)) {
                        if (larger.contains(v))
                            selected.add(v);
                    }
                    if (!selected.isEmpty()) {
                        outerVars.addAll(selected);
                        if (!nonDirectJoinSparqlVar.contains(sparqlVar))
                            break;
                    }
                }
            }
        }

        private void exposeGroundTermVars() {
            for (int i = 0, size = stars.size(); i < size; i++) {
                StarSubQuery star = stars.get(i);
                Map<Column, String> col2var = star2col2var.get(i);
                Term core = star.getCore();
                if (core.isGround()) {
                    for (Column column : star.getAllColumns(core))
                        outerVars.add(col2var.get(column));
                }
                for (Triple triple : star2pendingTriples.get(i)) {
                    Term o = triple.getObject();
                    if (!o.isGround()) continue;
                    for (Column column : star.getAllColumns(o))
                        outerVars.add(col2var.get(column));
                }
            }
        }

        public void fillStarProjections() {
            assert !star2joins.isEmpty();
            for (int i = 0, size = stars.size(); i < size; i++) {
                int index = i;
                Set<String> requiredByFilters = star2pendingFilters.get(i).stream()
                        .flatMap(f -> f.getVarTermNames().stream()
                                .flatMap(v -> sparqlVar2Var.get(v).stream()))
                        .collect(toSet());
                star2vars.get(i).removeIf(v -> {
                    if (outerVars.contains(v))
                        return false;
                    if (requiredByFilters.contains(v))
                        return false;
                    if (star2joins.get(index).stream().anyMatch(join -> join.contains(index, v)))
                        return false;
                    for (int j = index+1; j < size; j++) {
                        if (star2joins.get(j).stream().anyMatch(join -> join.contains(index, v)))
                            return false;
                    }
                    return true; // not used for anything
                });
                assert starProjected.size() == i;
                TreeSet<String> ordered = new TreeSet<>(star2vars.get(i));
                for (String v : ordered)
                    var2firstStar.putIfAbsent(v, i);
                starProjected.add(ordered);
            }
            star2vars.clear(); // we invalidated the contents during the above for
        }
    }

    /* --- ---- --- internals --- --- --- */

    @VisibleForTesting
    static @Nonnull List<StarSubQuery> orderJoinable(@Nonnull List<StarSubQuery> in) {
        List<StarSubQuery> list = new ArrayList<>();
        BitSet taken = new BitSet(in.size());
        HashSet<String> vars = new HashSet<>();
        while (taken.cardinality() < in.size()) {
            boolean found = false;
            for (int i = taken.nextClearBit(0); i < in.size(); i = taken.nextClearBit(i + 1)) {
                StarSubQuery candidate = in.get(i);
                if (vars.isEmpty() || candidate.getVarNames().stream().anyMatch(vars::contains)) {
                    taken.set(i);
                    list.add(candidate);
                    vars.addAll(candidate.getVarNames());
                    found = true;
                    break;
                }
            }
            if (!found) //throw to avoid infinite loop
                throw new IllegalArgumentException("Set of stars is not join-connected");
        }
        return list;
    }

    /* --- ---- --- getters --- --- --- */

    public @Nonnull CQuery getQuery() {
        return query;
    }
    public @Nonnull IndexedSet<String> getAllSparqlVars() {
        return stars.get(0).getVarNames().getParent();
    }
    public int getStarCount() {
        return stars.size();
    }
    public @Nonnull StarSubQuery getStar(int i) {
        return stars.get(i);
    }
    public int findStar(@Nonnull Term core) {
        for (int i = 0, size = stars.size(); i < size; i++) {
            StarSubQuery star = stars.get(i);
            if (star.getCore().equals(core))
                return i;
        }
        return -1;
    }
    public @Nonnull SortedSet<String> getProjection(int i) {
        return starProjected.get(i);
    }
    public int getFirstStar(@Nonnull String var) {
        return var2firstStar.getOrDefault(var, -1);
    }
    public @Nonnull SortedSet<String> getOuterProjection() {
        return outerVars;
    }
    public @Nonnull Column getColumn(@Nonnull String var) {
        Column column = var2col.get(var);
        if (column == null)
            throw new IllegalArgumentException(var+" is not a SQL var known to this index");
        return column;
    }
    public @Nonnull Set<StarJoin> getJoins(int i) {
        return star2joins.get(i);
    }
    public @Nonnull List<Selector> getSelectors(int i) {
        return star2selectors.get(i);
    }
    public @Nonnull IndexedSet<SPARQLFilter> getAllFilters() {
        return allFilters;
    }
    public @Nonnull IndexedSubset<SPARQLFilter> getCrossStarFilters() {
        return crossStarFilters;
    }
    public @Nonnull IndexedSubset<SPARQLFilter> getPendingFilters(int i) {
        return star2pendingFilters.get(i);
    }
    public @Nonnull IndexedSubset<Triple> getPendingTriples(int i) {
        return star2pendingTriples.get(i);
    }
}
