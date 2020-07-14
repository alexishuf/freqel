package br.ufsc.lapesd.riefederator.rel.common;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.ModifierUtils;
import br.ufsc.lapesd.riefederator.query.modifiers.Projection;
import br.ufsc.lapesd.riefederator.rel.mappings.Column;
import br.ufsc.lapesd.riefederator.rel.mappings.RelationalMapping;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.util.IndexedSubset;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class StarVarIndex {
    private final @Nonnull List<StarSubQuery> stars;
    private final @Nonnull Map<String, Integer> var2star;
    private final @Nonnull Map<String, Column> var2column;
    private final @Nonnull List<IndexedSubset<String>> starProjected;
    private final @Nonnull IndexedSubset<String> joinVars;
    private final @Nonnull List<Map<String, Column>> idVar2column;
    private final @Nonnull SortedSet<String> outerProjection;


    public StarVarIndex(@Nonnull CQuery query, @Nonnull List<StarSubQuery> stars) {
        this(query, stars, null, false);
    }

    public StarVarIndex(@Nonnull CQuery query, @Nonnull List<StarSubQuery> stars,
                        @Nullable RelationalMapping mapping, boolean exposeJoinVars) {
        this.stars = stars;
        IndexedSet<Var> varSet = query.getTermVars();
        var2star = new HashMap<>((int)Math.ceil(varSet.size()/0.75f)+1);
        starProjected = new ArrayList<>(stars.size());
        outerProjection = new TreeSet<>();
        IndexedSet<String> allVars = IndexedSet.from(varSet.stream().map(Var::getName)
                                                           .collect(toList()));
        stars.forEach(s -> starProjected.add(allVars.emptySubset()));
        Projection mod = ModifierUtils.getFirst(Projection.class, query.getModifiers());
        Set<String> projected = mod == null ? null : mod.getVarNames();
        for (String name : allVars) {
            int idx = -1;
            for (int i = 0, size = stars.size(); idx < 0 && i < size; i++) {
                if (stars.get(i).getVarNames().contains(name))
                    idx = i;
            }
            assert idx >= 0;
            var2star.put(name, idx);
            starProjected.get(idx).add(name);
            if (projected == null || projected.contains(name))
                outerProjection.add(name);
        }
        cleanupStarProjected(stars, allVars, projected);
        joinVars = getJoinVars(allVars);
        if (exposeJoinVars)
            outerProjection.addAll(joinVars);
        idVar2column = new ArrayList<>(stars.size());
        for (int i = 0, size = stars.size(); i < size; i++)
            idVar2column.add(new HashMap<>());
        if (mapping != null) {
            addIdProjections(mapping);
            int keys = var2star.size() + idVar2column.size();
            var2column = new HashMap<>((int)Math.ceil(keys/0.75)+1);
            mapColumns();
        } else {
            var2column = Collections.emptyMap();
        }
    }

    private IndexedSubset<String> getJoinVars(IndexedSet<String> allVars) {
        IndexedSubset<String> joinVars = allVars.emptySubset();
        for (int i = 0, size = stars.size(); i < size; i++) {
            IndexedSubset<String> outer = starProjected.get(i);
            for (int j = i+1; j < size; j++)
                joinVars.addAll(outer.createIntersection(starProjected.get(j)));
        }
        return joinVars;
    }

    private void cleanupStarProjected(@Nonnull List<StarSubQuery> stars, IndexedSet<String> allVars, Set<String> projected) {
        // starProjected already contains all vars that participate in joins with later stars.
        // Also project vars which participate in joins with previous stars
        for (int i = 1, size = stars.size(); i < size; i++) {
            IndexedSubset<String> focusProjected = starProjected.get(i);
            IndexedSubset<String> focusVars = allVars.subset(stars.get(i).getVarNames());
            for (int j = 0; j < i; j++)
                focusProjected.addAll(starProjected.get(j).createIntersection(focusVars));
        }
        // starProjected.get(i) may contain vars that first occur there but are not used elsewhere
        if (projected != null) {
            for (int i = 0, size = stars.size(); i < size; i++) {
                IndexedSubset<String> unused = starProjected.get(i).copy();
                unused.removeAll(outerProjection);
                for (int j = 0; !unused.isEmpty() && j < size; j++) {
                    if (j != i)
                        unused.removeAll(starProjected.get(j));
                }
                starProjected.get(i).removeAll(unused);
            }
        }
    }

    private void mapColumns() {
        idVar2column.forEach(var2column::putAll);
        for (Map.Entry<String, Integer> e : var2star.entrySet()) {
            Column column = stars.get(e.getValue()).getColumn(new StdVar(e.getKey()));
            if (column != null)
                var2column.put(e.getKey(), column);
        }
    }

    private void addIdProjections(@Nonnull RelationalMapping mapping) {
        int nextId = 0;
        for (int i = 0, size = stars.size(); i < size; i++) {
            StarSubQuery star = stars.get(i);
            Term core = star.getCore();
            if (core.isVar() && !outerProjection.contains(core.asVar().getName()))
                continue;

            String table = star.findTable(true);
            if (table == null)
                continue;
            Set<Column> columns = star.getTriples().stream()
                    .map(Triple::getObject).filter(Term::isVar)
                    .map(o -> star.getColumn(o, true)).collect(toSet());
            List<Column> idColumns = mapping.getIdColumns(table, columns);
            for (Column idColumn : idColumns) {
                if (columns.contains(idColumn)) continue;
                String shadow = "_riefederator_id_col_"+(nextId++);
                outerProjection.add(shadow);
                idVar2column.get(i).put(shadow, idColumn);
            }
        }
    }

    public static @Nonnull List<StarSubQuery> orderJoinable(@Nonnull List<StarSubQuery> in) {
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

    public int getStarCount() {
        return stars.size();
    }
    public @Nonnull StarSubQuery getStar(int i) {
        return stars.get(i);
    }
    public @Nonnull IndexedSubset<String> getStarProjection(int i) {
        return starProjected.get(i);
    }
    public @Nonnull SortedSet<String> getOuterProjection() {
        return outerProjection;
    }
    public @Nonnull IndexedSubset<String> getJoinVars() {
        return joinVars;
    }

    public int firstStar(@Nonnull String sparqlVar) {
        int idx = var2star.getOrDefault(sparqlVar, -1);
        if (idx < 0) {
            for (int i = 0, size = idVar2column.size(); i < size; i++) {
                if (idVar2column.get(i).containsKey(sparqlVar)) return i;
            }
        }
        return idx;
    }

    public @Nonnull Map<String, Column> getIdVar2Column(int i) {
        return idVar2column.get(i);
    }
    public @Nullable Column getColumn(@Nonnull String var) {
        return var2column.get(var);
    }
}
