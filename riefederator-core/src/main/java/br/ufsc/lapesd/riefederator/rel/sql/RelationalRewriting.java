package br.ufsc.lapesd.riefederator.rel.sql;

import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.rel.common.StarSubQuery;
import br.ufsc.lapesd.riefederator.rel.common.StarVarIndex;
import br.ufsc.lapesd.riefederator.rel.mappings.Column;
import br.ufsc.lapesd.riefederator.util.IndexedSubset;

import javax.annotation.Nonnull;
import java.util.*;

public class RelationalRewriting {
    private @Nonnull final String relationalQuery;
    private @Nonnull final StarVarIndex index;
    private @Nonnull final Set<String> vars;
    private final boolean distinct, limited;
    private @Nonnull final IndexedSubset<SPARQLFilter> pendingFilters;
    private @Nonnull final IndexedSubset<SPARQLFilter> doneFilters;
    private @Nonnull final List<List<String>> starVars = new ArrayList<>();
    private @Nonnull final List<List<Column>> starColumns = new ArrayList<>();

    public RelationalRewriting(@Nonnull String relationalQuery, @Nonnull Set<String> sqlVars,
                               boolean distinct, boolean limited,
                               @Nonnull IndexedSubset<SPARQLFilter> pendingFilters,
                               @Nonnull IndexedSubset<SPARQLFilter> doneFilters,
                               @Nonnull StarVarIndex varIndex) {
        this.relationalQuery = relationalQuery;
        this.vars = sqlVars;
        this.distinct = distinct;
        this.limited = limited;
        this.index = varIndex;
        this.pendingFilters = pendingFilters;
        this.doneFilters = doneFilters;

        for (int i = 0, size = varIndex.getStarCount(); i < size; i++) {
            StarSubQuery star = varIndex.getStar(i);
            String table = star.findTable();
            assert table != null;
            List<String> starVars = new ArrayList<>();
            List<Column> starColumns = new ArrayList<>();
            for (String v : varIndex.getProjection(i)) {
                if (varIndex.getOuterProjection().contains(v)) {
                    starVars.add(v);
                    starColumns.add(varIndex.getColumn(v));
                }
            }
            this.starVars.add(starVars);
            this.starColumns.add(starColumns);

            assert new HashSet<>(starVars).size() == starVars.size() : "Duplicates in starVars";
            assert starVars.size() == starColumns.size() : "#vars != #columns for star"+i;
            assert starColumns.stream().noneMatch(Objects::isNull) : "Null Columns in starColumns";
        }
        assert starVars.size() == index.getStarCount();
        assert starColumns.size() == index.getStarCount();
    }

    public @Nonnull String getRelationalQuery() {
        return relationalQuery;
    }
    public @Nonnull StarVarIndex getIndex() {
        return index;
    }
    public boolean isDistinct() {
        return distinct;
    }
    public boolean isLimited() {
        return limited;
    }
    public @Nonnull Set<String> getVars() {
        return vars;
    }
    public @Nonnull IndexedSubset<SPARQLFilter> getPendingFilters() {
        return pendingFilters;
    }
    public @Nonnull IndexedSubset<SPARQLFilter> getDoneFilters() {
        return doneFilters;
    }
    public int getStarsCount() {
        return index.getStarCount();
    }
    public @Nonnull CQuery getQuery() {
        return index.getQuery();
    }
    public @Nonnull StarSubQuery getStar(int i) {
        return index.getStar(i);
    }
    public @Nonnull List<String> getStarVars(int i) {
        return starVars.get(i);
    }
    public @Nonnull List<Column> getStarColumns(int i) {
        return starColumns.get(i);
    }

    @Override
    public @Nonnull String toString() {
        return relationalQuery;
    }
}
