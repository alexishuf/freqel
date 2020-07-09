package br.ufsc.lapesd.riefederator.rel.sql;

import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.rel.common.StarSubQuery;
import br.ufsc.lapesd.riefederator.rel.common.StarVarIndex;
import br.ufsc.lapesd.riefederator.rel.mappings.Column;
import br.ufsc.lapesd.riefederator.util.IndexedSubset;

import javax.annotation.Nonnull;
import java.util.*;

public class SqlRewriting {
    private @Nonnull final String sql;
    private @Nonnull final Set<String> vars;
    private final boolean distinct, limited;
    private @Nonnull final IndexedSubset<SPARQLFilter> pendingFilters;
    private @Nonnull final IndexedSubset<SPARQLFilter> doneFilters;
    private @Nonnull final CQuery query;
    private @Nonnull final List<StarSubQuery> stars = new ArrayList<>();
    private @Nonnull final List<List<String>> starVars = new ArrayList<>();
    private @Nonnull final List<List<Column>> starColumns = new ArrayList<>();

    public SqlRewriting(@Nonnull String sql, @Nonnull Set<String> sqlVars,
                        boolean distinct, boolean limited, @Nonnull CQuery query,
                        @Nonnull IndexedSubset<SPARQLFilter> pendingFilters,
                        @Nonnull IndexedSubset<SPARQLFilter> doneFilters,
                        @Nonnull StarVarIndex varIndex) {
        this.sql = sql;
        this.vars = sqlVars;
        this.distinct = distinct;
        this.limited = limited;
        this.query = query;
        this.pendingFilters = pendingFilters;
        this.doneFilters = doneFilters;

        for (int i = 0, size = varIndex.getStarCount(); i < size; i++) {
            StarSubQuery star = varIndex.getStar(i);
            stars.add(star);
            List<String> starVars = new ArrayList<>();
            List<Column> starColumns = new ArrayList<>();
            for (String v : varIndex.getStarProjection(i)) {
                if (star.isCore(v)) continue;
                starVars.add(v);
                starColumns.add(varIndex.getColumn(v));
            }
            for (Map.Entry<String, Column> e : varIndex.getIdVar2Column(i).entrySet()) {
                starVars.add(e.getKey());
                starColumns.add(e.getValue());
            }
            this.starVars.add(starVars);
            this.starColumns.add(starColumns);

            assert new HashSet<>(starVars).size() == starVars.size() : "Duplicates in starVars";
            assert starVars.size() == starColumns.size() : "#vars != #columns for star"+i;
            assert starColumns.stream().noneMatch(Objects::isNull) : "Null Columns in starColumns";
            assert varIndex.getOuterProjection().containsAll(starVars)
                    : "starVars for star "+i+" not in outerProjection";
        }
        assert stars.size() == starVars.size();
        assert stars.size() == starColumns.size();
    }

    public @Nonnull String getSql() {
        return sql;
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
        return stars.size();
    }
    public @Nonnull CQuery getQuery() {
        return query;
    }
    public @Nonnull StarSubQuery getStar(int i) {
        return stars.get(i);
    }
    public @Nonnull Term getStarCore(int i) {
        return getStar(i).getCore();
    }
    public @Nonnull List<String> getStarVars(int i) {
        return starVars.get(i);
    }
    public @Nonnull List<Column> getStarColumns(int i) {
        return starColumns.get(i);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SqlRewriting)) return false;
        SqlRewriting that = (SqlRewriting) o;
        return isDistinct() == that.isDistinct() &&
                isLimited() == that.isLimited() &&
                getSql().equals(that.getSql()) &&
                getVars().equals(that.getVars()) &&
                getPendingFilters().equals(that.getPendingFilters()) &&
                getDoneFilters().equals(that.getDoneFilters()) &&
                getQuery().equals(that.getQuery()) &&
                stars.equals(that.stars) &&
                starVars.equals(that.starVars) &&
                starColumns.equals(that.starColumns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSql(), getVars(), isDistinct(), isLimited(), getPendingFilters(), getDoneFilters(), getQuery(), stars, starVars, starColumns);
    }

    @Override
    public @Nonnull String toString() {
        return sql;
    }
}
