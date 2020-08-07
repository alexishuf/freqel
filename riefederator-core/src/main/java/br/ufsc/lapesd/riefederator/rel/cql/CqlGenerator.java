package br.ufsc.lapesd.riefederator.rel.cql;

import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.rel.common.RelationalTermWriter;
import br.ufsc.lapesd.riefederator.rel.common.Selector;
import br.ufsc.lapesd.riefederator.rel.common.StarVarIndex;
import br.ufsc.lapesd.riefederator.rel.sql.RelationalRewriting;
import br.ufsc.lapesd.riefederator.rel.sql.impl.DefaultSqlTermWriter;
import br.ufsc.lapesd.riefederator.util.IndexedSubset;

import javax.annotation.Nonnull;

import static com.google.common.base.Preconditions.checkArgument;

public class CqlGenerator {
    public static final @Nonnull CqlGenerator INSTANCE = new CqlGenerator();

    private final @Nonnull RelationalTermWriter termWriter;

    public CqlGenerator(@Nonnull RelationalTermWriter termWriter) {
        this.termWriter = termWriter;
    }

    public CqlGenerator() {
        this(DefaultSqlTermWriter.INSTANCE);
    }

    public @Nonnull RelationalRewriting transform(@Nonnull CQuery query) throws MultiStarException {
        checkArgument(query.attr().isJoinConnected());
        boolean distinct = query.getModifiers().distinct() != null;
        int limit = query.attr().limit();
        if (query.attr().isAsk()) limit = 1;
        CqlSelectorFactory selectorFactory = new CqlSelectorFactory(termWriter);
        StarVarIndex index = new StarVarIndex(query, selectorFactory);
        if (index.getStarCount() > 1)
            throw new MultiStarException(index);
        checkArgument(index.getStarCount() == 1, "Cassandra does not allow " +
                      "joins, yet found "+index.getStarCount()+" stars in "+query);
        String table = index.getStar(0).findTable();
        checkArgument(table != null, "No table found for the star "+query);

        String cql = write(distinct, limit, index, table);
        IndexedSubset<SPARQLFilter> doneFilters = index.getStar(0).getFilters().copy();
        doneFilters.removeAll(index.getPendingFilters(0));
        return new RelationalRewriting(cql, index.getOuterProjection(),
                                       distinct, limit > 0,
                                       index.getPendingFilters(0), doneFilters, index);
    }

    private @Nonnull String write(boolean distinct, int limit, StarVarIndex index, String table) {
        StringBuilder b = new StringBuilder();
        b.append("SELECT ").append(distinct ? "DISTINCT " : "");
        for (String v : index.getOuterProjection())
            b.append(index.getColumn(v).getColumn()).append(" AS ").append(v).append(", ");
        if (index.getOuterProjection().isEmpty())
            b.append("(INT)1 AS ask");
        else
            b.setLength(b.length()-2); //remove ", "
        b.append(" FROM ").append(table).append("\n WHERE ");
        for (Selector selector : index.getSelectors(0)) {
            if (selector.hasCondition())
                b.append('(').append(selector.getCondition()).append(")   AND  "); //space matters
        }
        b.setLength(b.length()-8); // "\n WHERE " and "   AND  " strings both have 6 chars
        if (limit > 0)
            b.append("\n LIMIT ").append(limit);
        return b.append("\n  ALLOW FILTERING ;").toString();
    }
}
