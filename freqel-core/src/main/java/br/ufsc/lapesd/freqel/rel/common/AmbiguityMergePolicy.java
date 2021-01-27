package br.ufsc.lapesd.freqel.rel.common;

import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.annotations.MergePolicyAnnotation;
import br.ufsc.lapesd.freqel.rel.mappings.Column;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class AmbiguityMergePolicy implements MergePolicyAnnotation {
    private static final Logger logger = LoggerFactory.getLogger(AmbiguityMergePolicy.class);

    @Override
    public boolean canMerge(@Nonnull CQuery a, @Nonnull CQuery b) {
        HashMap<Term, String> tableMap = Maps.newHashMapWithExpectedSize(a.size() + b.size());
        if (!checkTables(a, b, tableMap))
            return false;
        return checkColumns(a, b, tableMap);
    }

    private boolean checkTables(@Nonnull CQuery a, @Nonnull CQuery b,
                                @Nonnull Map<Term, String> tableMap) {
        for (Triple triple : a) {
            Term subject = triple.getSubject();
            if (!tableMap.containsKey(subject)) {
                String table = StarsHelper.findTable(a, subject);
                if (table != null) tableMap.put(subject, table);
            }
        }
        for (Triple triple : b) {
            Term subject = triple.getSubject();
            Object expected = tableMap.getOrDefault(subject, null);
            if (expected != null) {
                String table = StarsHelper.findTable(b, subject);
                if (table != null && !table.equals(expected)) {
                    logger.debug("Deny merge since {} has table {} on query a and table {} on " +
                                 "query b.\nQuery a: {}\nQuery b: {}",
                                 subject, expected, table, a, b);
                    return false;
                }
            }
        }
        return true; // no ambiguity
    }

    private boolean checkColumns(@Nonnull CQuery a, @Nonnull CQuery b,
                                 @Nonnull Map<Term, String> tableMap) {
        for (Triple triple : a) {
            String table = tableMap.getOrDefault(triple.getSubject(), null);
            if (table == null) continue;
            Term obj = triple.getObject();
            Set<Column> expected = StarsHelper.getColumns(a, table, obj);
            if (expected.isEmpty()) continue;
            Set<Column> other = StarsHelper.getColumns(b, table, obj);
            if (!expected.containsAll(other)) {
                logger.debug("Ambiguity: object {} has columns {} in query a and columns {} " +
                             "in query b.", obj, expected, other);
                return false;
            }
        }
        return true;
    }

}
