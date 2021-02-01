package br.ufsc.lapesd.freqel.rel.common;

import br.ufsc.lapesd.freqel.description.molecules.Atom;
import br.ufsc.lapesd.freqel.description.molecules.tags.AtomTag;
import br.ufsc.lapesd.freqel.description.molecules.tags.MoleculeLinkTag;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.annotations.TripleAnnotation;
import br.ufsc.lapesd.freqel.query.modifiers.filter.SPARQLFilter;
import br.ufsc.lapesd.freqel.rel.mappings.Column;
import br.ufsc.lapesd.freqel.rel.mappings.tags.ColumnsTag;
import br.ufsc.lapesd.freqel.rel.mappings.tags.PostRelationalTag;
import br.ufsc.lapesd.freqel.rel.mappings.tags.TableTag;
import br.ufsc.lapesd.freqel.util.indexed.FullIndexSet;
import br.ufsc.lapesd.freqel.util.indexed.IndexSet;
import br.ufsc.lapesd.freqel.util.indexed.subset.IndexSubset;
import br.ufsc.lapesd.freqel.description.molecules.MoleculeLinkAnnotation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

import static br.ufsc.lapesd.freqel.model.Triple.Position.SUBJ;
import static java.util.stream.Collectors.toList;

public class StarsHelper {
    private static final Logger logger = LoggerFactory.getLogger(StarsHelper.class);

    public static @Nonnull IndexSet<SPARQLFilter> getFilters(@Nonnull CQuery query) {
        return FullIndexSet.fromDistinctCopy(query.getModifiers().filters());
    }

    public static @Nonnull List<StarSubQuery> findStars(@Nonnull CQuery query) {
        return findStars(query, getFilters(query));
    }

    public static @Nonnull List<StarSubQuery> findStars(@Nonnull CQuery query,
                                                        @Nonnull IndexSet<SPARQLFilter> filters) {
        List<StarSubQuery> list = new ArrayList<>();

        IndexSubset<SPARQLFilter> pendingFilters = filters.fullSubset();
        IndexSet<String> allVarNames = query.attr().tripleVarNames();
        IndexSet<Triple> triples = query.attr().getSet();
        IndexSubset<Triple> visited = triples.emptySubset();
        ArrayDeque<Triple> queue = new ArrayDeque<>(triples);
        while (!queue.isEmpty()) {
            Triple triple = queue.remove();
            if (!visited.add(triple))
                continue;
            IndexSubset<Triple> star = query.attr().triplesWithTermAt(triple.getSubject(), SUBJ);
            visited.addAll(star);

            IndexSubset<String> vars = allVarNames.subset(star.stream().flatMap(Triple::stream)
                                          .filter(Term::isVar)
                                          .map(t -> t.asVar().getName()).collect(toList()));
            IndexSubset<SPARQLFilter> starFilters = filters.emptySubset();
            for (Iterator<SPARQLFilter> it = pendingFilters.iterator(); it.hasNext(); ) {
                SPARQLFilter filter = it.next();
                if (vars.containsAll(filter.getVarNames())) {
                    starFilters.add(filter);
                    it.remove();
                }
            }
            list.add(new StarSubQuery(star, vars, starFilters, query));
        }
        return list;
    }

    public static @Nullable String findTable(@Nonnull CQuery query, @Nonnull Term core) {
        Set<String> tables = new HashSet<>();
        for (Atom atom : query.attr().termAtoms(core)) {
            for (AtomTag tag : atom.getTags()) {
                if (tag instanceof TableTag)
                    tables.add(((TableTag) tag).getTable());
            }
        }
        if (tables.size() > 1) {
            logger.warn("Star core has multiple tables: {}.", tables);
            throw new AmbiguousTagException(TableTag.class, core);
        }
        return tables.isEmpty() ? null : tables.iterator().next();
    }

    public static boolean isPostRelational(@Nonnull CQuery query, @Nonnull Term term) {
        return query.attr().termAtoms(term)
                    .stream().flatMap(a -> a.getTags().stream())
                    .anyMatch(PostRelationalTag.class::isInstance);
    }

    public static @Nonnull Set<Column> getColumns(@Nonnull CQuery query, @Nullable String table,
                                                  @Nonnull Term term) {
        Set<Column> set = new HashSet<>();
        for (Atom atom : query.attr().termAtoms(term)) {
            for (AtomTag tag : atom.getTags()) {
                if (tag instanceof ColumnsTag) {
                    ColumnsTag columnsTag = (ColumnsTag) tag;
                    if (table == null || columnsTag.getTable().equals(table))
                        set.addAll(columnsTag.getColumns());
                }
            }
        }
        return set;
    }

    public static @Nullable ColumnsTag
    getColumnsTag(@Nonnull CQuery query, @Nonnull String table,
                      @Nonnull Term term, @Nonnull Collection<Triple> triples) {
        List<ColumnsTag> candidates = new ArrayList<>();
        for (Atom atom : query.attr().termAtoms(term)) {
            for (AtomTag tag : atom.getTags()) {
                if (tag instanceof ColumnsTag) {
                    ColumnsTag cTag = (ColumnsTag) tag;
                    if (cTag.getTable().equals(table))
                        candidates.add(cTag);
                }
            }
        }
        if (candidates.size() == 1)
            return candidates.get(0);

        List<List<ColumnsTag>> candidateLists = new ArrayList<>();
        for (Triple triple : triples) {
            if (!triple.contains(term)) continue;
            List<ColumnsTag> list = new ArrayList<>();
            for (TripleAnnotation a : query.getTripleAnnotations(triple)) {
                if (!(a instanceof MoleculeLinkAnnotation)) continue;
                MoleculeLinkAnnotation mla = (MoleculeLinkAnnotation) a;
                boolean goodDirection = ( term.equals(triple.getObject()) && !mla.isReversed() )
                        || ( term.equals(triple.getSubject()) && mla.isReversed() );
                if (!goodDirection) continue;
                for (MoleculeLinkTag tag : mla.getLink().getTags()) {
                    if (!(tag instanceof ColumnsTag)) continue;
                    ColumnsTag cTag = (ColumnsTag) tag;
                    if (cTag.getTable().equals(table))
                        list.add(cTag);
                }
            }
            if (!list.isEmpty())
                candidateLists.add(list);
        }

        if (candidateLists.isEmpty())
            return null;
        Set<ColumnsTag> common = new HashSet<>(candidateLists.get(0));
        for (int i = 1, size = candidateLists.size(); i < size; i++)
            common.retainAll(candidateLists.get(i));
        if (common.size() != 1) {
            throw new IllegalArgumentException("Found "+common.size()+" shared ColumnsTags " +
                    "(expected 1) among all triples that contain term "+term+" and have " +
                    "ColumnsTags on table"+table+". triples="+triples);
        }
        assert common.iterator().next().getTable().equals(table);
        return common.iterator().next();
    }
}
