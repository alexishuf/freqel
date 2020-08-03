package br.ufsc.lapesd.riefederator.rel.common;

import br.ufsc.lapesd.riefederator.description.molecules.tags.AtomTag;
import br.ufsc.lapesd.riefederator.description.molecules.tags.MoleculeLinkTag;
import br.ufsc.lapesd.riefederator.federation.SimpleFederationModule;
import br.ufsc.lapesd.riefederator.federation.decomp.FilterAssigner;
import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.planner.OuterPlanner;
import br.ufsc.lapesd.riefederator.federation.tree.ComponentNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.federation.tree.QueryNode;
import br.ufsc.lapesd.riefederator.federation.tree.TreeUtils;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.annotations.TermAnnotation;
import br.ufsc.lapesd.riefederator.query.annotations.TripleAnnotation;
import br.ufsc.lapesd.riefederator.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.ResultsExecutor;
import br.ufsc.lapesd.riefederator.query.results.impl.SequentialResultsExecutor;
import br.ufsc.lapesd.riefederator.rel.mappings.Column;
import br.ufsc.lapesd.riefederator.rel.mappings.tags.ColumnsTag;
import br.ufsc.lapesd.riefederator.rel.mappings.tags.PostRelationalTag;
import br.ufsc.lapesd.riefederator.rel.mappings.tags.TableTag;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.util.IndexedSubset;
import br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation;
import br.ufsc.lapesd.riefederator.webapis.description.MoleculeLinkAnnotation;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

import static br.ufsc.lapesd.riefederator.model.Triple.Position.SUBJ;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;

public class StarsHelper {
    private static final Logger logger = LoggerFactory.getLogger(StarsHelper.class);
    private static Injector sharedInjector;

    private static  @Nonnull Injector getInjector() {
        if (sharedInjector == null) {
            sharedInjector = Guice.createInjector(new SimpleFederationModule() {
                @Override
                protected void configureResultsExecutor() {
                    bind(ResultsExecutor.class).toInstance(new SequentialResultsExecutor());
                }
            });
        }
        return sharedInjector;
    }

    /**
     * Decompose the join-disconnected query into join-connected components and executes it.
     * FILTERs are executed at the components where possible, else thei are executed
     * after the cartesian products.
     *
     * @param query the join-disconnected query
     * @param target Endpoint that will receive all component queries. There is no source selection
     * @throws IllegalArgumentException if query is join-connected
     */
    public static @Nonnull
    Results executeJoinDisconnected(@Nonnull CQuery query, @Nonnull CQEndpoint target) {
        checkArgument(!query.attr().isJoinConnected(), "Query should not be join-connected");

        Injector injector = getInjector();
        PlanExecutor planExecutor = injector.getInstance(PlanExecutor.class);
        PlanNode plan = injector.getInstance(OuterPlanner.class).plan(query);
        Map<PlanNode, PlanNode> replacements = new HashMap<>();
        TreeUtils.streamPreOrder(plan).filter(ComponentNode.class::isInstance).forEach(n -> {
            CQuery componentQuery = ((ComponentNode) n).getQuery();
            replacements.put(n, new QueryNode(target, componentQuery));
        });
        plan = TreeUtils.replaceNodes(plan, replacements);
        FilterAssigner assigner = new FilterAssigner(query);
        assigner.placeBottommost(plan);
        return planExecutor.executePlan(plan);
    }

    public static @Nonnull IndexedSet<SPARQLFilter> getFilters(@Nonnull CQuery query) {
        return IndexedSet.fromDistinct(query.getModifiers().stream()
                .filter(f->f instanceof SPARQLFilter)
                .map(f -> (SPARQLFilter)f).iterator());
    }

    public static @Nonnull List<StarSubQuery> findStars(@Nonnull CQuery query) {
        return findStars(query, getFilters(query));
    }

    public static @Nonnull List<StarSubQuery> findStars(@Nonnull CQuery query,
                                                        @Nonnull IndexedSet<SPARQLFilter> filters) {
        List<StarSubQuery> list = new ArrayList<>();

        IndexedSubset<SPARQLFilter> pendingFilters = filters.fullSubset();
        IndexedSet<String> allVarNames = query.attr().tripleVarNames();
        IndexedSet<Triple> triples = query.attr().getSet();
        IndexedSubset<Triple> visited = triples.emptySubset();
        ArrayDeque<Triple> queue = new ArrayDeque<>(triples);
        while (!queue.isEmpty()) {
            Triple triple = queue.remove();
            if (!visited.add(triple))
                continue;
            IndexedSubset<Triple> star = query.attr().triplesWithTermAt(triple.getSubject(), SUBJ);
            visited.addAll(star);

            IndexedSubset<String> vars = allVarNames.subset(star.stream().flatMap(Triple::stream)
                                          .filter(Term::isVar)
                                          .map(t -> t.asVar().getName()).collect(toList()));
            IndexedSubset<SPARQLFilter> starFilters = filters.emptySubset();
            for (Iterator<SPARQLFilter> it = pendingFilters.iterator(); it.hasNext(); ) {
                SPARQLFilter filter = it.next();
                if (vars.containsAll(filter.getVarTermNames())) {
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
        for (TermAnnotation a : query.getTermAnnotations(core)) {
            if (a instanceof AtomAnnotation) {
                for (AtomTag tag : ((AtomAnnotation) a).getAtom().getTags()) {
                    if (tag instanceof TableTag)
                        tables.add(((TableTag) tag).getTable());
                }
            }
        }
        if (tables.size() > 1) {
            logger.warn("Star core has multiple tables: {}.", tables);
            throw new AmbiguousTagException(TableTag.class, core);
        }
        return tables.isEmpty() ? null : tables.iterator().next();
    }

    public static boolean isPostRelational(@Nonnull CQuery query, @Nonnull Term term) {
        return query.getTermAnnotations(term).stream()
                .filter(AtomAnnotation.class::isInstance)
                .flatMap(a -> ((AtomAnnotation)a).getAtom().getTags().stream())
                .anyMatch(PostRelationalTag.class::isInstance);
    }

    public static @Nonnull Set<Column> getColumns(@Nonnull CQuery query, @Nullable String table,
                                                  @Nonnull Term term) {
        Set<Column> set = new HashSet<>();
        for (TermAnnotation a : query.getTermAnnotations(term)) {
            if (!(a instanceof AtomAnnotation)) continue;
            for (AtomTag tag : ((AtomAnnotation) a).getAtom().getTags()) {
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
        for (TermAnnotation a : query.getTermAnnotations(term)) {
            if (!(a instanceof AtomAnnotation)) continue;
            AtomAnnotation aa = (AtomAnnotation) a;
            for (AtomTag tag : aa.getAtom().getTags()) {
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
