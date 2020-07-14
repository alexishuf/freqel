package br.ufsc.lapesd.riefederator.rel.common;

import br.ufsc.lapesd.riefederator.description.molecules.AtomTag;
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
import br.ufsc.lapesd.riefederator.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.ResultsExecutor;
import br.ufsc.lapesd.riefederator.query.results.impl.SequentialResultsExecutor;
import br.ufsc.lapesd.riefederator.rel.mappings.Column;
import br.ufsc.lapesd.riefederator.rel.mappings.tags.ColumnTag;
import br.ufsc.lapesd.riefederator.rel.mappings.tags.TableTag;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.util.IndexedSubset;
import br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation;
import com.google.common.base.Preconditions;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

import static br.ufsc.lapesd.riefederator.model.Triple.Position.SUBJ;
import static java.util.stream.Collectors.toSet;

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
        Preconditions.checkArgument(!query.isJoinConnected(), "Query should not be join-connected");

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
        List<StarSubQuery> list = new ArrayList<>();
        IndexedSet<SPARQLFilter> filters = getFilters(query);
        IndexedSubset<SPARQLFilter> pendingFilters = filters.fullSubset();
        IndexedSet<Triple> triples = query.getSet();
        IndexedSubset<Triple> visited = triples.emptySubset();
        ArrayDeque<Triple> queue = new ArrayDeque<>(triples);
        while (!queue.isEmpty()) {
            Triple triple = queue.remove();
            if (!visited.add(triple))
                continue;
            IndexedSubset<Triple> star = query.getTriplesWithTermAt(triple.getSubject(), SUBJ);
            visited.addAll(star);
            Set<String> vars = star.stream().flatMap(Triple::stream).filter(Term::isVar)
                                   .map(t -> t.asVar().getName()).collect(toSet());
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
        return findTable(query, core, StarsHelper.class.desiredAssertionStatus());
    }

    public static @Nullable String findTable(@Nonnull CQuery query, @Nonnull Term core,
                                             boolean forgiveAmbiguity) {
        String table = null;
        int tables = 0;
        for (TermAnnotation a : query.getTermAnnotations(core)) {
            if (a instanceof AtomAnnotation) {
                for (AtomTag tag : ((AtomAnnotation) a).getAtom().getTags()) {
                    if (tag instanceof TableTag) {
                        ++tables;
                        table = ((TableTag) tag).getTable();
                    }
                }
            }
        }
        if (tables > 1) {
            logger.warn("Star core has {} tables, arbitrarily using {}.", tables, table);
            if (!forgiveAmbiguity)
                throw new AmbiguousTagException(TableTag.class, core);
        }
        return table;
    }

    public static @Nullable Column getColumn(@Nonnull CQuery query, @Nullable String table,
                                         @Nonnull Term term) {
        return getColumn(query, table, term, StarsHelper.class.desiredAssertionStatus());
    }

    public static @Nullable Column getColumn(@Nonnull CQuery query, @Nullable String table,
                                             @Nonnull Term term, boolean forgiveAmbiguity) {
        Column column = null;
        int ambiguous = 0;
        for (TermAnnotation a : query.getTermAnnotations(term)) {
            if (!(a instanceof AtomAnnotation)) continue;
            for (AtomTag tag : ((AtomAnnotation) a).getAtom().getTags()) {
                if (tag instanceof ColumnTag) {
                    Column candidate = ((ColumnTag) tag).getColumn();
                    if (column == null)
                        column = candidate; //first encounter with a column
                    else if (table != null && !column.getTable().equals(table))
                        column = candidate; //old column was a fallback
                    else if (table == null || candidate.getTable().equals(table))
                        ++ambiguous; //ambiouity
                }
            }
        }
        if (ambiguous > 0 && !forgiveAmbiguity)
            throw new AmbiguousTagException(ColumnTag.class, term);
        return column;
    }
}
