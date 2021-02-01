package br.ufsc.lapesd.freqel.rel.common;

import br.ufsc.lapesd.freqel.federation.execution.tree.impl.joins.hash.InMemoryHashJoinResults;
import br.ufsc.lapesd.freqel.jena.query.JenaBindingSolution;
import br.ufsc.lapesd.freqel.model.SPARQLString;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.model.term.Var;
import br.ufsc.lapesd.freqel.model.term.std.StdVar;
import br.ufsc.lapesd.freqel.query.MutableCQuery;
import br.ufsc.lapesd.freqel.query.modifiers.*;
import br.ufsc.lapesd.freqel.query.modifiers.filter.SPARQLFilterFactory;
import br.ufsc.lapesd.freqel.query.modifiers.filter.SPARQLFilter;
import br.ufsc.lapesd.freqel.query.modifiers.filter.SPARQLFilterExecutor;
import br.ufsc.lapesd.freqel.query.results.AbstractResults;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.ResultsCloseException;
import br.ufsc.lapesd.freqel.query.results.Solution;
import br.ufsc.lapesd.freqel.query.results.impl.ArraySolution;
import br.ufsc.lapesd.freqel.query.results.impl.CollectionResults;
import br.ufsc.lapesd.freqel.query.results.impl.LazyCartesianResults;
import br.ufsc.lapesd.freqel.rel.mappings.RelationalMapping;
import br.ufsc.lapesd.freqel.rel.sql.RelationalRewriting;
import br.ufsc.lapesd.freqel.util.indexed.IndexSet;
import br.ufsc.lapesd.freqel.util.indexed.subset.IndexSubset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

import static java.util.Arrays.asList;

public abstract class RelationalResults extends AbstractResults {
    private static final Logger logger = LoggerFactory.getLogger(RelationalResults.class);

    private static final Var p = new StdVar("p"), o = new StdVar("o");

    private boolean closed = false;
    private final boolean ask;
    private Exception exception = null;
    private final @Nonnull RelationalMapping mapping;
    private final @Nonnull Queue<Solution> queue = new ArrayDeque<>();
    private final @Nonnull RelationalRewriting rewriting;
    private final @Nonnull Model tmpModel = ModelFactory.createDefaultModel();
    private final @Nonnull List<Query> jenaStars;
    private final @Nonnull List<Set<String>> jenaVars;
    private final @Nonnull List<JenaBindingSolution.Factory> jenaSolutionFac;
    private final @Nonnull List<Set<String>> jVars;
    private final @Nonnull List<Set<String>> jrVars;
    private final @Nullable ArraySolution.ValueFactory projector;
    private final @Nonnull SPARQLFilterExecutor filterExecutor = SPARQLFilterFactory.createExecutor();

    protected RelationalResults(@Nonnull RelationalRewriting rw,
                                @Nonnull RelationalMapping mapping) {
        super(rw.getQuery().attr().publicTripleVarNames());
        this.rewriting = rw;
        this.mapping = mapping;
        this.ask = rw.getQuery().attr().isAsk();
        this.jenaStars = new ArrayList<>(rw.getStarsCount());
        this.jenaVars = new ArrayList<>(rw.getStarsCount());
        this.jenaSolutionFac = new ArrayList<>(rw.getStarsCount());
        this.jVars = new ArrayList<>(rw.getStarsCount());
        this.jrVars = new ArrayList<>(rw.getStarsCount());
        StarVarIndex index = rw.getIndex();
        IndexSet<String> allVars = index.getAllSparqlVars();
        Set<String> projection = getVarNames();
        for (int i = 0, size = rw.getStarsCount(); i < size; i++) {
            MutableCQuery b = MutableCQuery.from(index.getPendingTriples(i));
            b.mutateModifiers().addAll(index.getPendingFilters(i));
            if (b.isEmpty()) {
                assert index.getPendingFilters(i).isEmpty();
                Term core = rw.getStar(i).getCore();
                if (core.isVar() && projection.contains(core.asVar().getName())) {
                    b.add(new Triple(core, p, o));
                    b.mutateModifiers().add(Projection.of(core.asVar().getName()));
                    b.mutateModifiers().add(Distinct.INSTANCE);
                } else { // no work on our side, skip it
                    jenaStars.add(null);
                    jenaVars.add(Collections.emptySet());
                    jenaSolutionFac.add(null);
                    jVars.add(Collections.emptySet());
                    jrVars.add(Collections.emptySet());
                    continue;
                }
            }
            SPARQLString ss = SPARQLString.create(b);
            jenaStars.add(QueryFactory.create(ss.getSparql()));
            jenaVars.add(ss.getVarNames());
            jenaSolutionFac.add(JenaBindingSolution.forVars(jenaVars.get(i)));
            if (i == 0) {
                jVars.add(Collections.emptySet());
                jrVars.add(jenaVars.get(i));
            } else {
                // subseting on allVars avoid joining the over the dummy p and o vars
                IndexSubset<String> set = allVars.subset(jenaVars.get(i));
                jVars.add(set.createIntersection(jenaVars.get(i-1)));
                set.addAll(jenaVars.get(i-1));
                jrVars.add(set);
            }
        }
        projector = allVars.equals(projection) ? null : ArraySolution.forVars(projection);
    }

    @Override
    public int getReadyCount() {
        return queue.size();
    }

    @Override
    public boolean isDistinct() {
        return rewriting.isDistinct();
    }

    @Override
    public boolean hasNext() {
        while (!closed && queue.isEmpty()) {
            try {
                if (!relationalAdvance())
                    break;
                convert();
            } catch (Exception e) {
                exception = e;
                silentClose();
            }
        }
        if (ask && !closed) { //will only close once, after first hasNext()
            boolean ok = !queue.isEmpty();
            queue.clear();
            silentClose();
            assert getVarNames().isEmpty();
            if (ok)
                queue.add(ArraySolution.EMPTY); //else: leave queue empty
        }
        return !queue.isEmpty();
    }

    @Override
    public @Nonnull Solution next() {
        if (!hasNext())
            throw new NoSuchElementException();
        return queue.remove();
    }


    private void convert() throws Exception {
        // results will be a tree of hash joins among the results of each star
        Results results = null;
        try {
            for (int i = 0; i < rewriting.getStarsCount(); i++) {
                if (jenaStars.get(i) == null)
                    continue; // the query is an ASK that was already executed in SPARQL
                Results r = executeStar(i);
                if (results == null) {
                    results = r; // first result, use as root
                } else if (jVars.get(i).isEmpty()) {
                    results = new LazyCartesianResults(asList(results, r), jrVars.get(i));
                } else {
                    results = new InMemoryHashJoinResults(results, r, jVars.get(i),
                            jrVars.get(i), false);
                }
            }
            assert results != null;
            filter(results);
        } finally {
            if (results != null) {
                try {
                    results.close();
                } catch (ResultsCloseException e) {
                    logger.error("results.close() threw during convert(): ", e);
                }
            }
        }
    }

    private @Nonnull Results executeStar(int i) throws Exception {
        List<String> starVars = rewriting.getStarVars(i);
        List<Object> starValues = new ArrayList<>(starVars.size());
        for (String v : starVars)
            starValues.add(relationalGetValue(v));

        tmpModel.removeAll();
        mapping.toRDF(tmpModel, rewriting.getStarColumns(i), starValues);
        JenaBindingSolution.Factory factory = jenaSolutionFac.get(i);
        List<Solution> solutions = new ArrayList<>();
        try (QueryExecution exec = QueryExecutionFactory.create(jenaStars.get(i), tmpModel)) {
            org.apache.jena.query.ResultSet rs = exec.execSelect();
            while (rs.hasNext())
                solutions.add(factory.transform(rs.nextBinding()));
        } catch (Exception e) {
            logger.error("Problem executing ARQ query for star {}={}. Will continue with " +
                    "{}} results.", i, jenaStars.get(i), solutions.size(), e);
        }
        return new CollectionResults(solutions, jenaVars.get(i));
    }

    /**
     * Apply filters not applied in SQL to the results and add valid Solutions to the queue
     */
    private void filter(@Nonnull Results results) {
        rs_loop:
        while (results.hasNext()) {
            Solution solution = results.next();
            for (SPARQLFilter filter : rewriting.getPendingFilters()) {
                if (!filterExecutor.evaluate(filter, solution))
                    continue rs_loop;
            }
            if (projector != null) queue.add(projector.fromSolution(solution));
            else                   queue.add(solution);
        }
    }

    protected abstract boolean relationalAdvance() throws Exception;
    protected abstract void relationalClose() throws Exception;
    protected abstract @Nullable Object relationalGetValue(String relationalVar)throws Exception;

    private void silentClose() {
        try {
            relationalClose();
        } catch (Exception e) {
            if (exception == null) exception = e;
            else                   exception.addSuppressed(e);
        }
        closed = true;
    }

    @Override
    public void close() throws ResultsCloseException {
        if (exception != null) {
            Exception cause = this.exception;
            this.exception = null;
            throw new ResultsCloseException(this, "SQLException during hasNext()", cause);
        }
        if (!closed) {
            closed = true;
            try {
                relationalClose();
            } catch (Exception e) {
                throw new ResultsCloseException(this, e);
            }
        }
    }
}
