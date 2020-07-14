package br.ufsc.lapesd.riefederator.rel.sql;

import br.ufsc.lapesd.riefederator.description.molecules.Molecule;
import br.ufsc.lapesd.riefederator.description.molecules.MoleculeMatcher;
import br.ufsc.lapesd.riefederator.federation.Federation;
import br.ufsc.lapesd.riefederator.federation.SimpleFederationModule;
import br.ufsc.lapesd.riefederator.federation.Source;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.hash.InMemoryHashJoinResults;
import br.ufsc.lapesd.riefederator.federation.tree.TreeUtils;
import br.ufsc.lapesd.riefederator.jena.query.JenaBindingSolution;
import br.ufsc.lapesd.riefederator.model.FastSPARQLString;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.annotations.MergePolicyAnnotation;
import br.ufsc.lapesd.riefederator.query.endpoint.AbstractTPEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.Capability;
import br.ufsc.lapesd.riefederator.query.endpoint.QueryExecutionException;
import br.ufsc.lapesd.riefederator.query.modifiers.ModifierUtils;
import br.ufsc.lapesd.riefederator.query.modifiers.Projection;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.results.*;
import br.ufsc.lapesd.riefederator.query.results.impl.*;
import br.ufsc.lapesd.riefederator.reason.tbox.TransitiveClosureTBoxReasoner;
import br.ufsc.lapesd.riefederator.rel.common.AmbiguityMergePolicy;
import br.ufsc.lapesd.riefederator.rel.common.StarSubQuery;
import br.ufsc.lapesd.riefederator.rel.common.StarsHelper;
import br.ufsc.lapesd.riefederator.rel.mappings.Column;
import br.ufsc.lapesd.riefederator.rel.mappings.RelationalMapping;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.util.IndexedSubset;
import com.google.inject.Guice;
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
import java.sql.*;
import java.util.*;

import static java.util.stream.Collectors.toSet;

public class JDBCCQEndpoint extends AbstractTPEndpoint implements CQEndpoint {
    private static final Logger logger = LoggerFactory.getLogger(JDBCCQEndpoint.class);
    private static final Var s = new StdVar("s"), p = new StdVar("p"), o = new StdVar("o");


    private @Nonnull final RelationalMapping mapping;
    private @Nonnull final SqlGenerator sqlGenerator;
    private @Nonnull final Molecule molecule;
    private @Nonnull final MoleculeMatcher moleculeMatcher;
    private @Nonnull final String name;
    private @Nonnull final ConnectionSupplier connectionSupplier;

    @FunctionalInterface
    public interface ConnectionSupplier {
        @Nonnull Connection connect() throws SQLException;
    }

    /* --- --- --- Constructor & Builder --- --- --- */

    public JDBCCQEndpoint(@Nonnull RelationalMapping mapping, @Nonnull String name,
                          @Nonnull ConnectionSupplier connectionSupplier) {
        this.mapping = mapping;
        this.sqlGenerator = new SqlGenerator(mapping).setExposeJoinVars(true);
        this.name = name;
        this.connectionSupplier = connectionSupplier;
        this.molecule = mapping.createMolecule();
        TransitiveClosureTBoxReasoner empty = new TransitiveClosureTBoxReasoner();
        MergePolicyAnnotation policy = new AmbiguityMergePolicy();
        this.moleculeMatcher = new MoleculeMatcher(this.molecule, empty, policy);
    }

    public static class Builder {
        private final @Nonnull RelationalMapping mapping;

        public Builder(@Nonnull RelationalMapping mapping) {
            this.mapping = mapping;
        }

        public @Nonnull
        JDBCCQEndpoint connectingTo(@Nonnull String jdbcUrl) {
            return new JDBCCQEndpoint(mapping, jdbcUrl, () -> DriverManager.getConnection(jdbcUrl));
        }
        public @Nonnull
        JDBCCQEndpoint connectingTo(@Nonnull String jdbcUrl,
                                    @Nonnull String user, @Nonnull String password) {
            return new JDBCCQEndpoint(mapping, jdbcUrl,
                                    () -> DriverManager.getConnection(jdbcUrl, user, password));
        }
        public @Nonnull
        JDBCCQEndpoint connectingTo(@Nonnull String jdbcUrl,
                                    @Nonnull Properties properties) {
            return new JDBCCQEndpoint(mapping, jdbcUrl,
                                    () -> DriverManager.getConnection(jdbcUrl, properties));
        }
        public @Nonnull
        JDBCCQEndpoint connectingTo(@Nonnull String name,
                                    @Nonnull ConnectionSupplier connectionSupplier) {
            return new JDBCCQEndpoint(mapping, name, connectionSupplier);
        }
        public @Nonnull
        JDBCCQEndpoint connectingTo(@Nonnull ConnectionSupplier connectionSupplier) {
            return new JDBCCQEndpoint(mapping, connectionSupplier.toString(), connectionSupplier);
        }
    }

    public static @Nonnull Builder createFor(@Nonnull RelationalMapping mapping) {
        return new Builder(mapping);
    }

    /* --- --- --- Getters --- --- --- */

    public @Nonnull Molecule getMolecule() {
        return molecule;
    }
    public @Nonnull MoleculeMatcher getDefaultMatcher() {
        return moleculeMatcher;
    }
    public @Nonnull String getName() {
        return name;
    }
    public @Nonnull Source asSource() {
        return new Source(getDefaultMatcher(), this);
    }

    /* --- --- --- Internals --- --- --- */

    private static class AnnotationStatus {
        final int missingTable, missingColumn, badColumn, size;

        public AnnotationStatus(@Nonnull CQuery query) {
            int missingTable = 0, missingColumns = 0, badColumn = 0;
            this.size = query.size();
            Map<Term, String> s2table = new HashMap<>();
            for (Triple triple : query) {
                Term s = triple.getSubject();
                String table = s2table.computeIfAbsent(s, k -> StarsHelper.findTable(query, s));
                if (table == null) ++missingTable;
                Column column = StarsHelper.getColumn(query, table, triple.getObject());
                if (column == null) ++missingColumns;
                else if (table != null && !column.getTable().equals(table)) ++badColumn;
            }
            this.missingTable = missingTable;
            this.missingColumn = missingColumns;
            this.badColumn = badColumn;
        }

        boolean isValid() {
            return missingTable == 0 && missingColumn == 0 && badColumn == 0;
        }

        boolean isEmpty() {
            return missingTable == size && missingColumn == size;
        }


    }

    private @Nonnull Results runUnderFederation(@Nonnull CQuery query) {
            SimpleFederationModule m = new SimpleFederationModule() {
                @Override
                protected void configureResultsExecutor() {
                    bind(ResultsExecutor.class).toInstance(new SequentialResultsExecutor());
                }
            };
            try (Federation federation = Guice.createInjector(m).getInstance(Federation.class)) {
                federation.addSource(asSource());
                return federation.query(query);
            }
    }

    private static @Nonnull Set<String> getResultVars(@Nonnull CQuery query) {
        if (query.isAsk())
            return Collections.emptySet();
        Projection p = ModifierUtils.getFirst(Projection.class, query.getModifiers());
        if (p != null) return p.getVarNames();
        return query.getTermVars().stream().map(Var::getName).collect(toSet());
    }

    private class SqlResults extends AbstractResults {
        private boolean closed = false;
        private final boolean ask;
        private final @Nonnull Statement stmt;
        private final @Nonnull ResultSet rs;
        private SQLException exception = null;
        private final @Nonnull Queue<Solution> queue = new ArrayDeque<>();
        private final @Nonnull SqlRewriting sql;
        private final @Nonnull Model tmpModel = ModelFactory.createDefaultModel();
        private final @Nonnull List<Query> jenaStars;
        private final @Nonnull List<Set<String>> jenaVars;
        private final @Nonnull List<JenaBindingSolution.Factory> jenaSolutionFac;
        private final @Nonnull List<Set<String>> jVars;
        private final @Nonnull List<Set<String>> jrVars;
        private final @Nullable ArraySolution.ValueFactory projector;

        public SqlResults(@Nonnull SqlRewriting sql,
                          @Nonnull Statement stmt, @Nonnull ResultSet rs) {
            super(getResultVars(sql.getQuery()));
            this.ask = sql.getQuery().isAsk();
            this.stmt = stmt;
            this.rs = rs;
            this.sql = sql;
            this.jenaStars = new ArrayList<>(sql.getStarsCount());
            this.jenaVars = new ArrayList<>(sql.getStarsCount());
            this.jenaSolutionFac = new ArrayList<>(sql.getStarsCount());
            this.jVars = new ArrayList<>(sql.getStarsCount());
            this.jrVars = new ArrayList<>(sql.getStarsCount());
            Set<String> sqlVars = sql.getVars();
            IndexedSet<String> allVars =
                    IndexedSet.fromDistinct(TreeUtils.union(sqlVars, getVarNames()));
            for (int i = 0, size = sql.getStarsCount(); i < size; i++) {
                StarSubQuery star = sql.getStar(i);
                CQuery.Builder b = CQuery.builder(star.getTriples().size());
                for (Triple triple : star.getTriples()) {
                    Term o = triple.getObject();
                    if (o.isVar() && sqlVars.contains(o.asVar().getName()))
                        b.add(triple);
                }
                if (b.isEmpty()) { // we've no object to fetch
                    if (star.getCore().isVar()) { // get the subject
                        b.add(new Triple(star.getCore(), p, o));
                        b.distinct();
                    } else { // degenerate into an ASK query
                        b.add(new Triple(s, p, o));
                        b.ask();
                    }
                }
                FastSPARQLString ss = new FastSPARQLString(b.build());
                jenaStars.add(QueryFactory.create(ss.getSparql()));
                jenaVars.add(allVars.subset(ss.getVarNames()));
                jenaSolutionFac.add(JenaBindingSolution.forVars(jenaVars.get(i)));
                if (i == 0) {
                    jVars.add(Collections.emptySet());
                    jrVars.add(Collections.emptySet());
                } else {
                    IndexedSubset<String> set = allVars.subset(jenaVars.get(i));
                    jVars.add(set.createIntersection(jenaVars.get(i-1)));
                    set.union(jenaVars.get(i-1));
                    jrVars.add(set);
                }
            }
            projector = allVars.equals(getVarNames()) ? null : ArraySolution.forVars(getVarNames());
        }

        @Override
        public int getReadyCount() {
            return queue.size();
        }

        @Override
        public boolean isDistinct() {
            return sql.isDistinct();
        }

        @Override
        public boolean hasNext() {
            while (!closed && queue.isEmpty()) {
                try {
                    if (!rs.next())
                        break;
                    convert();
                } catch (SQLException e) {
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

        private void convert() throws SQLException {
            // results will be a tree of hash joins among the results of each star
            Results results = null;
            try {
                for (int i = 0; i < sql.getStarsCount(); i++) {
                    Results r = executeStar(i);
                    if (results == null) {
                        results = r; // first result, use as root
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

        private @Nonnull Results executeStar(int i) throws SQLException {
            List<String> starVars = sql.getStarVars(i);
            List<Object> starValues = new ArrayList<>(starVars.size());
            for (String v : starVars)
                starValues.add(rs.getObject(v));

            tmpModel.removeAll();
            mapping.toRDF(tmpModel, sql.getStarColumns(i), starValues);
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
                for (SPARQLFilter filter : sql.getPendingFilters()) {
                    if (!filter.evaluate(solution))
                        continue rs_loop;
                }
                if (projector != null) queue.add(projector.fromSolution(solution));
                else                   queue.add(solution);
            }
        }

        @Override
        public @Nonnull Solution next() {
            if (!hasNext())
                throw new NoSuchElementException();
            return queue.remove();
        }

        private void silentClose() {
            try {
                stmt.close();
            } catch (SQLException e) {
                if (exception == null) exception = e;
                else                   exception.addSuppressed(e);
            }
            closed = true;
        }

        @Override
        public void close() throws ResultsCloseException {
            if (exception != null) {
                SQLException cause = this.exception;
                this.exception = null;
                throw new ResultsCloseException(this, "SQLException during hasNext()", cause);
            }
            if (!closed) {
                closed = true;
                try {
                    stmt.close();
                } catch (SQLException e) {
                    throw new ResultsCloseException(this, e);
                }
            }
        }
    }

    /* --- --- --- Interface implementation --- --- --- */

    @Override
    public @Nonnull Results query(@Nonnull CQuery query) {
        AnnotationStatus st = new AnnotationStatus(query);
        if (!st.isValid()) {
            if (st.isEmpty()) {
                return runUnderFederation(query);
            } else {
                throw new IllegalArgumentException("Query is partially annotated! It misses " +
                        "table annotation on "+st.missingTable+" triples, misses column " +
                        "annotations on "+st.missingColumn+" triples and has "+st.badColumn +
                        " ColumnTags not matching the TableTag. Query: "+query);
            }
        }
        IndexedSet<SPARQLFilter> filters = StarsHelper.getFilters(query);
        SqlRewriting sql;
        try {
            sql = sqlGenerator.transform(query, filters);
        } catch (RuntimeException e) {
            throw new QueryExecutionException("Could not generate SQL for "+query, e);
        }
        try {
            Statement stmt = connectionSupplier.connect().createStatement();
            ResultSet rs = stmt.executeQuery(sql.getSql());
            Results results = new SqlResults(sql, stmt, rs);
            // SqlResults implements FILTER()s and projection.
            // Maybe the SQL engine provided DISTINCT and LIMIT. If not (and required) provide here
            if (!sql.isDistinct())
                results = HashDistinctResults.applyIf(results, query);
            // LIMIT must always be re-enforced since the mapping may "unfold" the SQL results
            results = LimitResults.applyIf(results, query);
            return results;
        } catch (SQLException e) {
            throw new QueryExecutionException("Problem executing rewritten SQL query", e);
        }
    }

    @Override
    public @Nonnull Cardinality estimate(@Nonnull CQuery query, int estimatePolicy) {
        return Cardinality.UNSUPPORTED;
    }

    @Override
    public boolean hasRemoteCapability(@Nonnull Capability capability) {
        switch (capability) {
            case PROJECTION:
            case LIMIT:
            case DISTINCT:
            case SPARQL_FILTER:
                return true;
            default:
                // TODO add support for VALUES using IN
                return false;
        }
    }

    /* --- --- --- Object methods --- --- --- */
    @Override
    public String toString() {
        return "JDBCEndpoint("+name+")";
    }
}
