package br.ufsc.lapesd.freqel.rel.cql;

import br.ufsc.lapesd.freqel.algebra.Cardinality;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.freqel.algebra.util.TreeUtils;
import br.ufsc.lapesd.freqel.description.molecules.Molecule;
import br.ufsc.lapesd.freqel.description.molecules.MoleculeMatcher;
import br.ufsc.lapesd.freqel.federation.Federation;
import br.ufsc.lapesd.freqel.federation.SingletonSourceFederation;
import br.ufsc.lapesd.freqel.description.Source;
import br.ufsc.lapesd.freqel.federation.planner.ConjunctivePlanner;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.model.term.std.StdPlain;
import br.ufsc.lapesd.freqel.model.term.std.StdURI;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.MutableCQuery;
import br.ufsc.lapesd.freqel.query.annotations.NoMergePolicyAnnotation;
import br.ufsc.lapesd.freqel.query.endpoint.AbstractTPEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.Capability;
import br.ufsc.lapesd.freqel.query.endpoint.exceptions.QueryExecutionException;
import br.ufsc.lapesd.freqel.query.modifiers.Distinct;
import br.ufsc.lapesd.freqel.query.modifiers.ModifiersSet;
import br.ufsc.lapesd.freqel.query.results.AbstractResults;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.ResultsCloseException;
import br.ufsc.lapesd.freqel.query.results.Solution;
import br.ufsc.lapesd.freqel.query.results.impl.ArraySolution;
import br.ufsc.lapesd.freqel.query.results.impl.HashDistinctResults;
import br.ufsc.lapesd.freqel.query.results.impl.LimitResults;
import br.ufsc.lapesd.freqel.reason.tbox.EmptyTBox;
import br.ufsc.lapesd.freqel.rel.common.*;
import br.ufsc.lapesd.freqel.rel.mappings.RelationalMapping;
import br.ufsc.lapesd.freqel.rel.mappings.context.ContextMapping;
import br.ufsc.lapesd.freqel.rel.sql.RelationalRewriting;
import br.ufsc.lapesd.freqel.rel.sql.impl.DefaultSqlTermWriter;
import br.ufsc.lapesd.freqel.util.indexed.IndexSet;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.*;

public class CassandraCQEndpoint extends AbstractTPEndpoint implements CQEndpoint {
    private static final Logger logger = LoggerFactory.getLogger(CassandraCQEndpoint.class);
    private static final int ROWS_QUEUE_MAX = 16384;

    private @Nonnull final String name;
    private @Nonnull final CqlSession cqlSession;
    private @Nonnull final Molecule molecule;
    private final boolean sharedSession;
    private @Nonnull final RelationalMoleculeMatcher moleculeMatcher;
    private @Nonnull final RelationalMapping mapping;
    private @Nonnull final CqlGenerator cqlGenerator;
    private @Nullable Federation federation;
    private @Nullable ConjunctivePlanner planner;

    /* --- --- --- Constructor and builder --- --- --- */

    public CassandraCQEndpoint(@Nonnull CqlSession cqlSession, @Nonnull String name,
                               @Nonnull RelationalMapping mapping, boolean sharedSession) {
        this.cqlSession = cqlSession;
        this.name = name;
        this.mapping = mapping;
        this.molecule = mapping.createMolecule();
        this.sharedSession = sharedSession;
        NoMergePolicyAnnotation policy = new NoMergePolicyAnnotation();
        this.moleculeMatcher = new RelationalMoleculeMatcher(this.molecule, new EmptyTBox(), policy);
        this.cqlGenerator = new CqlGenerator(DefaultSqlTermWriter.INSTANCE);
    }

    public static @Nonnull ProtoBuilder builder() {
        return new ProtoBuilder();
    }

    public static class ProtoBuilder {
        private @Nullable String name;

        public @Nonnull SessionBuilder connectingTo(@Nonnull String host) {
            return new SessionBuilder(host);
        }
        public @Nonnull ProtoBuilder withName(@Nonnull String name) {
            this.name = name;
            return this;
        }
        public @Nonnull MappingBuilder sharing(@Nonnull CqlSession session) {
            return new MappingBuilder(session, name == null ? session.getName() : name, true);
        }
        public @Nonnull MappingBuilder owning(@Nonnull CqlSession session) {
            return new MappingBuilder(session, name == null ? session.getName() : name, false);
        }
    }

    public static class SessionBuilder {
        private @Nonnull String node, datacenter = "datacenter1";
        private @Nonnull String user = "cassandra", password = "cassandra";
        private @Nullable String cqEndpointName = null;
        private @Nullable CqlSession session;
        private int port = 9042;

        public SessionBuilder(@Nonnull String node) {
            this.node = node;
        }

        public @CanIgnoreReturnValue @Nonnull SessionBuilder session(@Nonnull CqlSession session) {
            this.session = session;
            return this;
        }

        public @CanIgnoreReturnValue @Nonnull SessionBuilder cqEndpointName(@Nonnull String name) {
            this.cqEndpointName = name;
            return this;
        }

        public @CanIgnoreReturnValue @Nonnull SessionBuilder node(@Nonnull String node) {
            this.node = node;
            return this;
        }

        public @CanIgnoreReturnValue @Nonnull SessionBuilder datacenter(@Nonnull String datacenter) {
            this.datacenter = datacenter;
            return this;
        }

        public @CanIgnoreReturnValue @Nonnull SessionBuilder user(@Nonnull String user) {
            this.user = user;
            return this;
        }

        public @CanIgnoreReturnValue @Nonnull SessionBuilder password(@Nonnull String password) {
            this.password = password;
            return this;
        }

        public @CanIgnoreReturnValue @Nonnull SessionBuilder port(int port) {
            this.port = port;
            return this;
        }

        private @Nonnull CqlSession getSession() {
            if (this.session == null) {
                return CqlSession.builder()
                        .addContactPoint(new InetSocketAddress(node, port))
                        .withLocalDatacenter(datacenter)
                        .withAuthCredentials(user,password).build();
            } else {
                return this.session;
            }
        }

        private @Nonnull String getName() {
            return this.cqEndpointName != null ? this.cqEndpointName : (this.node+":"+port);
        }

        public @Nonnull MappingBuilder connect() {
            return new MappingBuilder(getSession(), getName(), false);
        }
    }

    public static class MappingBuilder {
        private @Nonnull final String name;
        private @Nonnull final CqlSession session;
        private @Nonnull String uriPrefix = StdPlain.URI_PREFIX;
        private final boolean sharedSession;
        private static final Set<String> systemKeyspaces = Sets.newHashSet("system",
                "system_distributed", "system_schema", "system_traces", "system_auth");


        public MappingBuilder(@Nonnull CqlSession session, @Nonnull String name,
                              boolean sharedSession) {
            this.name = name;
            this.session = session;
            this.sharedSession = sharedSession;
        }

        public @CanIgnoreReturnValue @Nonnull MappingBuilder setPrefix(@Nonnull String prefix) {
            this.uriPrefix = prefix;
            return this;
        }

        private @Nonnull RelationalMapping fetchSchema(@Nullable String keyspace) {
            ContextMapping.Builder builder = ContextMapping.builder();
            String selectTables = "SELECT table_name, keyspace_name FROM system_schema.tables" +
                    (keyspace != null ? " WHERE keyspace_name = '"+keyspace+"';" : ";");
            ResultSet tables = session.execute(selectTables);
            for (Row tablesRow : tables) {
                String ks = tablesRow.getString("keyspace_name");
                String tb = tablesRow.getString("table_name");
                if (ks == null) {
                    assert false : "Null keyspace_name on Cassandra for table "+tb;
                    continue;
                }
                if (systemKeyspaces.contains(ks))
                    continue;
                if (tb == null) {
                    assert false : "Null table_name on Cassandra for keyspace "+ks+
                                   ", filtering by keyspace "+keyspace;
                    continue;
                }
                String tbQName = ks + "." + tb;
                ContextMapping.TableBuilder tableBuilder = builder.beginTable(tbQName);
                String selectColumns = "SELECT column_name FROM system_schema.columns" +
                        " WHERE table_name = '" + tb + "'" +
                        (keyspace != null ? " AND keyspace_name = '" + keyspace + "'" : "") +
                        " ALLOW FILTERING;";
                for (Row columnsRow : session.execute(selectColumns)) {
                    String column = columnsRow.getString("column_name");
                    if (column == null) {
                        assert false : "Null column_name for table "+tb+" in keyspace "+keyspace;
                        continue;
                    }
                    tableBuilder.column2uri(column, new StdURI(uriPrefix+column));
                }
                builder = tableBuilder.endTable();
            }
            return builder.build();
        }

        public @Nonnull CassandraCQEndpoint build(@Nonnull String keyspace) {
            return build(fetchSchema(keyspace));
        }

        public @Nonnull CassandraCQEndpoint build() {
            return build(fetchSchema(null));
        }

        public @Nonnull CassandraCQEndpoint build(@Nonnull RelationalMapping mapping) {
            return new CassandraCQEndpoint(session, name, mapping, sharedSession);
        }
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
        Source src = new Source(getDefaultMatcher(), this, getName());
        src.setCloseEndpoint(true);
        return src;
    }

    /* --- --- --- Interface implementation --- --- --- */

    @Override
    public @Nonnull Results query(@Nonnull CQuery query) {
        AnnotationStatus st = new AnnotationStatus(query);
        if (!st.isValid()) {
            if (st.isEmpty()) return getFederation().query(query);
            else              st.checkNotPartiallyAnnotated(); //throws IllegalArgumentException
        }
        RelationalRewriting cql;
        try {
            cql = cqlGenerator.transform(query);
            logger.debug("{} Query:\n  {}\nSQL:\n  {}", this, query.toString().replace("\n", "\n  "),
                    cql.getRelationalQuery().replace("\n", "\n  "));
        } catch (RuntimeException e) {
            throw new QueryExecutionException("Could not generate SQL for "+query, e);
        } catch (MultiStarException e) {
            return decomposeMultiStar(e.getIndex());
        }
        CompletionStage<AsyncResultSet> rs = cqlSession.executeAsync(cql.getRelationalQuery());
        Results results = new CassandraResults(cql, rs);
        // SqlResults implements FILTER()s and projection.
        // Maybe the SQL engine provided DISTINCT and LIMIT. If not (and required) provide here
        results = HashDistinctResults.applyIf(results, query);
        // LIMIT must always be re-enforced since the mapping may "unfold" the SQL results
        results = LimitResults.applyIf(results, query);
        return results;
    }

    @Override public double alternativePenalty(@NotNull CQuery query) {
        return 0.80;
    }

    @Override
    public @Nonnull Cardinality estimate(@Nonnull CQuery query, int estimatePolicy) {
        return Cardinality.UNSUPPORTED;
    }

    @Override
    public boolean hasRemoteCapability(@Nonnull Capability capability) {
        switch (capability) {
            case ASK:
            case PROJECTION:
            case DISTINCT:
            case SPARQL_FILTER:
            case LIMIT:
                return true;
            default:
                return false;
        }
    }

    @Override public boolean requiresBindWithOverride() {
        return true;
    }

    @Override
    public void close() {
        if (!sharedSession)
            cqlSession.close();
        if (federation != null) {
            Federation local = this.federation;
            federation = null; //avoid infinite recursion when federation closes me
            local.close();
        }
    }

    /* --- --- --- Internals --- --- --- */

    protected class CassandraResults extends AbstractResults {
        private @Nonnull CompletableFuture<Void> fetchFuture;
        private @Nonnull final BlockingQueue<Solution> queue = new LinkedBlockingDeque<>();
        private @Nullable Solution current = null;
        private @Nonnull final ConverterResults converterResults;
        private final boolean ask;
        private boolean stop = false, askResult = false, askReported = false;
        private boolean exhaustedProduction = false, exhaustedConsumption = false;

        public CassandraResults(@Nonnull RelationalRewriting rw,
                                @Nonnull CompletionStage<AsyncResultSet> rsStage) {
            super(rw.getQuery().attr().publicTripleVarNames());
            converterResults = new ConverterResults(rw, mapping);
            ask = rw.getQuery().attr().isAsk();
            fetchFuture = rsStage.handle(this::handle).toCompletableFuture();
        }

        private Void handle(AsyncResultSet rs, Throwable throwable) {
            boolean last = throwable != null || rs == null || stop || !rs.hasMorePages();
            if (throwable != null)
                logger.error("Failed to fetch query results", throwable);
            else if (rs == null)
                logger.error("Null AsyncResultSet without Throwable!");
            synchronized (converterResults) {
                if (!last) {
                    // start while holding the lock to avoid its handler running before this
                    fetchFuture = rs.fetchNextPage().handle(this::handle)
                                    .toCompletableFuture();
                }
                if (!stop && rs != null) {
                    for (Row row : rs.currentPage()) {
                        converterResults.nextRow = row;
                        while (converterResults.hasNext()) {
                            askResult = true;
                            queue.add(converterResults.next());
                        }
                    }
                }
                if (last) {
                    exhaustedProduction = true;
                    queue.add(ArraySolution.EMPTY);
                }
            }
            return null;
        }

        @Override
        public boolean isAsync() {
            return true;
        }

        @Override
        public boolean hasNext() {
            boolean interrupted = false;
            while (!exhaustedConsumption && current == null) {
                try {
                    current = queue.take();
                    if (current.isEmpty()) {
                        if (ask && !askReported) {
                            askReported = true;
                            if (!askResult)
                                current = null;
                        } else {
                            current = null;
                        }
                        if (exhaustedProduction)
                            exhaustedConsumption = true;
                    }
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted)
                Thread.currentThread().interrupt();
            return current != null;
        }

        @Override
        public @Nonnull Solution next() {
            if (!hasNext())
                throw new NoSuchElementException();
            Solution current = this.current;
            assert current != null;
            this.current = null;
            return current;
        }

        @Override
        public void close() throws ResultsCloseException {
            CassandraCQEndpoint ep = CassandraCQEndpoint.this;
            stop = true;
            try {
                fetchFuture.get(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.error("{}.close() interrupted for endpoint {}", this, ep);
            } catch (ExecutionException e) {
                throw new ResultsCloseException(this, e.getCause());
            } catch (TimeoutException e) {
                logger.error("Timed out waiting for pending Cassandra fetch to finish on " +
                             "{} over endpoint {}", this, ep);
            }
        }

        private class ConverterResults extends RelationalResults {
            private @Nullable Row row, nextRow;

            protected ConverterResults(@Nonnull RelationalRewriting rw,
                                       @Nonnull RelationalMapping mapping) {
                super(rw, mapping);
            }

            @Override
            protected boolean relationalAdvance() {
                row = nextRow;
                nextRow = null;
                return row != null;
            }

            @Override
            protected void relationalClose() {}

            @Override
            protected @Nullable Object relationalGetValue(String relationalVar) {
                if (row == null)
                    throw new IllegalStateException("No row set");
                return row.getObject(relationalVar);
            }
        }
    }

    private @Nonnull Federation getFederation() {
        if (federation == null)
            federation = SingletonSourceFederation.createFederation(asSource());
        return federation;
    }

    private @Nonnull Results decomposeMultiStar(@Nonnull StarVarIndex index) {
        assert index.getStarCount() > 1;
        CQuery query = index.getQuery();
        boolean distinct = query.getModifiers().distinct() != null;
        IndexSet<String> varsUniverse = query.attr().varNamesUniverseOffer();
        IndexSet<Triple> triplesUniverse = query.attr().triplesUniverseOffer();
        List<Op> leaves = new ArrayList<>();
        for (int i = 0, size = index.getStarCount(); i < size; i++) {
            StarSubQuery star = index.getStar(i);
            MutableCQuery cQuery = MutableCQuery.from(star.getTriples());
            if (varsUniverse != null) cQuery.attr().offerVarNamesUniverse(varsUniverse);
            if (triplesUniverse != null) cQuery.attr().offerTriplesUniverse(triplesUniverse);
            cQuery.mutateModifiers().addAll(star.getFilters());
            if (distinct)
                cQuery.mutateModifiers().add(Distinct.INSTANCE);
            leaves.add(new EndpointQueryOp(this, cQuery));
        }
        // optimize as usual, then execute under the inner federation
        ConjunctivePlanner planner = SingletonSourceFederation.getInjector().getInstance(ConjunctivePlanner.class);
        Op plan = planner.plan(query, leaves);
        ModifiersSet planModifiers = plan.modifiers();
        planModifiers.addAll(index.getCrossStarFilters());
        TreeUtils.copyNonFilter(plan, query.getModifiers());
        return getFederation().execute(plan);
    }

    /* --- --- --- Object methods --- --- --- */

    @Override
    public String toString() {
        return "CassandraCQEndpoint("+name+")";
    }
}
