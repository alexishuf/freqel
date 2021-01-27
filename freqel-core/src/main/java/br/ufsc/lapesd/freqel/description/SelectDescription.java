package br.ufsc.lapesd.freqel.description;

import br.ufsc.lapesd.freqel.federation.spec.source.SourceCache;
import br.ufsc.lapesd.freqel.jena.JenaWrappers;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.model.term.std.StdURI;
import br.ufsc.lapesd.freqel.model.term.std.StdVar;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.MutableCQuery;
import br.ufsc.lapesd.freqel.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.Capability;
import br.ufsc.lapesd.freqel.query.endpoint.MissingCapabilityException;
import br.ufsc.lapesd.freqel.query.modifiers.Distinct;
import br.ufsc.lapesd.freqel.query.modifiers.Projection;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.util.LogUtils;
import com.esotericsoftware.yamlbeans.YamlReader;
import com.esotericsoftware.yamlbeans.YamlWriter;
import com.google.common.base.Stopwatch;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.*;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

/**
 * A description that performs a SPARQL SELECT against the endpoint to get predicates
 * (and possibly classes).
 */
public class SelectDescription implements Description {
    private static final Logger logger = LoggerFactory.getLogger(SelectDescription.class);
    protected static final @Nonnull StdURI TYPE = new StdURI(RDF.type.getURI());

    private final @Nonnull CQEndpoint endpoint;
    private final boolean fetchClasses;
    protected @Nullable Set<Term> predicates, classes;
    private @Nullable Future<?> updateTask = null;
    private @Nullable SaveSpec saveSpec = null;
    private boolean isUpdated = false;

    public SelectDescription(@Nonnull CQEndpoint endpoint) throws MissingCapabilityException {
        this(endpoint, false);
    }

    public SelectDescription(@Nonnull CQEndpoint endpoint,
                             boolean fetchClasses) throws MissingCapabilityException {
        Capability.PROJECTION.requireFrom(endpoint);
        Capability.DISTINCT.requireFrom(endpoint);
        this.endpoint = endpoint;
        this.fetchClasses = fetchClasses;
    }

    public SelectDescription(@Nonnull CQEndpoint endpoint, @Nonnull State state) {
        this.endpoint = endpoint;
        predicates = State.toSet(state.predicates);
        fetchClasses = state.classes != null;
        classes = fetchClasses ? State.toSet(state.classes) : null;
        isUpdated = true;
    }

    private static class State {
        public List<String> predicates = null;
        public List<String> classes = null;

        private static @Nonnull Set<Term> toSet(@Nonnull List<String> list) {
            Set<Term> set = new HashSet<>(list.size());
            for (String uri : list)
                set.add(JenaWrappers.fromJena(ResourceFactory.createResource(uri)));
            return set;
        }
    }

    private static class SaveSpec {
        @Nonnull SourceCache cache;
        @Nonnull String endpointId;

        public SaveSpec(@Nonnull SourceCache cache, @Nonnull String endpointId) {
            this.cache = cache;
            this.endpointId = endpointId;
        }

        void save(@Nonnull Set<Term> predicates, @Nullable Set<Term> classes) throws IOException {
            cache.reloadIndex();
            File file = cache.createFile("select-description", "yaml", endpointId);
            try (FileOutputStream stream = new FileOutputStream(file);
                 OutputStreamWriter writer = new OutputStreamWriter(stream, UTF_8)) {
                YamlWriter yamlWriter = new YamlWriter(writer);
                State st = new State();
                st.predicates = predicates.stream().filter(Term::isURI)
                                            .map(t -> t.asURI().getURI()).collect(toList());
                if (classes != null) {
                    st.classes = classes.stream().filter(Term::isURI).map(t -> t.asURI().getURI())
                                         .collect(toList());
                }
                yamlWriter.write(st);
                yamlWriter.close();
            }
        }
    }

    public static @Nullable SelectDescription
    fromCache(@Nonnull CQEndpoint endpoint, @Nonnull SourceCache cache,
              @Nonnull String endpointId) throws IOException {
        File file = cache.getFile("select-description", endpointId);
        if (file != null)
            return fromYaml(endpoint, file);
        return null;
    }
    public static @Nonnull SelectDescription
    fromYaml(@Nonnull CQEndpoint endpoint, @Nonnull File file) throws IOException {
        try (FileInputStream inputStream = new FileInputStream(file)) {
            return fromYaml(endpoint, inputStream);
        }
    }
    public static @Nonnull SelectDescription
    fromYaml(@Nonnull CQEndpoint endpoint, @Nonnull InputStream inputStream) throws IOException {
        try (InputStreamReader reader = new InputStreamReader(inputStream, UTF_8)) {
            return fromYaml(endpoint, reader);
        }
    }
    public static @Nonnull SelectDescription
    fromYaml(@Nonnull CQEndpoint endpoint, @Nonnull Reader reader) throws IOException {
        YamlReader yamlReader = new YamlReader(reader);
        State state = yamlReader.read(State.class);
        return new SelectDescription(endpoint, state);
    }

    protected boolean initSync() {
        init();
        return waitForInit(60000);
    }

    public synchronized void saveWhenReady(@Nonnull SourceCache sourceCache,
                                           @Nonnull String endpointId) {
        saveSpec = new SaveSpec(sourceCache, endpointId);
        if (isUpdated)
            doSaveSpec(); // do it now, since it is ready
    }

    private void doSaveSpec() {
        if (saveSpec == null) return;
        assert predicates != null;
        assert !fetchClasses || classes != null;
        try {
            Stopwatch sw = Stopwatch.createStarted();
            saveSpec.save(predicates, classes);
            logger.debug("Saved description for {} at {} in {}ms", endpoint,
                         saveSpec.cache.getDir(), sw.elapsed(MICROSECONDS)/1000.0);
        } catch (IOException e) {
            logger.error("Problem saving SelectDescription for endpoint {} at cache dir {}",
                         saveSpec.endpointId, saveSpec.cache.getDir(), e);
        }
    }

    /**
     * Issues SELECT queries against the SPARQL service at getSparqlAddress()
     */
    @Override
    public boolean updateSync(int timeoutMilliseconds) {
        Future<?> localTask;
        synchronized (this) {
            update();
            assert updateTask != null;
            localTask = this.updateTask;
        }
        try {
            localTask.get(timeoutMilliseconds, TimeUnit.MILLISECONDS);
            synchronized (this) {
                if (updateTask == localTask)
                    updateTask = null;
                assert isUpdated;
            }
            return true;
        } catch (InterruptedException e) {
            logger.warn("updateSync interrupted");
        } catch (ExecutionException e) {
            logger.error("Failed to update description. Will not match anything", e);
            synchronized (this) {
                if (localTask == updateTask) {
                    updateTask = null;
                    if (predicates == null) predicates = Collections.emptySet();
                    if (fetchClasses && classes == null) classes = Collections.emptySet();
                }
            }
        } catch (TimeoutException e) {
            logger.warn("updateSync timed out after {} ms", timeoutMilliseconds);
        }
        return false;
    }

    private void doUpdate() {
        StdVar s = new StdVar("s"), p = new StdVar("p"), o = new StdVar("o");
        Stopwatch sw = Stopwatch.createStarted();
        Set<Term> predicates = fill(new Triple(s, p, o), "p");
        logger.debug("Fetched {} predicates from {} in {}ms",
                     predicates.size(), endpoint, sw.elapsed(MICROSECONDS)/1000.0);
        synchronized (this) {
            this.predicates = predicates;
        }
        if (fetchClasses) {
            Set<Term> classes = fill(new Triple(s, TYPE, o), "o");
            logger.debug("Fetched {} classes from {} in {}ms",
                         classes.size(), endpoint, sw.elapsed(MICROSECONDS)/1000.0);
            synchronized (this) {
                this.classes = classes;
            }
        }
        doSaveSpec();
        synchronized (this) {
            updateTask = null;
            isUpdated = true;
        }
    }

    @Override
    public synchronized void update() {
        if (updateTask == null) {
            isUpdated = false;
            updateTask = ForkJoinPool.commonPool().submit(this::doUpdate);
        }
    }

    @Override
    public synchronized void init() {
        if (!isUpdated && updateTask == null) update();
    }

    @Override
    public boolean waitForInit(int timeoutMilliseconds) {
        Future<?> localTask;
        synchronized (this) {
            if  (isUpdated) {
                assert predicates != null && (!fetchClasses || classes != null);
                return true; // is ready
            } else if (updateTask == null) {
                return false; // init() not called
            } else {
                localTask = this.updateTask; // wait outside of the monitor
            }
        }
        try {
            localTask.get(timeoutMilliseconds, MILLISECONDS);
            synchronized (this) {
                if (updateTask == localTask)
                    updateTask = null;
                assert isUpdated;
            }
        } catch (InterruptedException e) {
            logger.warn("waitForInit({}) interrupted", timeoutMilliseconds);
        } catch (ExecutionException e) {
            logger.error("Failed to init descriptio. Will not match anything", e);
            synchronized (this) {
                if (localTask == updateTask) {
                    updateTask = null;
                    if (predicates == null) predicates = Collections.emptySet();
                    if (fetchClasses && classes == null) classes = Collections.emptySet();
                }
            }
        } catch (TimeoutException e) {
            logger.warn("waitForInit timed out after {} ms", timeoutMilliseconds);
        }
        assert !isUpdated || ( predicates != null && (!fetchClasses || classes != null) );
        return isUpdated;
    }

    private Set<Term> fill(@Nonnull Triple query, @Nonnull String varName) {
        Stopwatch sw = Stopwatch.createStarted();
        MutableCQuery cQuery = MutableCQuery.from(query);
        cQuery.mutateModifiers().add(Projection.of(varName));
        cQuery.mutateModifiers().add(Distinct.INSTANCE);
        Set<Term> set = null;
        try (Results results = endpoint.query(cQuery)) {
            set = new HashSet<>();
            while (results.hasNext())
                set.add(requireNonNull(results.next().get(varName)));
            return set;
        } catch (Exception e) {
            logger.error("Problem fetching results from {} for {}", endpoint,
                         LogUtils.toString(cQuery), e);
            throw e;
        } finally {
            if (set != null)
                LogUtils.logQuery(logger, cQuery, endpoint, set.size(), sw);
        }
    }

    @Override
    public @Nonnull CQueryMatch match(@Nonnull CQuery query) {
        CQueryMatch.Builder b = null;
        if (!initSync())
            return CQueryMatch.EMPTY; //return empty result if timed out or error
        Set<Term> predicates = requireNonNull(this.predicates);
        for (Triple triple : query) {
            Term p = triple.getPredicate();
            Term o = triple.getObject();
            if (classes != null && p.equals(TYPE) && o.isGround()) {
                if (classes.contains(o))
                    (b == null ? (b = CQueryMatch.builder(query)) : b).addTriple(triple);
            } else if (p.isVar() || predicates.contains(p)) {
                (b == null ? (b = CQueryMatch.builder(query)) : b).addTriple(triple);
            }
        }
        return b == null ? CQueryMatch.EMPTY : b.build();
    }
}
