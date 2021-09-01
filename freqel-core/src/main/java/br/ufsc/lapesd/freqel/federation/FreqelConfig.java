package br.ufsc.lapesd.freqel.federation;

import br.ufsc.lapesd.freqel.algebra.util.RelativeCardinalityAdder;
import br.ufsc.lapesd.freqel.cardinality.impl.*;
import br.ufsc.lapesd.freqel.federation.concurrent.PoolPlanningExecutorService;
import br.ufsc.lapesd.freqel.federation.decomp.agglutinator.StandardAgglutinator;
import br.ufsc.lapesd.freqel.federation.decomp.match.SourcesListMatchingStrategy;
import br.ufsc.lapesd.freqel.federation.execution.InjectedExecutor;
import br.ufsc.lapesd.freqel.federation.execution.tree.impl.LazyCartesianOpExecutor;
import br.ufsc.lapesd.freqel.federation.execution.tree.impl.SimpleEmptyOpExecutor;
import br.ufsc.lapesd.freqel.federation.execution.tree.impl.SimplePipeOpExecutor;
import br.ufsc.lapesd.freqel.federation.execution.tree.impl.SimpleQueryOpExecutor;
import br.ufsc.lapesd.freqel.federation.execution.tree.impl.joins.DefaultJoinOpExecutor;
import br.ufsc.lapesd.freqel.federation.execution.tree.impl.joins.bind.SimpleBindJoinResults;
import br.ufsc.lapesd.freqel.federation.execution.tree.impl.joins.hash.ParallelInMemoryHashJoinResults;
import br.ufsc.lapesd.freqel.federation.performance.NoOpPerformanceListener;
import br.ufsc.lapesd.freqel.federation.planner.conjunctive.GreedyJoinOrderPlanner;
import br.ufsc.lapesd.freqel.federation.planner.conjunctive.bitset.BitsetConjunctivePlannerDispatcher;
import br.ufsc.lapesd.freqel.federation.planner.equiv.NoEquivCleaner;
import br.ufsc.lapesd.freqel.federation.planner.utils.DefaultFilterJoinPlanner;
import br.ufsc.lapesd.freqel.reason.tbox.NoEndpointReasoner;
import br.ufsc.lapesd.freqel.reason.tbox.NoOpTBoxMaterializer;
import br.ufsc.lapesd.freqel.reason.tbox.TBoxSpec;
import br.ufsc.lapesd.freqel.util.DictTree;
import com.github.lapesd.rdfit.util.Utils;
import com.google.common.base.Splitter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Pattern;

import static java.lang.Runtime.getRuntime;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;

@SuppressWarnings("UnusedReturnValue")
public class FreqelConfig {
    private static final Splitter LIST_SPLITTER = Splitter.onPattern("\\s*,\\s*")
                                                          .omitEmptyStrings().trimResults();
    private static final Pattern URI_RX = Pattern.compile("^[^:]+:");
    private static final Pattern CLASS_RX = Pattern.compile("(\\w+\\.)*\\w+");
    private final Map<Key, Object> values = new HashMap<>();

    public static class InvalidValueException extends Exception {
        public InvalidValueException(@Nonnull Key key, @Nullable Object value, @Nullable String reason) {
            super(format("Value %s is not valid for %s%s",
                         value, key, reason == null ? "" : ": "+reason));
        }
    }

    public enum ConfigSource {
        JAVA_PROPERTIES,
        ENVIRONMENT,
        RESOURCE,
        CWD_FILE,
        HOME_FILE;

        private static ConfigSource[] fromLowestPrecedenceArray;
        public static @Nonnull ConfigSource[] fromLowestPrecedence() {
            if (fromLowestPrecedenceArray == null) {
                ArrayList<ConfigSource> list = new ArrayList<>(asList(values()));
                Collections.reverse(list);
                fromLowestPrecedenceArray = list.toArray(new ConfigSource[0]);
            }
            return fromLowestPrecedenceArray;
        }
    }

    public enum Key {
        ENDPOINT_REASONER {
            @Override public @Nonnull String parse(@Nullable Object value) throws InvalidValueException {
                return parseClassName(value);
            }
        },
        REPLACEMENT_PRUNE_BY_DESCRIPTION {
            @Override public @Nonnull Boolean parse(@Nullable Object value) throws InvalidValueException {
                return parseBool(value);
            }
        },
        TBOX_HDT {
            @Override public @Nullable String parse(@Nullable Object value) throws InvalidValueException {
                return value == null ? null : value.toString();
            }
        },
        TBOX_RDF {
            @Override public @Nullable  String parse(@Nullable Object value) throws InvalidValueException {
                return value == null ? null : value.toString();
            }
        },
        MATERIALIZER {
            @Override public @Nullable String parse(@Nullable Object value) throws InvalidValueException {
                return parseClassName(value);
            }
        },
        MATERIALIZER_SPEC {
            @Override public @Nullable Object parse(@Nullable Object value) throws InvalidValueException {
                if (value == null || value instanceof TBoxSpec)
                    return value;
                TBoxSpec spec = new TBoxSpec();
                if (value instanceof Object[]) {
                    for (Object o : (Object[]) value) spec.add(o);
                } else if (value instanceof Iterable) {
                    for (Object o : (Iterable<?>) value) spec.add(o);
                } else {
                    spec.add(value);
                }
                return spec;
            }
        },
        MATERIALIZER_STORAGE {
            @Override public @Nullable File parse(@Nullable Object value) throws InvalidValueException {
                return parseDir(value);
            }
        },
        TEMP_DIR {
            @Override public @Nullable File parse(@Nullable Object value) throws InvalidValueException {
                return parseDir(value);
            }
        },
        SOURCES_CACHE_DIR {
            @Override public @Nullable File parse(@Nullable Object value) throws InvalidValueException {
                return parseDir(value);
            }
        },
        MATCHING {
            @Override public @Nullable String parse(@Nullable Object value) throws InvalidValueException {
                return parseClassName(value);
            }
        },
        RESULTS_EXECUTOR_CONCURRENCY_FACTOR {
            @Override public @Nonnull Double parse(@Nullable Object value) throws InvalidValueException {
                return parseDouble(value);
            }
        },
        RESULTS_EXECUTOR_BUFFER_SIZE {
            @Override public @Nonnull Integer parse(@Nullable Object value) throws InvalidValueException {
                return parseInteger(value);
            }
        },
        ESTIMATE_LIMIT {
            @Override public @Nonnull Integer parse(@Nullable Object value) throws InvalidValueException {
                return parseInteger(value);
            }
        },
        ESTIMATE_ASK_LOCAL {
            @Override public @Nonnull Boolean parse(@Nullable Object value) throws InvalidValueException {
                return parseBool(value);
            }
        },
        ESTIMATE_QUERY_LOCAL {
            @Override public @Nonnull Boolean parse(@Nullable Object value) throws InvalidValueException {
                return parseBool(value);
            }
        },
        ESTIMATE_ASK_REMOTE {
            @Override public @Nonnull Boolean parse(@Nullable Object value) throws InvalidValueException {
                return parseBool(value);
            }
        },
        ESTIMATE_QUERY_REMOTE {
            @Override public @Nonnull Boolean parse(@Nullable Object value) throws InvalidValueException {
                return parseBool(value);
            }
        },
        PERFORMANCE_LISTENER {
            @Override public @Nullable String parse(@Nullable Object value) throws InvalidValueException {
                return parseClassName(value);
            }
        },
        AGGLUTINATOR {
            @Override public @Nullable String parse(@Nullable Object value) throws InvalidValueException {
                return parseClassName(value);
            }
        },
        CONJUNCTIVE_PLANNER {
            @Override public @Nullable String parse(@Nullable Object value) throws InvalidValueException {
                return parseClassName(value);
            }
        },
        JOIN_ORDER_PLANNER {
            @Override public @Nullable String parse(@Nullable Object value) throws InvalidValueException {
                return parseClassName(value);
            }
        },
        FILTER_JOIN_PLANNER {
            @Override public @Nullable String parse(@Nullable Object value) throws InvalidValueException {
                return parseClassName(value);
            }
        },
        CARDINALITY_ADDER {
            @Override public @Nullable String parse(@Nullable Object value) throws InvalidValueException {
                return parseClassName(value);
            }
        },
        INNER_CARDINALITY_COMPUTER {
            @Override public @Nullable String parse(@Nullable Object value) throws InvalidValueException {
                return parseClassName(value);
            }
        },
        JOIN_CARDINALITY_ESTIMATOR {
            @Override public @Nullable String parse(@Nullable Object value) throws InvalidValueException {
                return parseClassName(value);
            }
        },
        CARDINALITY_ENSEMBLE {
            @Override public @Nullable String parse(@Nullable Object value) throws InvalidValueException {
                return parseClassName(value);
            }
        },
        CARDINALITY_HEURISTICS {
            @Override public @Nullable Set<String> parse(@Nullable Object v) throws InvalidValueException {
                return parseSet(v, String.class);
            }
        },
        FAST_CARDINALITY_HEURISTIC {
            @Override public @Nullable String parse(@Nullable Object value) throws InvalidValueException {
                return parseClassName(value);
            }
        },
        CARDINALITY_COMPARATOR {
            @Override public @Nullable String parse(@Nullable Object value) throws InvalidValueException {
                return parseClassName(value);
            }
        },
        LARGE_CARDINALITY_THRESHOLD {
            @Override public @Nonnull Integer parse(@Nullable Object value) throws InvalidValueException {
                return parseInteger(value);
            }
        },
        HUGE_CARDINALITY_THRESHOLD {
            @Override public @Nonnull Integer parse(@Nullable Object value) throws InvalidValueException {
                return parseInteger(value);
            }
        },
        REL_CARDINALITY_ADDER_NONEMPTY_MIN {
            @Override public @Nonnull Integer parse(@Nullable Object value) throws InvalidValueException {
                return parseInteger(value);
            }
        },
        REL_CARDINALITY_ADDER_NONEMPTY_PROPORTION {
            @Override public @Nonnull Double parse(@Nullable Object value) throws InvalidValueException {
                return parseDouble(value);
            }
        },
        REL_CARDINALITY_ADDER_UNSUPPORTED_PROPORTION {
            @Override public @Nonnull Double parse(@Nullable Object value) throws InvalidValueException {
                return parseDouble(value);
            }
        },
        EQUIV_CLEANER {
            @Override public @Nullable String parse(@Nullable Object value) throws InvalidValueException {
                return parseClassName(value);
            }
        },
        PLANNING_EXECUTOR {
            @Override public @Nullable String parse(@Nullable Object value) throws InvalidValueException {
                return parseClassName(value);
            }
        },
        PLANNING_CORE_THREADS {
            @Override public @Nonnull Integer parse(@Nullable Object value) throws InvalidValueException {
                return parseInteger(value);
            }
        },
        PLANNING_MAX_THREADS {
            @Override public @Nonnull Integer parse(@Nullable Object value) throws InvalidValueException {
                return parseInteger(value);
            }
        },
        PREPLANNER_FLATTEN {
            @Override public @Nonnull Boolean parse(@Nullable Object value) throws InvalidValueException {
                return parseBool(value, true);
            }
        },
        PREPLANNER_CARTESIAN_INTRODUCTION {
            @Override public @Nonnull Boolean parse(@Nullable Object value) throws InvalidValueException {
                return parseBool(value, true);
            }
        },
        PREPLANNER_UNION_DISTRIBUTION {
            @Override public @Nonnull Boolean parse(@Nullable Object value) throws InvalidValueException {
                return parseBool(value, true);
            }
        },
        PREPLANNER_CARTESIAN_DISTRIBUTION {
            @Override public @Nonnull Boolean parse(@Nullable Object value) throws InvalidValueException {
                return parseBool(value, true);
            }
        },
        PREPLANNER_PUSH_FILTERS {
            @Override public @Nonnull Boolean parse(@Nullable Object value) throws InvalidValueException {
                return parseBool(value, true);
            }
        },
        POSTPLANNER_CONJUNCTION_REPLACE {
            @Override public @Nonnull Boolean parse(@Nullable Object value) throws InvalidValueException {
                return parseBool(value, true);
            }
        },
        POSTPLANNER_FILTER2BIND {
            @Override public @Nonnull Boolean parse(@Nullable Object value) throws InvalidValueException {
                return parseBool(value, true);
            }
        },
        POSTPLANNER_PUSH_DISTINCT {
            @Override public @Nonnull Boolean parse(@Nullable Object value) throws InvalidValueException {
                return parseBool(value, true);
            }
        },
        POSTPLANNER_PUSH_LIMIT {
            @Override public @Nonnull Boolean parse(@Nullable Object value) throws InvalidValueException {
                return parseBool(value, true);
            }
        },
        POSTPLANNER_PIPE_CLEANER {
            @Override public @Nonnull Boolean parse(@Nullable Object value) throws InvalidValueException {
                return parseBool(value, true);
            }
        },
        POSTPLANNER_PUSH_DISJUNCTIVE {
            @Override public @Nonnull Boolean parse(@Nullable Object value) throws InvalidValueException {
                return parseBool(value, true);
            }
        },
        BIND_JOIN_RESULTS_FACTORY {
            @Override public @Nonnull String parse(@Nullable Object value) throws InvalidValueException {
                return parseClassName(value);
            }
        },
        HASH_JOIN_RESULTS_FACTORY {
            @Override public @Nonnull String parse(@Nullable Object value) throws InvalidValueException {
                return parseClassName(value);
            }
        },
        JOIN_OP_EXECUTOR {
            @Override public @Nonnull String parse(@Nullable Object value) throws InvalidValueException {
                return parseClassName(value);
            }
        },
        PLAN_EXECUTOR {
            @Override public @Nonnull String parse(@Nullable Object value) throws InvalidValueException {
                return parseClassName(value);
            }
        },
        QUERY_OP_EXECUTOR {
            @Override public @Nonnull String parse(@Nullable Object value) throws InvalidValueException {
                return parseClassName(value);
            }
        },
        DQUERY_OP_EXECUTOR {
            @Override public @Nonnull String parse(@Nullable Object value) throws InvalidValueException {
                return parseClassName(value);
            }
        },
        UNION_OP_EXECUTOR {
            @Override public @Nonnull String parse(@Nullable Object value) throws InvalidValueException {
                return parseClassName(value);
            }
        },
        SPARQLVALUESTEMPLATE_OP_EXECUTOR {
            @Override public @Nonnull String parse(@Nullable Object value) throws InvalidValueException {
                return parseClassName(value);
            }
        },
        CARTESIAN_OP_EXECUTOR {
            @Override public @Nonnull String parse(@Nullable Object value) throws InvalidValueException {
                return parseClassName(value);
            }
        },
        EMPTY_OP_EXECUTOR {
            @Override public @Nonnull String parse(@Nullable Object value) throws InvalidValueException {
                return parseClassName(value);
            }
        },
        PIPE_OP_EXECUTOR {
            @Override public @Nonnull String parse(@Nullable Object value) throws InvalidValueException {
                return parseClassName(value);
            }
        };

        public abstract @Nullable Object parse(@Nullable Object value) throws InvalidValueException;

        protected String parseClassName(@Nullable Object value) throws InvalidValueException {
            if (value == null) {
                Object fallback = getDefault();
                return fallback == null ? null : fallback.toString();
            }
            if (value instanceof Class) return ((Class<?>) value).getName();
            String name =  value.toString().trim();
            if (!CLASS_RX.matcher(name).matches()) {
                throw new InvalidValueException(this, value, ".toString() does not match an " +
                                                    "optionally unqualified class name");
            }
            return name;
        }

        protected @Nonnull Boolean parseBool(@Nullable Object value) throws InvalidValueException {
            return parseBool(value, false);
        }
        protected @Nonnull Boolean parseBool(@Nullable Object value,
                                             boolean fallback) throws InvalidValueException {
            if (value == null)
                return fallback;
            else if (value.toString().matches("(?i)\\s*(true|t|on|yes|y|1)\\s*"))
                return true;
            else if (value.toString().matches("(?i)\\s*(false|f|off|no|n|0)\\s*"))
                return false;
            throw new InvalidValueException(this, value, "not a boolean");
        }

        protected @Nonnull Integer parseInteger(@Nullable Object value) throws InvalidValueException {
            if (value == null)
                return (Integer) Objects.requireNonNull(getDefault());
            try {
                return Integer.parseInt(value.toString());
            } catch (NumberFormatException e) {
                throw new InvalidValueException(this, value, "Not a integer");
            }
        }

        protected @Nonnull Double parseDouble(@Nullable Object value) throws InvalidValueException {
            if (value == null)
                return (Double) Objects.requireNonNull(getDefault());
            try {
                return Double.parseDouble(value.toString());
            } catch (NumberFormatException e) {
                throw new InvalidValueException(this, value, "Not a double");
            }
        }

        @SuppressWarnings("unchecked")
        protected @Nullable <T> Set<T> parseSet(@Nullable Object value,
                                                @Nonnull Class<T> valueClass) throws InvalidValueException {
            if (value == null)
                return null;
            Set<T> set = new HashSet<>();
            if (value instanceof Iterable<?>) {
                for (Object o : (Iterable<?>) value) {
                    if (o == null)
                        throw new InvalidValueException(this, value, "nulls members not allowed");
                    if (valueClass.equals(String.class) && o instanceof Class)
                        o = ((Class<?>)o).getName();
                    if (!valueClass.isAssignableFrom(o.getClass())) {
                        throw new InvalidValueException(this, value, "Expected "+valueClass+
                                               ", got "+o.getClass());
                    }
                    set.add((T)o);
                }
            } else if (value instanceof String && valueClass.equals(String.class)) {
                List<String> pieces = new ArrayList<>(LIST_SPLITTER.splitToList(value.toString()));
                if (!pieces.isEmpty() && pieces.get(0).matches("[\\[{(]"))
                    pieces.remove(0);
                else if (!pieces.isEmpty())
                    pieces.set(0, pieces.get(0).replaceFirst("^[\\[{(]", ""));
                int last = pieces.size() - 1;
                if (!pieces.isEmpty() && pieces.get(last).matches("[})\\]]"))
                    pieces.remove(last);
                else if (!pieces.isEmpty())
                    pieces.set(last, pieces.get(last).replaceFirst("[})\\]]$", ""));
                for (String piece : pieces)
                    set.add((T)piece);
            } else if (valueClass.isInstance(value)) {
                set.add((T) value);
            } else if (valueClass.isAssignableFrom(String.class)) {
                set.add((T)value.toString());
            } else {
                throw new InvalidValueException(this, value, "value cannot be converted to "+valueClass);
            }
            return Collections.unmodifiableSet(set);
        }

        protected @Nullable File parseFile(@Nullable Object value) throws InvalidValueException {
            if (value == null) {
                return null;
            } else if (value instanceof Path) {
                return ((Path) value).toFile();
            } else if (value instanceof File) {
                return (File) value;
            } else if (value instanceof URL) {
                try {
                    value = ((URL) value).toURI();
                } catch (URISyntaxException e) {
                    throw new InvalidValueException(this, value, "URL is not an URI:"+e.getMessage());
                }
            } else if (URI_RX.matcher(value.toString()).find()) {
                try {
                    value = new URI(value.toString());
                } catch (URISyntaxException e) {
                    throw new InvalidValueException(this, value, "Badly formatted URI: "+e.getMessage());
                }
            }

            if (value instanceof URI) {
                URI uri = (URI) value;
                if (uri.getScheme().equalsIgnoreCase("file"))
                    value = uri.getPath();
                else
                    throw new InvalidValueException(this, value, "Only file: URIs are allowed");
            }
            return new File(value.toString());
        }

        protected @Nullable File parseDir(@Nullable Object value) throws InvalidValueException {
            File file = parseFile(value);
            if (file != null && file.exists() && file.isFile())
                throw new InvalidValueException(this, value, "value is a file, expected a dir");
            return file;
        }

        public @Nonnull Class<?> getValueClass() {
            switch (this) {
                case RESULTS_EXECUTOR_CONCURRENCY_FACTOR:
                case REL_CARDINALITY_ADDER_NONEMPTY_PROPORTION:
                case REL_CARDINALITY_ADDER_UNSUPPORTED_PROPORTION:
                    return Double.class;
                case ESTIMATE_LIMIT:
                case RESULTS_EXECUTOR_BUFFER_SIZE:
                case PLANNING_CORE_THREADS:
                case PLANNING_MAX_THREADS:
                case LARGE_CARDINALITY_THRESHOLD:
                case HUGE_CARDINALITY_THRESHOLD:
                case REL_CARDINALITY_ADDER_NONEMPTY_MIN:
                    return Integer.class;
                case ESTIMATE_QUERY_LOCAL:
                case ESTIMATE_QUERY_REMOTE:
                case ESTIMATE_ASK_LOCAL:
                case ESTIMATE_ASK_REMOTE:
                case REPLACEMENT_PRUNE_BY_DESCRIPTION:
                case PREPLANNER_FLATTEN:
                case PREPLANNER_CARTESIAN_INTRODUCTION:
                case PREPLANNER_UNION_DISTRIBUTION:
                case PREPLANNER_CARTESIAN_DISTRIBUTION:
                case PREPLANNER_PUSH_FILTERS:
                case POSTPLANNER_CONJUNCTION_REPLACE:
                case POSTPLANNER_FILTER2BIND:
                case POSTPLANNER_PUSH_DISTINCT:
                case POSTPLANNER_PUSH_LIMIT:
                case POSTPLANNER_PIPE_CLEANER:
                case POSTPLANNER_PUSH_DISJUNCTIVE:
                    return Boolean.class;
                case TBOX_HDT:
                case TBOX_RDF:
                case ENDPOINT_REASONER:
                case MATERIALIZER:
                case MATCHING:
                case PERFORMANCE_LISTENER:
                case AGGLUTINATOR:
                case CONJUNCTIVE_PLANNER:
                case JOIN_ORDER_PLANNER:
                case FILTER_JOIN_PLANNER:
                case CARDINALITY_ADDER:
                case INNER_CARDINALITY_COMPUTER:
                case JOIN_CARDINALITY_ESTIMATOR:
                case CARDINALITY_ENSEMBLE:
                case FAST_CARDINALITY_HEURISTIC:
                case CARDINALITY_COMPARATOR:
                case EQUIV_CLEANER:
                case PLANNING_EXECUTOR:
                case BIND_JOIN_RESULTS_FACTORY:
                case HASH_JOIN_RESULTS_FACTORY:
                case JOIN_OP_EXECUTOR:
                case PLAN_EXECUTOR:
                case QUERY_OP_EXECUTOR:
                case DQUERY_OP_EXECUTOR:
                case UNION_OP_EXECUTOR:
                case SPARQLVALUESTEMPLATE_OP_EXECUTOR:
                case CARTESIAN_OP_EXECUTOR:
                case EMPTY_OP_EXECUTOR:
                case PIPE_OP_EXECUTOR:
                    return String.class;
                case CARDINALITY_HEURISTICS:
                    return Set.class;
                case MATERIALIZER_STORAGE:
                case SOURCES_CACHE_DIR:
                case TEMP_DIR:
                    return File.class;
                case MATERIALIZER_SPEC:
                    return TBoxSpec.class;
            }
            throw new UnsupportedOperationException();
        }

        public @Nullable Object getDefault() {
            switch (this) {
                case RESULTS_EXECUTOR_CONCURRENCY_FACTOR:
                    return -1.0;
                case RESULTS_EXECUTOR_BUFFER_SIZE:
                    return 10;
                case ESTIMATE_LIMIT:
                    return 100;
                case LARGE_CARDINALITY_THRESHOLD:
                    return 256;
                case HUGE_CARDINALITY_THRESHOLD:
                    return 2048;
                case REL_CARDINALITY_ADDER_NONEMPTY_MIN:
                    return 1;
                case REL_CARDINALITY_ADDER_NONEMPTY_PROPORTION:
                    return 0.25;
                case REL_CARDINALITY_ADDER_UNSUPPORTED_PROPORTION:
                    return 0.75;
                case PLANNING_CORE_THREADS:
                    return getRuntime().availableProcessors();
                case PLANNING_MAX_THREADS:
                    return getRuntime().availableProcessors()
                            + (getRuntime().availableProcessors()/3 + 1);
                case ESTIMATE_ASK_REMOTE:
                case ESTIMATE_QUERY_REMOTE:
                    return false;
                case ESTIMATE_ASK_LOCAL:
                case ESTIMATE_QUERY_LOCAL:
                case REPLACEMENT_PRUNE_BY_DESCRIPTION:
                case PREPLANNER_FLATTEN:
                case PREPLANNER_CARTESIAN_INTRODUCTION:
                case PREPLANNER_UNION_DISTRIBUTION:
                case PREPLANNER_CARTESIAN_DISTRIBUTION:
                case PREPLANNER_PUSH_FILTERS:
                case POSTPLANNER_CONJUNCTION_REPLACE:
                case POSTPLANNER_FILTER2BIND:
                case POSTPLANNER_PUSH_DISTINCT:
                case POSTPLANNER_PUSH_LIMIT:
                case POSTPLANNER_PIPE_CLEANER:
                case POSTPLANNER_PUSH_DISJUNCTIVE:
                    return true;
                case TBOX_HDT:
                case TBOX_RDF:
                    return null;
                case ENDPOINT_REASONER:
                    return NoEndpointReasoner.class.getName();
                case MATERIALIZER:
                    return NoOpTBoxMaterializer.class.getName();
                case FILTER_JOIN_PLANNER:
                    return DefaultFilterJoinPlanner.class.getName();
                case PLANNING_EXECUTOR:
                    return PoolPlanningExecutorService.class.getName();
                case JOIN_ORDER_PLANNER:
                    return GreedyJoinOrderPlanner.class.getName();
                case EQUIV_CLEANER:
                    return NoEquivCleaner.class.getName();
                case CONJUNCTIVE_PLANNER:
                    return BitsetConjunctivePlannerDispatcher.class.getName();
                case AGGLUTINATOR:
                    return StandardAgglutinator.class.getName();
                case MATCHING:
                    return SourcesListMatchingStrategy.class.getName();
                case BIND_JOIN_RESULTS_FACTORY:
                    return SimpleBindJoinResults.Factory.class.getName();
                case HASH_JOIN_RESULTS_FACTORY:
                    return ParallelInMemoryHashJoinResults.Factory.class.getName();
                case JOIN_OP_EXECUTOR:
                    return DefaultJoinOpExecutor.class.getName();
                case PERFORMANCE_LISTENER:
                    return NoOpPerformanceListener.class.getName();
                case INNER_CARDINALITY_COMPUTER:
                    return DefaultInnerCardinalityComputer.class.getName();
                case JOIN_CARDINALITY_ESTIMATOR:
                    return BindJoinCardinalityEstimator.class.getName();
                case FAST_CARDINALITY_HEURISTIC:
                    return QuickSelectivityHeuristic.class.getName();
                case CARDINALITY_COMPARATOR:
                    return ThresholdCardinalityComparator.class.getName();
                case CARDINALITY_ADDER:
                    return RelativeCardinalityAdder.class.getName();
                case CARDINALITY_ENSEMBLE:
                    return WorstCaseCardinalityEnsemble.class.getName();
                case CARDINALITY_HEURISTICS:
                    return Collections.unmodifiableSet(new HashSet<>(asList(
                            QuickSelectivityHeuristic.class.getName(),
                            LimitCardinalityHeuristic.class.getName())
                    ));
                case PLAN_EXECUTOR:
                    return InjectedExecutor.class.getName();
                case QUERY_OP_EXECUTOR:
                    return SimpleQueryOpExecutor.class.getName();
                case DQUERY_OP_EXECUTOR:
                    return SimpleQueryOpExecutor.class.getName();
                case UNION_OP_EXECUTOR:
                    return SimpleQueryOpExecutor.class.getName();
                case SPARQLVALUESTEMPLATE_OP_EXECUTOR:
                    return SimpleQueryOpExecutor.class.getName();
                case CARTESIAN_OP_EXECUTOR:
                    return LazyCartesianOpExecutor.class.getName();
                case EMPTY_OP_EXECUTOR:
                    return SimpleEmptyOpExecutor.class.getName();
                case PIPE_OP_EXECUTOR:
                    return SimplePipeOpExecutor.class.getName();
                case MATERIALIZER_STORAGE:
                    return new File("materialized");
                case SOURCES_CACHE_DIR:
                    return new File("cache");
                case TEMP_DIR:
                    String path = System.getProperty("java.io.tmpdir");
                    assert path != null;
                    File file = new File(path);
                    assert file.isDirectory();
                    return file;
                case MATERIALIZER_SPEC:
                    return new TBoxSpec();
            }
            throw new UnsupportedOperationException();
        }

        private static String[] camelCaseNames;


        public @Nonnull String inCamelCase() {
            if (camelCaseNames == null) {
                String[] names = new String[values().length];
                int i = 0;
                for (Key key : values()) {
                    StringBuilder b = new StringBuilder();
                    for (String p : Splitter.on('_').omitEmptyStrings().splitToList(key.name())) {
                        if (b.length() == 0)
                            b.append(p.toLowerCase());
                        else
                            b.append(b.substring(0, 1)).append(p.substring(1).toLowerCase());
                    }
                    names[i++] = b.toString();
                }
                camelCaseNames = names;
            }
            return camelCaseNames[ordinal()];
        }
    }

    public @Nonnull FreqelConfig
    readFrom(@Nonnull Function<String, Object> getter,
             @Nullable File referenceDir) throws InvalidValueException {
        for (Key key : Key.values()) {
            List<String> alternatives = asList(key.name(), key.name().toLowerCase(),
                    key.name().toLowerCase().replace('_', '-'),
                    key.name().toLowerCase().replace('_', '.'),
                    key.name().replace('_', '-'), key.name().replace('_', '.'),
                    key.inCamelCase());
            for (String alternative : alternatives) {
                Object value = getter.apply(alternative);
                Object parsed = key.parse(value);
                if (value != null) {
                    if (key.getValueClass().equals(File.class)) {
                        File file = (File) parsed;
                        if (file != null && !file.isAbsolute() && referenceDir != null)
                            parsed = referenceDir.toPath().resolve(file.toPath()).toFile();
                    }
                    values.put(key, parsed);
                }
            }
        }
        return this;
    }
    public @Nonnull FreqelConfig readFrom(@Nonnull Map<?, ?> map,
                                          @Nullable File refDir) throws InvalidValueException {
        for (Key key : Key.values()) {
            if (map.containsKey(key))
                values.put(key, key.parse(map.get(key)));
        }
        return readFrom(map::get, refDir);
    }
    public @Nonnull FreqelConfig readFrom(@Nonnull DictTree dictTree,
                                          @Nullable File refDir) throws InvalidValueException {
        return readFrom(dictTree::get, refDir);
    }
    public @Nonnull FreqelConfig readFrom(@Nonnull String fileUriOrResource,
                                          @Nullable File referenceDir)
            throws IOException, InvalidValueException {
        boolean isUri = URI_RX.matcher(fileUriOrResource).find();
        referenceDir = referenceDir != null || isUri
                     ? referenceDir : new File(fileUriOrResource).getParentFile();
        if (fileUriOrResource.matches(".*\\.properties(\\?.*)?$")) {
            Properties p = new Properties();
            if (isUri) {
                try (InputStream is = new URI(fileUriOrResource).toURL().openStream()) {
                    p.load(new InputStreamReader(is, UTF_8));
                } catch (URISyntaxException e) {
                    throw new IOException(fileUriOrResource+" looks like an URI but is invalid");
                }
            } else {
                try (FileInputStream is = new FileInputStream(fileUriOrResource);
                     Reader reader = new InputStreamReader(is, UTF_8)) {
                    p.load(reader);
                }
            }
            return readFrom(p, referenceDir);
        }
        DictTree.Loader loader = DictTree.load(fileUriOrResource);
        DictTree dictTree;
        if (isUri) {
            dictTree = loader.fromUri(fileUriOrResource);
        } else {
            File file = new File(fileUriOrResource);
            if (file.exists() && file.isFile()) {
                dictTree = loader.fromFile(file);
            } else {
                InputStream is;
                try {
                    is = Utils.openResource(fileUriOrResource);
                } catch (IllegalArgumentException e) {
                    throw new IOException("Resource not found: "+fileUriOrResource);
                }
                dictTree = loader.fromInputStream(is);
            }
        }
        return readFrom(dictTree, referenceDir);
    }
    private void readFromDictTreeFile(@Nonnull String base) throws IOException, InvalidValueException {
        for (String suffix : asList(".yaml", ".json")) {
            String full = base + suffix;
            File file = new File(full);
            if (file.exists() && file.isFile()) {
                readFrom(DictTree.load(full).fromFile(file), file.getParentFile());
            }
        }
    }
    private void readFromPropertiesFile(@Nonnull File file) throws IOException, InvalidValueException {
        if (!file.exists() || !file.isFile())
            return;
        try (Reader reader = new InputStreamReader(new FileInputStream(file), UTF_8)) {
            Properties p = new Properties();
            p.load(reader);
            readFrom(p, file.getParentFile());
        }
    }
    public @Nonnull FreqelConfig
    readFrom(@Nonnull ConfigSource source) throws IOException, InvalidValueException {
        if (source == ConfigSource.JAVA_PROPERTIES) {
            return readFrom(System::getProperty, null);
        } else if (source == ConfigSource.ENVIRONMENT) {
            return readFrom(System::getenv, null);
        } else if (source == ConfigSource.RESOURCE) {
            try (InputStream is = Utils.openResource("freqel-config.properties");
                 Reader reader = new InputStreamReader(is, UTF_8)) {
                Properties p = new Properties();
                p.load(reader);
                readFrom(p, null);
            } catch (IllegalArgumentException ignored) { }
            try {
                readFrom(DictTree.load().fromResource("freqel-config.yaml"), null);
            } catch (FileNotFoundException ignored) { }
            try {
                readFrom(DictTree.load().fromResource("freqel-config.json"), null);
            } catch (FileNotFoundException ignored) { }
        } else if (source == ConfigSource.CWD_FILE) {
            readFromPropertiesFile(new File("freqel-config.properties"));
            readFromDictTreeFile("freqel-config");
        } else if (source == ConfigSource.HOME_FILE) {
            File home = new File(System.getProperty("user.home"));
            String base = new File(home, "freqel-config").getAbsolutePath();
            readFromPropertiesFile(new File(base+".properties"));
            readFromDictTreeFile(base);
        } else {
            assert false : "Unexpected ConfigSource";
        }
        return this;
    }

    public @Nonnull FreqelConfig readFrom(@Nonnull FreqelConfig other) {
        for (Map.Entry<Key, Object> e : other.values.entrySet())
            set(e.getKey(), e.getValue());
        return this;
    }

    @SuppressWarnings("unchecked")
    public @Nonnull FreqelConfig readFrom(@Nonnull Object o) throws IOException, InvalidValueException {
        if (o instanceof ConfigSource)
            return readFrom((ConfigSource)o);
        if (o instanceof Function)
            return readFrom((Function<String, Object>) o, null);
        else if (o instanceof Map)
            return readFrom((Map<?, ?>) o, null);
        else if (o instanceof DictTree)
            return readFrom((DictTree) o, null);
        else if (o instanceof File)
            return readFrom(((File) o).getAbsolutePath(), null);
        else if (o instanceof Path)
            return readFrom(((Path) o).toFile().getAbsolutePath(), null);
        else if (o instanceof URI || o instanceof URL || o instanceof CharSequence)
            return readFrom(o.toString(), null);
        else
            throw new IllegalArgumentException("Unexpected source type: "+o.getClass());
    }

    public FreqelConfig(@Nonnull Object... sources) throws IOException, InvalidValueException {
        this(sources.length == 0 ? asList(ConfigSource.fromLowestPrecedence()) : asList(sources));
    }
    public FreqelConfig(@Nonnull List<?> sources) throws IOException, InvalidValueException {
        for (Object source : sources)
            readFrom(source);
    }

    public static @Nonnull FreqelConfig createDefault() {
        try {
            return new FreqelConfig();
        } catch (IOException | InvalidValueException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Tests whether there is an explicit configuration for given key.
     * @param key the Key to test for
     * @return true iff a value was explicitly set.
     */
    public boolean has(@Nonnull Key key) {
        return values.containsKey(key);
    }

    public Object get(@Nonnull Key key) {
        return values.getOrDefault(key, key.getDefault());
    }
    public @Nonnull <T> T orElse(@Nonnull Key key, @Nonnull T fallback) {
        //noinspection unchecked
        return (T)values.getOrDefault(key, fallback);
    }
    public <T> T get(@Nonnull Key key, @Nonnull Class<T> cls) {
        assert cls.isAssignableFrom(key.getValueClass());
        //noinspection unchecked
        return (T)values.getOrDefault(key, key.getDefault());
    }

    public @Nonnull FreqelConfig reset(@Nonnull Key k) {
        values.remove(k);
        return this;
    }

    public @Nonnull FreqelConfig set(@Nonnull Key k, @Nullable Object val) {
        if (val != null) {
            if (!k.getValueClass().isAssignableFrom(val.getClass()) || val instanceof Iterable) {
                try {
                    val = k.parse(val);
                } catch (InvalidValueException e) {
                    throw new IllegalArgumentException(e);
                }
            }
        }
        values.put(k, val);
        return this;
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FreqelConfig)) return false;
        FreqelConfig that = (FreqelConfig) o;
        return Objects.equals(values, that.values);
    }

    @Override public int hashCode() {
        return Objects.hash(values);
    }

    @Override public @Nonnull String toString() {
        StringBuilder b = new StringBuilder();
        b.append("{\n");
        Object missing = new Object();
        for (Key key : Key.values()) {
            Object v = values.getOrDefault(key, missing);
            if (v != missing && !Objects.equals(v, key.getDefault())) {
                if (v instanceof String && CLASS_RX.matcher(v.toString()).matches()) {
                    v = v.toString().replaceAll("(\\w)[^.]+\\.", "$1.");
                    v = v.toString().replace("b.u.l.f.", "...");
                }
                b.append("  ").append(key.name()).append('=').append(v).append('\n');
            }
        }
        b.setLength(b.length()-1);
        return b.append('}').toString();
    }
}
