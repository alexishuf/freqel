FreqelConfig reference
======================

The `FreqelConfig` class is a container for assorted configurations that have 
effect during instantiation of a `Federation`. These configurations which 
implementations are used for interfaces (indirectly) held by a Federation 
instance and how such instances of said implementations are configured. 

All configurations are listed in the `FreqelConfig.Key` enum and can be 
referred to in 7 different formats:

1. UPPER_CASE (preferred for environment variables)
2. underscore_notation
3. kebab-case
4. Capital-Kebab-Case
5. camelCase (preferred for json)
6. CapitalCamelCase
7. dot.notation (preferred for `java -D` and `.properties` files)

By default, configuration is loaded from these locations in this order 
(later loads override previously loaded configurations)

1.  `$HOME/freqel-config.properties`
2.  `$HOME/freqel-config.yaml`
3.  `$HOME/freqel-config.json`
4.  `$(pwd)/freqel-config.properties`
5.  `$(pwd)/freqel-config.yaml`
6.  `$(pwd)/freqel-config.json`
7.  `freqel-config.properties` resource in the classpath
8.  `freqel-config.yaml` resource in the classpath
19. `freqel-config.json` resource in the classpath
10. Environment variables
11. Java properties
12. federation spec file given to `freqel-server-1.0-SNAPSHOT.jar --config`

Many of the configurations take a Fully-Qualified Class Names (FQCN) as value. 
For classes built into the `freqel-core` module or exposed via the Service 
Provider Interface (SPI) feature of Java, the FQCN can be abbreviated to the 
final substring. For example, `SystemVLogMaterializer` can be used in place of
`br.ufsc.lapesd.freqel.reason.tbox.vlog.SystemVLogMaterializer`. Abbreviating 
package names is not supported: `b.u.l.f.r.t.v.SystemVLogMaterializer` will 
cause a failure to instantiate the class.

Configuration reference
-----------------------

### Reasoning

**ENDPOINT_REASONER**: FQCN of an `EndpointReasoner` implementation. Default 
value is `NoEndpointReasoner` (no reasoning will be performed). A Common 
alternative is `HeuristicEndpointReasoner` (use cardinality heuristics to 
prevent combinatorial explosions).

**REPLACEMENT_PRUNE_BY_DESCRIPTION**: Boolean, default value is true. If 
enabled, an `EndpointReasoner` that relies on alternative set generation will 
prune the alternative sets using `Description.localMatch` to avoid sending 
queries which the source definitely will have no answers for. 

**TBOX_HDT**: String representing a file path. Default is `null`. If provided 
shall point to a readable HDT file that contains the TBox materialization.

**TBOX_RDF**: String representing a file path. Default is `null`. If provided
shall point to a readable RDF file (any syntax supported by 
[rdfit](https://github.com/lapesd/rdfit/)) file that contains the TBox 
materialization. Note that unlike **MATERIALIZER_SPEC**, no materialization 
will be computed.

**MATERIALIZER**: FQCN of a TBoxMaterializer implementation. Default is 
`NoOpTBoxMaterializer` (no reasoning will be performed).  

**MATERIALIZER_SPEC**: list of Strings pointing to files and URLs that will 
be loaded into the provided **MATERIALIZER** to create a TBox materialization.
Default is an empty set.

**MATERIALIZER_STORAGE**: where the **MATERIALIZER** should store intermediary 
files (which will be deleted ASAP) and the TBox materialization. Avoid using 
a `/tmp` directory, as these are usually memory-mapped file systems and 
TBoxes and their materializations may easily fill such filesystems. Default is 
the relative directory `materialized`

**TEMP_DIR**: where to store large temporary files (since `/tmp` is usually 
limitted by system memory). Default is the sytem temporary directory 
(`java.io.tmpdir` property).

### Sources description

**SOURCES_CACHE_DIR**: Where to store source index cached data. Usually 
predicates and classes list fetched by sources using `SelectDescription`. 
Default value is the relative directory `cache`.

### Cardinality handling

**ESTIMATE_LIMIT**: A Integer value for the `LIMIT` modifier in SPARQL 
`SELECT` queries sent by `LimitCardinalityHeuristic`. Default is 100

**ESTIMATE_ASK_LOCAL**: A Boolean that enables `LimitCardinalityHeuristic` 
to send `ASK` queries for sources local to the federation mediator. 
Default is true.

**ESTIMATE_QUERY_LOCAL**: A Boolean that enables `LimitCardinalityHeuristic`
to send `SELECT` queries for sources local to the federation mediator.
Default is true.

**ESTIMATE_ASK_REMOTE**: A Boolean that enables `LimitCardinalityHeuristic`
to send `ASK` queries for sources **not** local to the federation mediator.
Default is false.

**ESTIMATE_QUERY_REMOTE**: A Boolean that enables `LimitCardinalityHeuristic`
to send `SELECT` queries for sources **not** local to the federation mediator.
Default is false.

**CARDINALITY_ADDER**: FQCN of a `CardinalityAdder` implementation. Such class 
adds `Cardinality` instances with possibly distinct `Cardinality.Reliability`.
Default is `RelativeCardinalityAdder`, which uses the counterpart cardinality 
in an addition to assign a value to `NON_EMPTY` and `UNSUPPORTED` cardinalities.

**REL_CARDINALITY_ADDER_NONEMPTY_MIN**: Integer for the `nonEmptyMin` argument 
in `RelativeCardinalityAdder` constructor. When adding a `NON_EMPTY` 
cardinality to one above `GUESS`, the NON_EMPTY will receive a value no smaller 
than this. Default is 1.

**REL_CARDINALITY_ADDER_NONEMPTY_PROPORTION**: Double for the
`nonEmptyProportion` argument in `RelativeCardinalityAdder` constructor.
If adding a `NON_EMPTY` cardinality to a `GUESS` or above cardinality, the
`NON_EMPTY` is considered to have a value that is this proportion of its
valued counterpart in the addition. Default is 0.25 

**REL_CARDINALITY_ADDER_UNSUPPORTED_PROPORTION**: Double for the 
`unsupportedProportion` argument in `RelativeCardinalityAdder` constructor. 
If adding a `UNSUPPORTED` cardinality to a `GUESS` or above cardinality, the 
`UNSUPPORTED` is considered to have a value tha is this proportion of its 
valued counterpart in the addition. Default is 0.75

**INNER_CARDINALITY_COMPUTER**: FQCN of an `InnerCardinalityComputer` 
implementation. Default is `DefaultInnerCardinalityComputer`.

**JOIN_CARDINALITY_ESTIMATOR**: FQCN of an `JoinCardinalityEstimator` 
implementation. An instance of this class will be used by 
`GreedyJoinOrderPlanner` to plan the least cardinality joins to execute first.
Default is `BindJoinCardinalityEstimator`, which applies the configured 
`CardinalityEnsemble` to a virtual query representing the join. Alternative 
is `AverageJoinCardinalityEstimator`

**CARDINALITY_ENSEMBLE**: FCQN of a `CardinalityEnsemble` implementation. 
Default is `WorstCaseCardinalityEnsemble`, which takes the worst estimate 
among heuristics in **CARDINALITY_HEURISTICS**, with `EXACT` estimates 
overuling non-`EXACT` estimates even if smaller.  Alternatives are 
`FixedCardinalityEnsemble` which  will only use **FAST_CARDINALITY_HEURISTIC** 
or `NoCardinalityEnsemble` which will perform no estimate.

**CARDINALITY_HEURISTICS**: Collection of FQCN of `CardinalityHeuristic` 
implementations. Default is `QuickSelectivityHeuristic` and 
`LimitCardinalityHeuristic`.

**FAST_CARDINALITY_HEURISTIC**: A single FQCN of a `CardinalityHeuristic` 
fast implementation. Default is `QuickSelectivityHeuristic`.

**CARDINALITY_COMPARATOR**: FQCN of an `CardinalityComparator` implementation. 
Default is `ThresholdCardinalityComparator`, which employs two thresholds
(*large* and *huge*) as follows:

- `UNSUPPORTED` and `NON_EMPTY` cardinalities are assigned the *huge* value
- `GUESS` and `LOWER_BOUND` reliabilities are considered equal
- `UPPER_BOUND` and `EXACT` reliabilities are considered equal
- When comparing a `GUESS` or `LOWER_BOUND` cardinality with another with 
  `UPPER_BOUND` or `EXACT` reliability, the lower reliability value is 
  increased by the *large* threshold.

**LARGE_CARDINALITY_THRESHOLD**: Integer value for the *large* threshold used 
by `ThresholdCardinalityComparator`. Default is 256.

**HUGE_CARDINALITY_THRESHOLD**: Integer value for the *huge* threshold used
by `ThresholdCardinalityComparator`. Default is 2048.

### Query Planning

**MATCHING**: FQCN of an `MatchingStrategy` implementation. Default is 
`SourcesListMatchingStrategy`. A possible alternative is 
`ParallelSourcesListMatchingStrategy`

**PERFORMANCE_LISTENER**: FQCN of an `PerformanceListener` implementation. 
Default is `NoOpPerformanceListener` (do not collect performance data). 
A common alternative is `ThreadedPerformanceListener` thread-safe lock-based 
in-memory storage. 

**AGGLUTINATOR**. FQCN of an `Agglutinator` implementation. Default is 
`StandardAgglutinator`. Alternatives are `ParallelStandardAgglutinator` 
and `EvenAgglutinator`.

**CONJUNCTIVE_PLANNER**: FQCN of an `ConjunctivePlanner` implementation. 
Default is `BitsetConjunctivePlannerDispatcher`. Alternatives are:
- `BitsetConjunctivePlanner` (has a small perforamnce waste if no sources take
inputs)
- `BitsetNoInputsConjunctivePlanner` (no support of sources that require inputs)
- `JoinPathsConjunctivePlanner` (very slow)

**JOIN_ORDER_PLANNER**: FQCN of a JoinOrderPLanner implementation. Default 
is `GreedyJoinOrderPlanner`. The alternative `ArbitraryJoinOrderPlanner` is 
can yield plans with terrible execution times.

**EQUIV_CLEANER**: FQCN of an `EquivCleaner` implementation. Default is 
`NoEquivCleaner`, which is a no-op. `Bitset*` conjunctive planners forgo 
this step. With `JoinPathsPlanner`, `DefaultEquivCleaner` may have a positive 
effect.

**FILTER_JOIN_PLANNER**: FQCN of a `FilterJoinPlanner` implementation, used 
in `FilterToBindJoinStep` optimizations use existing `FILTER`s to replace 
cartesian products with bind joins. Default value is `DefaultFilterJoinPlanner`.

**PLANNING_EXECUTOR**: FQCN of a `PlanningExecutorService`. 
Default value is `PoolPlanningExecutorService`. Such executors are used to 
parallelize some steps of planning, notably the `Parallel` implementations for 
`Agglutinator` and `MatchingStrategy`. 

**PLANNING_CORE_THREADS**: Integer with the number of long-lived threads in 
the `PoolPlanningExecutorService` instance. Default is 
`getRuntime().availableProcessors()`

**PLANNING_MAX_THREADS**: Maximum number of active threads in the 
`PoolPlanningExecutorService` instance. Default value is 133% (rounded up) of 
the number  of cores detected by the JVM

#### Pre-planner steps

**PREPLANNER_FLATTEN**: Boolean indicating whether to include `FlattenStep` as 
a step in the `PrePlanner`. Default is true.

**PREPLANNER_CARTESIAN_INTRODUCTION**: Boolean indicating whether to include 
`CartesianIntroductionStep` as a step in the `PrePlanner`. Default is true.

**PREPLANNER_UNION_DISTRIBUTION**: Boolean indicating whether to include 
`UnionDistributionStep` as a step in the `PrePlanner`. Default is true.

**PREPLANNER_CARTESIAN_DISTRIBUTION**: Boolean indicating whether to include 
`CartesianDistributionStep` as  a step in the `PrePlanner`. Default is true.

**PREPLANNER_PUSH_FILTERS**: Boolean indicating whether to include 
`PushFiltersStep` as a step in the `PrePlanner`. Default is true.


#### Post-planner steps

**POSTPLANNER_CONJUNCTION_REPLACE**: Boolean indicating whether to include 
`ConjunctionReplaceStep` as a step in the `PostPlanner`. Default is true.

**POSTPLANNER_FILTER2BIND**: Boolean indicating whether to include 
`FilterToBindJoinStep` as a step in the `PostPlanner`. Default is true.

**POSTPLANNER_PUSH_DISTINCT**: Boolean indicating whether to include 
`PushDistinctStep` as a step in the `PostPlanner`. Default is true.

**POSTPLANNER_PUSH_LIMIT**: Boolean indicating whether to include 
`PushLimitStep` as a step in the `PostPlanner`. Default is true.

**POSTPLANNER_PIPE_CLEANER**: Boolean indicating whether to include
`PipeCleanerStep` as a step in the `PostPlanner`. Default is true.

**POSTPLANNER_PUSH_DISJUNCTIVE**: Boolean indicating whether to include
`PushDisjunctiveStep` as a step in the `PostPlanner`. Default is true.

### Execution

**RESULTS_EXECUTOR_CONCURRENCY_FACTOR**: Concurrency factor of the `ResultsExecutor` 
instance held by the `Federation` as a multiplier of the number of cores in 
the machine. The special value of -1 means unbounded concurrency. Any other 
value less than 1 will mean "no concurrency". Default is -1.

**RESULTS_EXECUTOR_BUFFER_SIZE**: For a `BufferedResultsExecutor`, this is 
the maximum number of `Solution`s held in a buffer for every active `Results` 
instance being managed by the executor. Default is 10.

#### Join algorithms

**BIND_JOIN_RESULTS_FACTORY**: FQCN of a `BindJoinResultsFactory` 
implementation. Default is `SimpleBindJoinResults.Factory`.

**HASH_JOIN_RESULTS_FACTORY**: FQCN of a `HashJoinResultsFactory` 
implementation. Default is `ParallelInMemoryHashJoinResults.Factory`, which 
consumes both operands in parallel (this requires more memory but is faster 
to start producing solutions and to finish). The built-in alternative is 
`InMemoryHashJoinResults.Factory` which eagerly consumes the suposedly 
smaller operand first.

**JOIN_OP_EXECUTOR**: FQCN of a `JoinOpExecutor`. Default is 
`DefaultJoinOpExecutor`. Built-in possibilities are:

- `DefaultJoinOpExecutor`: If one or both operands have cardinality below 1024 
  with at least `UPPER_BOUND` reliability and none requires inputs, delegate 
  to `DefaultHashJoinOpExecutor`. Else, delegate to 
  **BIND_JOIN_RESULTS_FACTORY**  
- `FixedBindJoinOpExecutor`: delegates to **BIND_JOIN_RESULTS_FACTORY** 
- `DefaultHashJoinOpExecutor`:  If at least one of the operands has a 
  cardinality value below 1024, use an eager hash-join 
  (`InMemoryHashJoinResults.Factory`), else use one that consumes both
  operands in parallel (`ParallelInMemoryHashJoinResults`).
- `FixedHashJoinOpExecutor`: delegates to **HASH_JOIN_RESULTS_FACTORY**


#### Op Executors

**PLAN_EXECUTOR**: FQCN of a `PlanExecutor` implementation. Default is 
`InjectedExecutor`.

**QUERY_OP_EXECUTOR**: FQCN of a `QueryOpExecutor` implementation. 
Default is `SimpleQueryOpExecutor`

**DQUERY_OP_EXECUTOR**: FQCN of a `DQueryOpExecutor` implementation. 
Default is `SimpleQueryOpExecutor`

**UNION_OP_EXECUTOR**: FQCN of a `UnionOpExecutor` implementation. 
Default is `SimpleQueryOpExecutor`

**SPARQLVALUESTEMPLATE_OP_EXECUTOR**: FQCN of a `SPARQLValuesTemplateOpExecutor` 
implementation. Default is `SimpleQueryOpExecutor`

**CARTESIAN_OP_EXECUTOR**: FQCN of a `CartesianOpExecutor` implementation. 
Default is `LazyCartesianOpExecutor`

**EMPTY_OP_EXECUTOR**: FQCN of a `EmptyOpExecutor` implementation. Default is 
`SimpleEmptyOpExecutor`

**PIPE_OP_EXECUTOR**: FQCN of a `PipeOpExecutor` implementation. Default is 
`SimplePipeOpExecutor`