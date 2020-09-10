package br.ufsc.lapesd.riefederator.benchmark.impl;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.benchmark.BenchmarkUtils;
import br.ufsc.lapesd.riefederator.federation.Federation;
import br.ufsc.lapesd.riefederator.federation.PerformanceListener;
import br.ufsc.lapesd.riefederator.federation.performance.ThreadedPerformanceListener;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.Metrics;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.TimeSampler;
import br.ufsc.lapesd.riefederator.federation.spec.FederationSpecException;
import br.ufsc.lapesd.riefederator.federation.spec.FederationSpecLoader;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.CollectionResults;
import br.ufsc.lapesd.riefederator.server.sparql.impl.CSVResultsFormatter;
import com.google.common.base.Stopwatch;
import com.google.inject.AbstractModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static br.ufsc.lapesd.riefederator.server.sparql.impl.CSVResultsFormatter.CSV_TYPE;
import static br.ufsc.lapesd.riefederator.server.sparql.impl.CSVResultsFormatter.TSV_TYPE;

public class QueryExperiment {
    private static final Logger logger = LoggerFactory.getLogger(QueryExperiment.class);

    public static @Nonnull List<String> HEADERS = Arrays.asList("queryName", "timestamp",
            "preheatRuns", "runs", "run", "onlyPlan", "nSources", "nResults", "cooldownMs",
            "initSourcesMs", "selMs", "planMs", "outerPlanMs", "innerPlanMs", "optMs",
            "execMs", "firstResultMs", "queryMs", "resultsBasename");

    private final @Nonnull String name;
    private final @Nonnull Op query;
    private final @Nonnull File federationConfig;
    private final int preheat;
    private final int runs;
    private final boolean onlyPlan;
    private final @Nullable File queryResultsDir;

    public QueryExperiment(@Nonnull String name, @Nonnull Op query,
                           @Nonnull File federationConfig, int preheat, int runs,
                           boolean onlyPlan, @Nullable File queryResultsDir) {
        this.name = name;
        this.query = query;
        this.federationConfig = federationConfig;
        this.preheat = preheat;
        this.runs = runs;
        this.onlyPlan = onlyPlan;
        this.queryResultsDir = queryResultsDir;
    }

    public @Nonnull String getName() {
        return name;
    }

    public int getPreheat() {
        return preheat;
    }

    public @Nonnull File getFederationConfig() {
        return federationConfig;
    }

    public @Nullable File getQueryResultsDir() {
        return queryResultsDir;
    }

    public int getRuns() {
        return runs;
    }

    public boolean isOnlyPlan() {
        return onlyPlan;
    }

    public static class Result {
        private @Nonnull String name;
        private @Nonnull LocalDateTime timestamp;
        private int preheat, runs, run;
        private boolean onlyPlan;
        private int sourcesCount, resultsCount;
        private double cooldownMs, initSourcesMs, selectionMs, planMs;
        private double outerPlanMs, innerPlanMs, optMs, execMs, firstResultMs, queryMs;
        private transient  @Nullable CollectionResults results;
        private @Nullable String resultsBasename;

        public Result(@Nonnull String name, @Nonnull LocalDateTime timestamp,
                      int preheat, int runs, int run, boolean onlyPlan) {
            this(name, timestamp, preheat, runs, run, onlyPlan, Integer.MIN_VALUE,
                 Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE,
                 Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE,
                 Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE,
                 null,null);
        }

        public Result(@Nonnull String name, @Nonnull LocalDateTime timestamp, int preheat,
                      int runs, int run, boolean onlyPlan, int sourcesCount, int resultsCount,
                      double cooldownMs, double initSourcesMs, double selectionMs, double planMs,
                      double outerPlanMs, double innerPlanMs, double optMs,
                      double execMs, double firstResultMs, double queryMs, @Nullable CollectionResults results,
                      @Nullable String resultsBasename) {
            this.name = name;
            this.timestamp = timestamp;
            this.preheat = preheat;
            this.runs = runs;
            this.run = run;
            this.onlyPlan = onlyPlan;
            this.sourcesCount = sourcesCount;
            this.resultsCount = resultsCount;
            this.cooldownMs = cooldownMs;
            this.initSourcesMs = initSourcesMs;
            this.selectionMs = selectionMs;
            this.planMs = planMs;
            this.outerPlanMs = outerPlanMs;
            this.innerPlanMs = innerPlanMs;
            this.optMs = optMs;
            this.execMs = execMs;
            this.firstResultMs = firstResultMs;
            this.queryMs = queryMs;
            this.results = results;
            this.resultsBasename = resultsBasename;
        }

        public @Nonnull String getName() {
            return name;
        }

        public @Nonnull LocalDateTime getTimestamp() {
            return timestamp;
        }
        public @Nonnull String getISOTimestamp() {
            return getTimestamp().toString();
        }
        public int getPreheat() {
            return preheat;
        }
        public int getRuns() {
            return runs;
        }
        public int getRun() {
            return run;
        }
        public boolean isOnlyPlan() { return onlyPlan; }
        public int getSourcesCount() {
            return sourcesCount;
        }
        public int getResultsCount() {
            return resultsCount;
        }
        public double getCooldownMs() {
            return cooldownMs;
        }
        public double getInitSourcesMs() {
            return initSourcesMs;
        }
        public double getSelectionMs() {
            return selectionMs;
        }
        public double getPlanMs() {
            return planMs;
        }
        public double getOuterPlanMs() {
            return outerPlanMs;
        }
        public double getInnerPlanMs() {
            return innerPlanMs;
        }
        public double getOptMs() {
            return optMs;
        }
        public double getExecMs() {
            return execMs;
        }
        public double getFirstResultMs() {
            return firstResultMs;
        }
        public double getQueryMs() {
            return queryMs;
        }
        public @Nullable CollectionResults getResults() {
            return results;
        }
        public @Nullable String getResultsBasename() {
            return resultsBasename;
        }

        public void setName(@Nonnull String name) {
            this.name = name;
        }
        public void setTimestamp(@Nonnull LocalDateTime timestamp) {
            this.timestamp = timestamp;
        }
        public void setPreheat(int preheat) {
            this.preheat = preheat;
        }
        public void setRuns(int runs) {
            this.runs = runs;
        }
        public void setRun(int run) {
            this.run = run;
        }
        public void setOnlyPlan(boolean value) {
            this.onlyPlan = value;
        }
        public void setSourcesCount(int sourcesCount) {
            this.sourcesCount = sourcesCount;
        }
        public void setResultsCount(int resultsCount) {
            this.resultsCount = resultsCount;
        }
        public void setCooldownMs(double cooldownMs) {
            this.cooldownMs = cooldownMs;
        }
        public void setInitSourcesMs(double initSourcesMs) {
            this.initSourcesMs = initSourcesMs;
        }
        public void setSelectionMs(double selectionMs) {
            this.selectionMs = selectionMs;
        }
        public void setPlanMs(double planMs) {
            this.planMs = planMs;
        }
        public void setOuterPlanMs(double outerPlanMs) {
            this.outerPlanMs = outerPlanMs;
        }
        public void setInnerPlanMs(double innerPlanMs) {
            this.innerPlanMs = innerPlanMs;
        }
        public void setOptMs(double optMs) {
            this.optMs = optMs;
        }
        public void setExecMs(double execMs) {
            this.execMs = execMs;
        }
        public void setFirstResultMs(double firstResultMs) {
            this.firstResultMs = firstResultMs;
        }
        public void setQueryMs(double queryMs) {
            this.queryMs = queryMs;
        }
        public void setResults(@Nonnull CollectionResults results) {
            this.results = results;
        }
        public void setResultsBasename(@Nullable String resultsBasename) {
            this.resultsBasename = resultsBasename;
        }

        public @Nonnull List<Object> toValueList() {
            double dNA = Integer.MIN_VALUE;
            int iNA = Integer.MIN_VALUE;
            return Arrays.asList(getName(), getISOTimestamp(),
                    getPreheat(), getRuns(), getRun(), isOnlyPlan(),
                    getSourcesCount()  == iNA ? null : getSourcesCount(),
                    getResultsCount()  == iNA ? null : getResultsCount(),
                    getCooldownMs()    == dNA ? null : getCooldownMs(),
                    getInitSourcesMs() == dNA ? null : getInitSourcesMs(),
                    getSelectionMs()   == dNA ? null : getSelectionMs(),
                    getPlanMs()        == dNA ? null : getPlanMs(),
                    getOuterPlanMs()   == dNA ? null : getOuterPlanMs(),
                    getInnerPlanMs()   == dNA ? null : getInnerPlanMs(),
                    getOptMs()         == dNA ? null : getOptMs(),
                    getExecMs()        == dNA ? null : getExecMs(),
                    getFirstResultMs() == dNA ? null : getFirstResultMs(),
                    getQueryMs()       == dNA ? null : getQueryMs(),
                    getResultsBasename());
        }
    }

    public @Nonnull Result execute(int run) throws IOException, FederationSpecException {
        LocalDateTime timestamp = LocalDateTime.now(ZoneId.systemDefault());
        PerformanceListener perf = new ThreadedPerformanceListener();
        FederationSpecLoader loader = new FederationSpecLoader();
        loader.overrideWith(
                new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(PerformanceListener.class).toInstance(perf);
                    }
                });
        CollectionResults collResults = null;
        Stopwatch sw;
        try (TimeSampler ignored = Metrics.COOLDOWN_MS.createThreadSampler(perf)) {
            BenchmarkUtils.preheatCooldown();
        }
        try (Federation federation = loader.load(federationConfig)) {
            federation.initAllSources(5, TimeUnit.MINUTES);
            sw = Stopwatch.createStarted();
            Op plan = federation.plan(query);
            if (!isOnlyPlan()) {
                List<Solution> resultsList = new ArrayList<>(10000);
                try (TimeSampler ignored = Metrics.COOLDOWN_MS.createThreadSampler(perf)) {
                    BenchmarkUtils.preheatCooldown();
                }
                try (TimeSampler ignored = new TimeSampler(perf, Metrics.EXEC_MS)) {
                    Stopwatch firstResultSw = Stopwatch.createStarted();
                    try (Results results = federation.execute(plan)) {
                        while (results.hasNext()) {
                            resultsList.add(results.next());
                            if (firstResultSw.isRunning()) {
                                firstResultSw.stop();
                                perf.sample(Metrics.FIRST_RESULT_EXEC_MS,
                                     firstResultSw.elapsed(TimeUnit.MICROSECONDS)/1000.0);
                            }
                        }
                        results.forEachRemaining(resultsList::add);
                    }
                }
                collResults = new CollectionResults(resultsList, plan.getResultVars());
            }
        }
        double queryMs = sw.elapsed(TimeUnit.MICROSECONDS) / 1000.0;
        logger.info("Results for {}: planMs={}, execMs={}, queryMs={}",
                    getName(), perf.getValue(Metrics.FULL_PLAN_MS),
                    perf.getValue(Metrics.EXEC_MS), queryMs);
        perf.sync();
        String resultsBasename = collResults == null ? "" : saveResults(collResults);
        return new Result(getName(), timestamp, getPreheat(), getRuns(), run, isOnlyPlan(),
                perf.getValue(Metrics.SOURCES_COUNT, Integer.MIN_VALUE),
                collResults == null ? Integer.MIN_VALUE : collResults.getCollection().size(),
                perf.getValue(Metrics.COOLDOWN_MS, (double)Integer.MIN_VALUE),
                perf.getValue(Metrics.INIT_SOURCES_MS, (double)Integer.MIN_VALUE),
                perf.getValue(Metrics.SELECTION_MS, (double)Integer.MIN_VALUE),
                perf.getValue(Metrics.FULL_PLAN_MS, (double)Integer.MIN_VALUE),
                perf.getValue(Metrics.PRE_PLAN_MS, (double)Integer.MIN_VALUE),
                perf.getValue(Metrics.PLAN_MS, (double)Integer.MIN_VALUE),
                perf.getValue(Metrics.OPT_MS, (double)Integer.MIN_VALUE),
                perf.getValue(Metrics.EXEC_MS, (double)Integer.MIN_VALUE),
                perf.getValue(Metrics.FIRST_RESULT_EXEC_MS, (double)Integer.MIN_VALUE),
                queryMs,
                collResults,
                resultsBasename);
    }

    private @Nullable String saveResults(CollectionResults results) throws IOException {
        File resultsDir = getQueryResultsDir();
        if (resultsDir == null)
            return null;
        if (!resultsDir.exists() && resultsDir.mkdirs()) {
            logger.error("Failed to mkdir {}. Will not save results", resultsDir.getAbsolutePath());
            return null;
        }
        Path dirPath = resultsDir.toPath();
        File csv = null, tsv = null;
        while (tsv == null || tsv.exists()) {
            if (csv != null && !csv.delete())
                logger.warn("Failed to delete file {}", csv);
            csv = Files.createTempFile(dirPath, getName()+"-", ".csv").toFile();
            tsv = new File(resultsDir, csv.getName().replaceAll("\\.csv$", ".tsv"));
        }
        CSVResultsFormatter formatter = new CSVResultsFormatter();
        try (FileOutputStream stream = new FileOutputStream(csv)) {
            stream.write(formatter.format(results, false, CSV_TYPE).getBytes());
        }
        try (FileOutputStream stream = new FileOutputStream(tsv)) {
            stream.write(formatter.format(results, false, TSV_TYPE).getBytes());
        }
        return csv.getName().replaceAll("\\.csv$", "");
    }
}
