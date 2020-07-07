package br.ufsc.lapesd.riefederator.webapis;

import br.ufsc.lapesd.riefederator.description.CQueryMatch;
import br.ufsc.lapesd.riefederator.federation.Federation;
import br.ufsc.lapesd.riefederator.federation.Source;
import br.ufsc.lapesd.riefederator.federation.decomp.DecompositionStrategy;
import br.ufsc.lapesd.riefederator.federation.decomp.EvenDecomposer;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.SimpleExecutionModule;
import br.ufsc.lapesd.riefederator.federation.planner.Planner;
import br.ufsc.lapesd.riefederator.federation.planner.impl.JoinPathsPlanner;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.endpoint.AbstractTPEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.Capability;
import br.ufsc.lapesd.riefederator.query.modifiers.ModifierUtils;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.impl.*;
import br.ufsc.lapesd.riefederator.webapis.description.*;
import br.ufsc.lapesd.riefederator.webapis.requests.APIRequestExecutor;
import br.ufsc.lapesd.riefederator.webapis.requests.HTTPRequestObserver;
import br.ufsc.lapesd.riefederator.webapis.requests.MismatchingQueryException;
import br.ufsc.lapesd.riefederator.webapis.requests.impl.APIRequestExecutorException;
import com.google.inject.Guice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.ref.SoftReference;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

public class WebAPICQEndpoint extends AbstractTPEndpoint implements WebApiEndpoint {
    private static final @Nonnull Logger logger = LoggerFactory.getLogger(WebAPICQEndpoint.class);
    private final @Nonnull APIMolecule molecule;
    private @Nonnull SoftReference<APIMoleculeMatcher> matcher = new SoftReference<>(null);

    public WebAPICQEndpoint(@Nonnull APIMolecule molecule) {
        this.molecule = molecule;
    }

    public @Nonnull APIMolecule getMolecule() {
        return molecule;
    }

    public @Nonnull Source asSource() {
        return asSource(molecule.getName());
    }

    public @Nonnull Source asSource(@Nullable String name) {
        return new Source(getMatcher(), this,
                          name != null ? name : getMolecule().getExecutor().toString());
    }

    @Override
    public @Nonnull Cardinality estimate(@Nonnull CQuery query, int policy) {
        if (query.isEmpty()) return Cardinality.EMPTY;
        return molecule.getCardinality();
    }

    @Override
    public @Nonnull
    Results query(@Nonnull CQuery query) {
        MapSolution.Builder b = MapSolution.builder();
        APIRequestExecutor exec = molecule.getExecutor();
        boolean hasAtomAnnotations =
                query.forEachTermAnnotation(AtomAnnotation.class, (t, ann) -> {
                    if (!(ann instanceof AtomInputAnnotation))
                        return;
                    AtomInputAnnotation a = (AtomInputAnnotation)ann;
                    String input = a.getInputName();
                    if (a.isOverride()) {
                        b.put(input, Objects.requireNonNull(a.getOverrideValue()));
                    } else if (t.isGround()) {
                        b.put(input, t);
                    } else if (exec.getRequiredInputs().contains(input)) {
                        logger.error("Required input {} (Atom={}) is not ground!",
                                     input, a.getAtomName());
                    }
        });
        if (!hasAtomAnnotations) {
            logger.info("No AtomAnnotations in {}. Will call matchAndQuery()", query);
            return matchAndQuery(query, false);
        }
        MapSolution bound = b.build();

        Set<String> missing = IndexedParam.getMissing(exec.getRequiredInputs(), bound.getVarNames());
        Set<String> resultVars = query.getTermVars().stream().map(Var::getName).collect(toSet());
        if (!missing.isEmpty()) {
            logger.error("The required inputs {} are missing when invoking {}. " +
                         "Will return no results", missing, this);
            return CollectionResults.empty(resultVars);
        }
        if (bound.getVarNames().isEmpty()) {
            logger.warn("Calling WebAPI {} without arguments. This may be slow...",
                        molecule.getName());
        }
        // from here onwards, this class is responsible for modifiers
        ModifierUtils.check(this, query.getModifiers());
        Iterator<? extends CQEndpoint> it;
        try {
            it = exec.execute(bound);
        } catch (APIRequestExecutorException e) {
            logger.error("Exception on execution of query {} against {}. Will return empty results",
                         query, molecule.getName(), e);
            return CollectionResults.empty(resultVars);
        }
        Results results = new EndpointIteratorResults(it, query);
        results = SPARQLFilterResults.applyIf(results, query);
        results = ProjectingResults.applyIf(results, query);
        results = HashDistinctResults.applyIf(results, query);
        return results;
    }

    public @Nonnull Results matchAndQuery(@Nonnull CQuery query) {
        return matchAndQuery(query, true);
    }

    private @Nonnull Results matchAndQuery(@Nonnull CQuery query, boolean throwOnFailedMatch) {
        APIMoleculeMatcher matcher = getMatcher();
        Set<String> varNames = query.streamTerms(Var.class).map(Var::getName).collect(toSet());
        CQueryMatch match = matcher.match(query);
        if (match.getKnownExclusiveGroups().isEmpty()) {
            return reportFailure(query, throwOnFailedMatch, varNames);
        } else if (match.getKnownExclusiveGroups().size() == 1) {
            CQuery subQuery = match.getKnownExclusiveGroups().get(0);
            if (subQuery.size() != query.size())
                return reportFailure(query, throwOnFailedMatch, varNames);
            return query(subQuery.withModifiers(query)); // no loop, since it has AtomAnnotations
        } else {
            Set<Triple> allTriples = match.getKnownExclusiveGroups().stream()
                    .flatMap(CQuery::stream).collect(toSet());
            if (!allTriples.equals(query.getSet()))
                return reportFailure(query, throwOnFailedMatch, varNames);

            // use a federation over myself to plan and execute the joins
            Federation fed = Guice.createInjector(new SimpleExecutionModule() {
                @Override
                protected void configure() {
                    super.configure();
                    bind(Planner.class).to(JoinPathsPlanner.class);
                    bind(DecompositionStrategy.class).to(EvenDecomposer.class);
                }
            }).getInstance(Federation.class);
            fed.addSource(new Source(matcher, this));
            return fed.query(query);
        }
    }

    public @Nonnull APIMoleculeMatcher getMatcher() {
        APIMoleculeMatcher matcher = this.matcher.get();
        if (matcher == null)
            this.matcher = new SoftReference<>(matcher = new APIMoleculeMatcher(getMolecule()));
        return matcher;
    }

    public @Nullable HTTPRequestObserver setObserver(@Nonnull HTTPRequestObserver observer) {
        return this.molecule.getExecutor().setObserver(observer);
    }

    private Results reportFailure(@Nonnull CQuery query, boolean mustThrow,
                                  @Nonnull Set<String> varNames) {
        if (mustThrow)
            throw new MismatchingQueryException(query, this);
        else
            logger.info("Query mismatch for {} at {}: no results", query, this);
        return CollectionResults.empty(varNames);
    }

    @Override
    public boolean hasCapability(@Nonnull Capability capability) {
        switch (capability) {
            case PROJECTION:
            case DISTINCT:
            case SPARQL_FILTER:
                return true;
            default:
                return false;
        }
    }

    @Override
    public boolean hasRemoteCapability(@Nonnull Capability capability) {
        return false;
    }

    @Override
    public @Nonnull String toString() {
        return String.format("WebAPICQEndpoint(%s, %s, %s)", getMolecule().getMolecule(),
                getMolecule().getExecutor(), getMolecule().getElement2Input());
    }
}
