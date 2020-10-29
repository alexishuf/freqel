package br.ufsc.lapesd.riefederator.webapis;

import br.ufsc.lapesd.riefederator.algebra.Cardinality;
import br.ufsc.lapesd.riefederator.description.CQueryMatch;
import br.ufsc.lapesd.riefederator.federation.Federation;
import br.ufsc.lapesd.riefederator.federation.SingletonSourceFederation;
import br.ufsc.lapesd.riefederator.federation.Source;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.AbstractTPEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.Capability;
import br.ufsc.lapesd.riefederator.query.modifiers.ModifierUtils;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.impl.CollectionResults;
import br.ufsc.lapesd.riefederator.query.results.impl.EndpointIteratorResults;
import br.ufsc.lapesd.riefederator.query.results.impl.MapSolution;
import br.ufsc.lapesd.riefederator.webapis.description.*;
import br.ufsc.lapesd.riefederator.webapis.requests.APIRequestExecutor;
import br.ufsc.lapesd.riefederator.webapis.requests.HTTPRequestObserver;
import br.ufsc.lapesd.riefederator.webapis.requests.MismatchingQueryException;
import br.ufsc.lapesd.riefederator.webapis.requests.impl.APIRequestExecutorException;
import br.ufsc.lapesd.riefederator.webapis.requests.impl.QueryGlobalContextCache;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.ref.SoftReference;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

public class WebAPICQEndpoint extends AbstractTPEndpoint implements WebApiEndpoint {
    private static final @Nonnull Logger logger = LoggerFactory.getLogger(WebAPICQEndpoint.class);
    private final @Nonnull APIMolecule molecule;
    private @Nonnull SoftReference<APIMoleculeMatcher> matcher = new SoftReference<>(null);
    private @Nullable Federation federation;

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

    @Override public double alternativePenalty(@NotNull CQuery query) {
        APIRequestExecutor executor = molecule.getExecutor();
        Set<String> reqUnbound = new HashSet<>(executor.getRequiredInputs()),
                    optUnbound = new HashSet<>(executor.getOptionalInputs());
        int nReq = reqUnbound.size(), nOpt = optUnbound.size();
        query.forEachTermAnnotation(AtomInputAnnotation.class, (t, a) -> {
            String input = a.getInputName();
            if (t.isGround() || a.isOverride()) {
                reqUnbound.remove(input);
                optUnbound.remove(input);
            }
        });
        if (!reqUnbound.isEmpty()) return Double.MAX_VALUE;
        // if there are no required inputs, start the penalty from 2, else from 1
        double base = nReq == 0 ? 2 : 1;
        // add up to 1 in penalty relative to proportion of unbound optionals
        if (nOpt > 0)
            base += optUnbound.size() / (double)nOpt;
        return base;
    }

    @Override
    public @Nonnull Cardinality estimate(@Nonnull CQuery query, int policy) {
        if (query.isEmpty()) return Cardinality.EMPTY;
        return molecule.getCardinality();
    }

    @Override
    public @Nonnull Results query(@Nonnull CQuery query) {
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
        Set<String> resultVars = query.attr().tripleVarNames();
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
        QueryGlobalContextCache cache = QueryGlobalContextCache.get(query);
        Iterator<? extends CQEndpoint> it;
        try {
            it = exec.execute(bound, cache);
        } catch (APIRequestExecutorException e) {
            logger.error("Exception on execution of query {} against {}. Will return empty results",
                         query, molecule.getName(), e);
            return CollectionResults.empty(resultVars);
        }
        return new EndpointIteratorResults(it, query);
    }

    public @Nonnull Results matchAndQuery(@Nonnull CQuery query) {
        return matchAndQuery(query, true);
    }

    private @Nonnull Results matchAndQuery(@Nonnull CQuery query, boolean throwOnFailedMatch) {
        Set<String> varNames = query.attr().allVarNames();
        CQueryMatch match = getMatcher().match(query);
        if (match.getKnownExclusiveGroups().isEmpty()) {
            return reportFailure(query, throwOnFailedMatch, varNames);
        } else if (match.getKnownExclusiveGroups().size() == 1) {
            CQuery subQuery = match.getKnownExclusiveGroups().get(0);
            if (subQuery.size() != query.size())
                return reportFailure(query, throwOnFailedMatch, varNames);
            return query(subQuery.withModifiers(query.getModifiers())); // no loop, since it has AtomAnnotations
        } else {
            Set<Triple> allTriples = match.getKnownExclusiveGroups().stream()
                    .flatMap(CQuery::stream).collect(toSet());
            if (!allTriples.equals(query.attr().getSet()))
                return reportFailure(query, throwOnFailedMatch, varNames);
            return getFederation().query(query);
        }
    }

    private @Nonnull Federation getFederation() {
        if (federation == null) {
            Source source = new Source(getMatcher(), this);
            federation = SingletonSourceFederation.createFederation(source);
        }
        return federation;
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
            case CARTESIAN:
            case LIMIT:
            case ASK:
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

    @Override public void close() {
        if (federation != null)
            federation.close();
        molecule.getExecutor().close();
        super.close();
    }

    @Override
    public @Nonnull String toString() {
        return String.format("WebAPICQEndpoint(%s, %s, %s)", getMolecule().getMolecule(),
                getMolecule().getExecutor(), getMolecule().getElement2Input());
    }
}
