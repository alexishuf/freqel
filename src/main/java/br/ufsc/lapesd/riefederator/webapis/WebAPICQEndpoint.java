package br.ufsc.lapesd.riefederator.webapis;

import br.ufsc.lapesd.riefederator.description.CQueryMatch;
import br.ufsc.lapesd.riefederator.federation.Federation;
import br.ufsc.lapesd.riefederator.federation.Source;
import br.ufsc.lapesd.riefederator.federation.decomp.DecompositionStrategy;
import br.ufsc.lapesd.riefederator.federation.decomp.EvenDecomposer;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.SimpleExecutionModule;
import br.ufsc.lapesd.riefederator.federation.planner.Planner;
import br.ufsc.lapesd.riefederator.federation.planner.impl.HeuristicPlanner;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.query.*;
import br.ufsc.lapesd.riefederator.query.impl.*;
import br.ufsc.lapesd.riefederator.query.modifiers.ModifierUtils;
import br.ufsc.lapesd.riefederator.webapis.description.APIMolecule;
import br.ufsc.lapesd.riefederator.webapis.description.APIMoleculeMatcher;
import br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation;
import br.ufsc.lapesd.riefederator.webapis.requests.MismatchingQueryException;
import br.ufsc.lapesd.riefederator.webapis.requests.MissingAPIInputsException;
import br.ufsc.lapesd.riefederator.webapis.requests.NoTermSerializationException;
import br.ufsc.lapesd.riefederator.webapis.requests.impl.APIRequestExecutorException;
import com.google.inject.Guice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.lang.ref.SoftReference;
import java.util.Iterator;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

public class WebAPICQEndpoint extends AbstractTPEndpoint implements CQEndpoint {
    private static final @Nonnull Logger logger = LoggerFactory.getLogger(WebAPICQEndpoint.class);
    private final @Nonnull APIMolecule molecule;
    private @Nonnull SoftReference<APIMoleculeMatcher> matcher = new SoftReference<>(null);

    public WebAPICQEndpoint(@Nonnull APIMolecule molecule) {
        this.molecule = molecule;
    }

    public @Nonnull APIMolecule getMolecule() {
        return molecule;
    }

    @Override
    public @Nonnull Results query(@Nonnull CQuery query) {
        MapSolution.Builder b = MapSolution.builder();
        boolean hasAtomAnnotations = query.forEachTermAnnotation(AtomAnnotation.class, (t, a) -> {
            if (!a.isInput()) return;
            String atomName = a.getAtomName();
            String input = molecule.getAtom2input().get(atomName);
            if (input != null) {
                if (t.isGround()) {
                    b.put(input, t);
                } else if (molecule.getExecutor().getRequiredInputs().contains(input)) {
                    logger.error("Required input {} (Atom={}) is not ground!", input, atomName);
                }
            }
        });
        if (!hasAtomAnnotations && molecule.getExecutor().hasInputs()) {
            logger.info("No AtomAnnotations in {}. Will call matchAndQuery()", query);
            return matchAndQuery(query, false);
        }
        // from here onwards, this class is responsible for modifiers
        ModifierUtils.check(this, query.getModifiers());
        Iterator<? extends CQEndpoint> it;
        try {
            it = molecule.getExecutor().execute(b.build());
        } catch (APIRequestExecutorException e) {
            logger.error("Exception on execution of query {}. Will return empty results", query, e);
            Set<String> names = query.streamTerms(Var.class).map(Var::getName).collect(toSet());
            return CollectionResults.empty(names);
        }
        Results results = new EndpointIteratorResults(it, query);
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
            return query(subQuery); // no loop, since it has AtomAnnotations
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
                    bind(Planner.class).to(HeuristicPlanner.class);
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
                return true;
            default:
                return false;
        }
    }

    @Override
    public @Nonnull String toString() {
        return String.format("WebAPICQEndpoint(%s, %s, %s)", getMolecule().getMolecule(),
                getMolecule().getExecutor(), getMolecule().getAtom2input());
    }
}
