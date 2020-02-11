package br.ufsc.lapesd.riefederator.query.impl;

import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.query.*;
import br.ufsc.lapesd.riefederator.query.error.ResultsCloseException;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import static br.ufsc.lapesd.riefederator.query.Cardinality.Reliability.LOWER_BOUND;
import static java.util.stream.Collectors.toSet;

public class EndpointIteratorResults implements Results {
    private static final Logger logger = LoggerFactory.getLogger(EndpointIteratorResults.class);

    private final @Nonnull Iterator<? extends TPEndpoint> epIterator;
    private final @Nonnull CQuery query;
    private final @Nonnull Set<String> varNames;
    private final boolean projecting;
    private @Nullable Results current = null;
    private TPEndpoint currentEp;

    private static @Nonnull Set<String> computeVarNames(@Nonnull CQuery query) {
        return query.streamTerms(Var.class).map(Var::getName).collect(toSet());
    }

    public EndpointIteratorResults(@Nonnull Iterator<? extends TPEndpoint> epIterator,
                                   @Nonnull CQuery query,
                                   @Nonnull Set<String> varNames, boolean projecting) {
        if (getClass().desiredAssertionStatus()) {
            Set<String> all = computeVarNames(query);
            Preconditions.checkArgument(all.containsAll(varNames),
                    "There are names in varNames which do not occur in query!");
            Preconditions.checkArgument(projecting || all.equals(varNames),
                    "Not projecting, but given varNames misses some var names in query");
            Preconditions.checkArgument(!projecting || !all.equals(varNames),
                    "Projecting, but given varNames contains all variables");
        }
        this.epIterator = epIterator;
        this.query = query;
        this.varNames = varNames;
        this.projecting = projecting;
    }

    public EndpointIteratorResults(@Nonnull Iterator<? extends TPEndpoint> epIterator,
                                   @Nonnull CQuery query) {
        this(epIterator, query, computeVarNames(query), false);
    }

    @Override
    public int getReadyCount() {
        return current != null ? current.getReadyCount() : 0;
    }

    @Override
    public boolean hasNext() {
        while ((current == null || !current.hasNext()) && epIterator.hasNext()) {
            if (currentEp != null) {
                try {
                    currentEp.close();
                } catch (ResultsCloseException e) {
                    logger.error("Ignoring (currentEp={}).close() exception", currentEp, e);
                }
                currentEp = null;
            }
            currentEp = epIterator.next();
            if (currentEp != null) {
                if (query.size() > 1) {
                    if (!(currentEp instanceof CQEndpoint)) {
                        logger.error("Skipping TP-only ep {} for {}", query, currentEp);
                    } else {
                        current = currentEp.query(this.query);
                    }
                } else {
                    current = currentEp.query(this.query);
                }
            }
        }
        return current != null && current.hasNext();
    }

    @Override
    public @Nonnull Solution next() {
        if (!hasNext())
            throw new NoSuchElementException();

        assert current != null;
        Solution next = current.next();
        if (projecting) {
            MapSolution.Builder b = MapSolution.builder();
            for (String name : varNames) {
                Term term = next.get(name);
                if (term != null)
                    b.put(name, term);
                else
                    logger.info("Missing projected {} from ep {} for {}", name, currentEp, query);
            }
            next = b.build();
        }
        return next;
    }

    @Override
    public @Nonnull Cardinality getCardinality() {
        if (current == null)
            return Cardinality.UNSUPPORTED;
        Cardinality card = current.getCardinality();
        if (card.getReliability().ordinal() > LOWER_BOUND.ordinal() && epIterator.hasNext())
            return new Cardinality(LOWER_BOUND, card.getValue(0));
        return card;
    }

    @Override
    public @Nonnull Set<String> getVarNames() {
        return varNames;
    }

    @Override
    public void close() throws ResultsCloseException {
        if (currentEp != null) {
            currentEp.close();
            currentEp = null;
        }
    }
}
