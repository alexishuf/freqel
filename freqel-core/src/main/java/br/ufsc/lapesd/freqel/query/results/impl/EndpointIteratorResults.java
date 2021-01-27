package br.ufsc.lapesd.freqel.query.results.impl;

import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.query.results.AbstractResults;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.ResultsCloseException;
import br.ufsc.lapesd.freqel.query.results.Solution;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

public class EndpointIteratorResults extends AbstractResults implements Results {
    private static final Logger logger = LoggerFactory.getLogger(EndpointIteratorResults.class);

    private final @Nonnull Iterator<? extends TPEndpoint> epIterator;
    private final @Nonnull CQuery query;
    private final @Nullable ArraySolution.ValueFactory projectingFactory;
    private @Nullable Results current = null;
    private TPEndpoint currentEp;

    public EndpointIteratorResults(@Nonnull Iterator<? extends TPEndpoint> epIterator,
                                   @Nonnull CQuery query,
                                   @Nonnull Set<String> varNames, boolean projecting) {
        super(varNames);
        if (getClass().desiredAssertionStatus()) {
            Set<String> all = query.attr().allVarNames();
            Preconditions.checkArgument(all.containsAll(varNames),
                    "There are names in varNames which do not occur in query!");
            Preconditions.checkArgument(projecting || all.equals(varNames),
                    "Not projecting, but given varNames misses some var names in query");
            Preconditions.checkArgument(!projecting || !all.equals(varNames),
                    "Projecting, but given varNames contains all variables");
        }
        this.epIterator = epIterator;
        this.query = query;
        projectingFactory = projecting ? ArraySolution.forVars(varNames) : null;
    }

    public EndpointIteratorResults(@Nonnull Iterator<? extends TPEndpoint> epIterator,
                                   @Nonnull CQuery query) {
        this(epIterator, query, query.attr().publicVarNames(),
                query.getModifiers().projection() != null);
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
    public @Nonnull
    Solution next() {
        if (!hasNext())
            throw new NoSuchElementException();
        assert current != null;
        Solution next = current.next();
        if (projectingFactory != null)
            return projectingFactory.fromSolution(next);
        return next;
    }

    @Override
    public void close() throws ResultsCloseException {
        if (currentEp != null) {
            currentEp.close();
            currentEp = null;
        }
    }
}
