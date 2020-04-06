package br.ufsc.lapesd.riefederator.query.results.impl;

import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.ResultsCloseException;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.*;

public class CartesianResults implements Results {
    private static final @Nonnull Logger logger = LoggerFactory.getLogger(CartesianResults.class);

    private final @Nonnull Results largest;
    private final @Nonnull List<? extends List<Solution>> fetched;
    private final @Nonnull Set<String> varNames;
    private final int productSize;
    private int readyProduct;
    private Solution current;
    private Iterator<List<Solution>> cartesianIt;

    public CartesianResults(@Nonnull Results largest,
                            @Nonnull List<? extends List<Solution>> fetched,
                            @Nonnull Set<String> varNames) {
        if (fetched.stream().anyMatch(List::isEmpty)) {
            logger.info("There are empty solution lists. Will ignore all and be an empty result");
            fetched = Collections.emptyList(); // drop reference to allow GC
            largest = new CollectionResults(Collections.emptyList(), largest.getVarNames());
        }
        this.largest = largest;
        this.fetched = fetched;
        this.varNames = varNames;
        this.current = null;
        this.productSize = fetched.stream().map(List::size).reduce((l, r) -> l * r).orElse(0);
        this.readyProduct = 0;
        cartesianIt = new Iterator<List<Solution>>() {
            @Override
            public boolean hasNext() { return false; }
            @Override
            public List<Solution> next() { throw new NoSuchElementException(); }
        };
    }

    @Override
    public int getReadyCount() {
        return fetched.isEmpty() ? largest.getReadyCount() : readyProduct;
    }

    @Override
    public boolean hasNext() {
        return cartesianIt.hasNext() || largest.hasNext();
    }

    @Override
    public @Nonnull Solution next() {
        if (fetched.isEmpty())
            return largest.next();
        if (current == null || !cartesianIt.hasNext()) {
            current = largest.next();
            readyProduct = productSize;
            cartesianIt = Lists.cartesianProduct(fetched).iterator();
        }
        MapSolution.Builder builder = MapSolution.builder();
        for (Solution sol : cartesianIt.next())
            sol.forEach(builder::put);
        --readyProduct;
        current.forEach(builder::put);
        return builder.build();
    }

    @Override
    public @Nonnull
    Cardinality getCardinality() {
        if (fetched.isEmpty())
            return largest.getCardinality();
        Cardinality cardinality = largest.getCardinality();
        Cardinality.Reliability reliability = cardinality.getReliability();
        if (reliability.ordinal() < Cardinality.Reliability.LOWER_BOUND.ordinal())
            reliability = Cardinality.Reliability.LOWER_BOUND;
        return new Cardinality(reliability,
                readyProduct + cardinality.getValue(0) * productSize);
    }

    @Override
    public @Nonnull Set<String> getVarNames() {
        return varNames;
    }

    @Override
    public void close() throws ResultsCloseException {
        largest.close();
    }
}
