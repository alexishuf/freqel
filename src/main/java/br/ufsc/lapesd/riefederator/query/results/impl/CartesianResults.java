package br.ufsc.lapesd.riefederator.query.results.impl;

import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.ResultsCloseException;
import br.ufsc.lapesd.riefederator.query.results.ResultsList;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

public class CartesianResults implements Results {
    private static final @Nonnull Logger logger = LoggerFactory.getLogger(CartesianResults.class);

    private @Nullable String nodeName;
    private @Nonnull Results largest;
    private @Nullable List<List<Solution>> fetched;
    private final @Nonnull Set<String> varNames;
    private int productSize = -1;
    private @Nonnull final ResultsList toFetch;
    private int readyProduct;
    private Solution current;
    private Iterator<List<Solution>> cartesianIt;

    public CartesianResults(@Nonnull Results largest,
                            @Nonnull List<Results> toFetch,
                            @Nonnull Set<String> varNames) {
        this.largest = largest;
        this.varNames = varNames;
        this.toFetch = ResultsList.of(toFetch);
        this.current = null;
        this.readyProduct = 0;
        this.cartesianIt = new Iterator<List<Solution>>() {
            @Override
            public boolean hasNext() { return false; }
            @Override
            public List<Solution> next() { throw new NoSuchElementException(); }
        };
    }

    @Override
    public @Nullable String getNodeName() {
        return nodeName;
    }

    @Override
    public void setNodeName(@Nullable String nodeName) {
        this.nodeName = nodeName;
    }

    @Override
    public int getReadyCount() {
        if (fetched == null) return 0;
        return fetched.isEmpty() ? largest.getReadyCount() : readyProduct;
    }

    @Override
    public boolean hasNext() {
        if (fetched == null) fetchAll();
        return cartesianIt.hasNext() || largest.hasNext();
    }

    @Override
    public @Nonnull Solution next() {
        if (fetched == null)
            fetchAll();
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

    private void fetchAll() {
        fetched = new ArrayList<>();
        for (Results results : toFetch) {
            List<Solution> list = new ArrayList<>();
            results.forEachRemainingThenClose(list::add);
            if (list.isEmpty()) {
                logger.info("There are empty solution lists. Will ignore all and be an empty result");
                fetched = Collections.emptyList(); // drop reference to allow GC
                largest = new CollectionResults(Collections.emptyList(), largest.getVarNames());
                productSize = 0;
                largest = new CollectionResults(Collections.emptyList(), largest.getVarNames());
                return;
            }
            fetched.add(list);
        }
        productSize = fetched.stream().map(List::size).reduce((l, r) -> l * r).orElse(0);
    }

    @Override
    public @Nonnull Set<String> getVarNames() {
        return varNames;
    }

    @Override
    public void close() throws ResultsCloseException {
        toFetch.close();
        largest.close();
    }
}
