package br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.hash;

import br.ufsc.lapesd.riefederator.query.results.Solution;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * A Simple HashMap for use during hash-joins that does not use memory for the keys
 */
public class CrudeSolutionHashTable {
    private @Nonnull ArrayList<ArrayList<Solution>> buckets;
    private @Nonnull Collection<String> varNames;
    private final int nBuckets, bucketCapacity;

    public CrudeSolutionHashTable(@Nonnull Collection<String> varNames, int expectedValues) {
        this(varNames, expectedValues, 16);
    }

    public CrudeSolutionHashTable(@Nonnull Collection<String> varNames,
                                  int expectedValues, int bucketCapacity) {
        this.varNames = varNames;
        this.nBuckets = Math.max((int)Math.ceil(expectedValues/(double)bucketCapacity), 64);
        this.bucketCapacity = bucketCapacity;
        this.buckets = new ArrayList<>(nBuckets);
        for (int i = 0; i < nBuckets; i++)
            this.buckets.add(new ArrayList<>(bucketCapacity));

    }

    protected  @Nonnull List<Solution> getBucket(@Nonnull Solution solution) {
        HashCodeBuilder builder = new HashCodeBuilder();
        for (String name : varNames)
            builder.append(solution.get(name));
        return buckets.get(Math.abs(builder.toHashCode()) % nBuckets);
    }

    public void clear() {
        for (ArrayList<Solution> b : buckets) {
            b.clear();
            b.trimToSize();
        }
    }

    public void add(@Nonnull Solution solution) {
        getBucket(solution).add(solution);
    }

    public @Nonnull Collection<Solution> getAll(@Nonnull Solution reference) {
        ArrayList<Solution> list = new ArrayList<>(bucketCapacity);
        outer:
        for (Solution sol : getBucket(reference)) {
            for (String name : varNames) {
                if (!Objects.equals(sol.get(name), reference.get(name)))
                    continue outer;
            }
            list.add(sol);
        }
        return list;
    }
}
