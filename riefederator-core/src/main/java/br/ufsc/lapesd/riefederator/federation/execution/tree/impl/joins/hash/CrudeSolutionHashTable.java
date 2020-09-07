package br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.hash;

import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Consumer;

import static java.util.Collections.singletonList;

/**
 * A Simple HashMap for use during hash-joins that does not use memory for the keys
 */
public class CrudeSolutionHashTable {
    private final @Nonnull List<ArrayList<Solution>> buckets;
    private final @Nonnull Collection<String> varNames;
    private final int nBuckets, bucketCapacity;
    private @Nullable ArrayList<BitSet> fetched = null;

    public CrudeSolutionHashTable(@Nonnull Collection<String> varNames, int expectedValues) {
        this(varNames, expectedValues, 16);
    }

    public CrudeSolutionHashTable(@Nonnull Collection<String> varNames,
                                  int expectedValues, int bucketCapacity) {
        this.varNames = varNames;
        if (varNames.isEmpty()) { // only a single bucket will ever be used
            this.nBuckets = 1;
            this.bucketCapacity = expectedValues;
            this.buckets = singletonList(new ArrayList<>(expectedValues));
        } else {
            this.nBuckets = Math.max((int) Math.ceil(expectedValues / (double) bucketCapacity), 64);
            this.bucketCapacity = bucketCapacity;
            this.buckets = new ArrayList<>(nBuckets);
            for (int i = 0; i < nBuckets; i++)
                this.buckets.add(new ArrayList<>(bucketCapacity));
        }
    }

    public void recordFetches() {
        Preconditions.checkState(fetched == null, "Already recording!");
        int size = buckets.size();
        fetched = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
            fetched.add(new BitSet());
    }

    protected int getBucketIndex(@Nonnull Solution solution) {
        int hash = 17;
        for (String name : varNames) {
            Term term = solution.get(name);
            hash = 37*hash + (term == null ? 17 : term.hashCode());
        }
        return Math.abs(hash) % nBuckets;
    }

    public void clear() {
        if (fetched != null) {
            for (BitSet bitSet : fetched) bitSet.clear();
        }
        for (ArrayList<Solution> b : buckets) b.clear();
    }

    public class AddedHandle {
        private int bucketIndex, solutionIndex;

        public AddedHandle(int bucketIndex, int solutionIndex) {
            this.bucketIndex = bucketIndex;
            this.solutionIndex = solutionIndex;
        }

        public void markFetched() {
            if (fetched != null)
                fetched.get(bucketIndex).set(solutionIndex);
        }
    }

    public AddedHandle add(@Nonnull Solution solution) {
        int bucketIndex = getBucketIndex(solution);
        ArrayList<Solution> bucket = buckets.get(bucketIndex);
        AddedHandle handle = new AddedHandle(bucketIndex, bucket.size());
        bucket.add(solution);
        return handle;
    }

    public @Nonnull Collection<Solution> getAll(@Nonnull Solution reference) {
        if (varNames.isEmpty()) { // single large bucket special case
            ArrayList<Solution> bucket = buckets.get(0);
            if (fetched != null)
                fetched.get(0).set(0, bucket.size()); //mark all as fetched
            return Collections.unmodifiableList(bucket);
        }
        ArrayList<Solution> list = new ArrayList<>(bucketCapacity);
        int bit = -1, bucketIndex = getBucketIndex(reference);
        BitSet bitset = fetched == null ? null : fetched.get(bucketIndex);
        outer:
        for (Solution sol : buckets.get(bucketIndex)) {
            ++bit;
            for (String name : varNames) {
                if (!Objects.equals(sol.get(name), reference.get(name)))
                    continue outer;
            }
            list.add(sol);
            if (bitset != null) bitset.set(bit);
        }
        return list;
    }

    public void forEachNotFetched(@Nonnull Consumer<Solution> consumer) {
        Preconditions.checkState(fetched != null, "Fetches not recorded");
        assert fetched.size() == buckets.size();
        for (int i = 0, size = fetched.size(); i < size; i++) {
            BitSet bs = fetched.get(i);
            ArrayList<Solution> bucket = buckets.get(i);
            for (int s = bucket.size(), j = bs.nextClearBit(0); j < s; j = bs.nextClearBit(j+1))
                consumer.accept(bucket.get(j));
        }
    }

    public void forEach(@Nonnull Consumer<Solution> consumer) {
        for (ArrayList<Solution> bucket : buckets) {
            for (Solution solution : bucket) {
                consumer.accept(solution);
            }
        }
    }

    public @Nonnull List<Solution> toList() {
        ArrayList<Solution> list = new ArrayList<>(buckets.size()*bucketCapacity);
        for (ArrayList<Solution> bucket : buckets)
            list.addAll(bucket);
        list.trimToSize();
        return list;
    }
}
