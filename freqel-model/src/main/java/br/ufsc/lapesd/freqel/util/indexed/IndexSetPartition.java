package br.ufsc.lapesd.freqel.util.indexed;

import com.google.errorprone.annotations.Immutable;
import com.google.errorprone.annotations.concurrent.LazyInit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

import static com.google.common.base.Preconditions.checkPositionIndex;

@Immutable
public class IndexSetPartition<T>  extends BaseIndexSet<T> implements ImmIndexSet<T> {
    private final @Nonnull BaseIndexSet<T> parent;
    private final int begin, end, size;
    private @LazyInit int hash = 0;

    public IndexSetPartition(@Nonnull BaseIndexSet<T> parent, int begin, int end) {
        super(parent.getIndexMap(), parent.getData());
        if (end < begin)
            throw new IllegalArgumentException("Negative size: end < begin");
        if (end > parent.size())
            throw new IllegalArgumentException("end > parent.size()");
        this.parent = parent;
        this.begin = begin;
        this.end = end;
        this.size = end - begin;
    }

    @Override public @Nonnull ImmIndexSet<T> asImmutable() {
        return this;
    }

    @Override public @Nonnull BaseIndexSet<T> getParent() {
        return parent;
    }

    @Override public @Nonnull IndexSet<T> copy() {
        return new IndexSetPartition<>((BaseIndexSet<T>) parent.copy(), begin, end);
    }

    @Override public @Nonnull ImmIndexSet<T> immutableCopy() {
        return this;
    }

    public static @Nonnull <U> IndexSetPartition<U> of(@Nonnull IndexSet<U> parent,
                                                       int begin, int end) {
        if (!(parent instanceof BaseIndexSet))
            throw new IllegalArgumentException("Expected a FullIndexedSet instance");
        BaseIndexSet<U> full = (BaseIndexSet<U>) parent;
        assert !(full instanceof IndexSetPartition) : "Inneficient";
        return new IndexSetPartition<>(full, begin, end);
    }

    public int getBegin() {
        return begin;
    }

    public int getEnd() {
        return end;
    }

    protected class PartitionEntrySetIt extends EntrySetIt<T> {
        private @Nullable Map.Entry<T, Integer> current = null;

        public PartitionEntrySetIt(@Nonnull Iterator<Map.Entry<T, Integer>> it) {
            super(it);
        }

        @Override public boolean hasNext() {
            while (current == null && it.hasNext()) {
                Map.Entry<T, Integer> candidate = it.next();
                int idx = candidate.getValue();
                if (idx >= begin && idx < end) current = candidate;
            }
            return current != null;
        }

        @Override public @Nonnull Map.Entry<T, Integer> next() {
            if (!hasNext()) throw new NoSuchElementException();
            Map.Entry<T, Integer> entry = this.current;
            assert entry != null;
            current = null;
            return entry;
        }
    }

    @Override public @Nonnull Iterator<Map.Entry<T, Integer>> entryIterator() {
        return new PartitionEntrySetIt(indexMap.entrySet().iterator());
    }

    private class It implements Iterator<T> {
        private int i = begin;

        @Override public boolean hasNext() {
            return i < end;
        }

        @Override public @Nonnull T next() {
            if (i >= end) throw new NoSuchElementException();
            return data.get(i++);
        }
    }

    @Override public @Nonnull Iterator<T> iterator() {
        return new It();
    }

    @Override public int size() {
        return size;
    }

    @Override public boolean contains(Object o) {
        @SuppressWarnings("SuspiciousMethodCalls") int idx = indexMap.getOrDefault(o, -1);
        return idx >= begin && idx < end;
    }

    @Override public T get(int index) {
        checkPositionIndex(index, size());
        return data.get(begin+index);
    }

    @Override public int indexOf(Object o) {
        @SuppressWarnings("SuspiciousMethodCalls") int idx = indexMap.getOrDefault(o, -1) - begin;
        return idx < 0 || idx >= size ? -1 : idx;
    }

    private class ListIt implements ListIterator<T> {
        private int cursor;

        private ListIt(int cursor) {
            this.cursor = cursor + begin;
        }

        @Override public boolean hasNext() { return cursor < end; }
        @Override public int nextIndex() { return cursor; }
        @Override public @Nonnull T next() {
            if (cursor >= end) throw new NoSuchElementException();
            return data.get(cursor++);
        }
        @Override public int previousIndex() { return cursor - 1; }
        @Override public boolean hasPrevious() { return cursor > begin; }
        @Override public T previous() {
            if (cursor <= begin) throw new NoSuchElementException();
            return data.get(--cursor);
        }

        @Override public void set(T t) {
            throw new UnsupportedOperationException("IndexedSubsetPartition is immutable");
        }
        @Override public void add(T t) {
            throw new UnsupportedOperationException("IndexedSubsetPartition is immutable");
        }
        @Override public void remove() {
            throw new UnsupportedOperationException("IndexedSubsetPartition is immutable");
        }
    }

    @Override public @Nonnull ListIterator<T> listIterator(int index) {
        return new ListIt(index);
    }

    @Override public @Nonnull ListIterator<T> listIterator() {
        return new ListIt(0);
    }

    @Override public @Nonnull List<T> subList(int fromIndex, int toIndex) {
        checkPositionIndex(fromIndex, size);
        checkPositionIndex(toIndex, size+1);
        assert fromIndex+begin <  data.size();
        assert toIndex  +begin <= data.size();
        return data.subList(fromIndex+begin, toIndex+begin);
    }

    @Override public int hashCode() {
        assert hash == 0 || hash == computeHashCode() : "Cached hash became invalid";
        return hash == 0 ? (hash = computeHashCode()) : hash;
    }

    private int computeHashCode() {
        int local = 0;
        for (int i = begin; i < end; i++)
            local += data.get(i).hashCode();
        return local;
    }
}
