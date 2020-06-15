package br.ufsc.lapesd.riefederator.util;

import com.google.errorprone.annotations.Immutable;
import com.google.errorprone.annotations.concurrent.LazyInit;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nonnull;
import java.util.*;

@Immutable
public class ArraySet<T> implements Set<T>  {
    private final @Nonnull Object[] array;
    private final int size;
    private @LazyInit int hash = 0;

    private static boolean distinctNonNull(Object[] array, int size) {
        if (size > 0 && array[0] == null)
            return false;
        for (int i = 1; i < size; i++) {
            if (array[i] == null)
                return false;
            if (Objects.equals(array[i-1], array[i]))
                return false;
        }
        return true;
    }

    public static @Nonnull <U> ArraySet<U> fromDistinct(@Nonnull Collection<U> collection) {
        if (collection instanceof ArraySet)
            return (ArraySet<U>)collection;
        Object[] array = collection.toArray();
        if (!(collection instanceof SortedSet))
            Arrays.sort(array);
        assert distinctNonNull(array, array.length);
        return new ArraySet<>(array, collection.size());
    }

    public static @Nonnull <U> ArraySet<U> fromDistinct(@Nonnull Iterator<U> it) {
        Object[] array = new Object[16];
        int size = 0;
        while (it.hasNext()) {
            if (size == array.length)
                array = Arrays.copyOf(array, array.length*2);
            array[size++] = it.next();
        }
        Arrays.sort(array, 0, size);
        assert distinctNonNull(array, size);
        if (array.length - size > 32)
            array = Arrays.copyOf(array, size);
        return new ArraySet<>(array, size);
    }

    protected ArraySet(@Nonnull Object[] array, int size) {
        this.array = array;
        this.size = size;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public boolean contains(Object o) {
        if (o == null)
            return false;
        return Arrays.binarySearch(array, 0, size, o) >= 0;
    }

    private static class It<U> implements Iterator<U> {
        private int nextIdx;
        private final int size;
        private final @Nonnull U[] array;

        public It(@Nonnull U[] array, int size) {
            this.array = array;
            this.size = size;
            nextIdx = 0;
        }

        @Override
        public boolean hasNext() {
            return nextIdx < size;
        }

        @Override
        public U next() {
            if (!hasNext()) throw new NoSuchElementException();
            int currIdx = nextIdx;
            ++nextIdx;
            return array[currIdx];
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public @Nonnull Iterator<T> iterator() {
        return new It<T>((T[])array, size);
    }

    @Override
    public @Nonnull Object[] toArray() {
        return Arrays.copyOf(array, size);
    }

    @SuppressWarnings("unchecked")
    @Override
    public @Nonnull <T1> T1[] toArray(@NotNull T1[] a) {
        if (a.length < size)
            a = (T1[])new Object[size];
        //noinspection SuspiciousSystemArraycopy
        System.arraycopy(array, 0, a, 0, size);
        return a;
    }

    @Override
    public boolean add(T t) {
        throw new UnsupportedOperationException("ArraySet is immutable");
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException("ArraySet is immutable");
    }

    @Override
    public boolean containsAll(@NotNull Collection<?> c) {
        if (c instanceof Set && c.size() > size) return false;
        for (Object item : c) {
            if (!contains(item)) return false;
        }
        return true;
    }

    @Override
    public boolean addAll(@NotNull Collection<? extends T> c) {
        throw new UnsupportedOperationException("ArraySet is immutable");
    }

    @Override
    public boolean retainAll(@NotNull Collection<?> c) {
        throw new UnsupportedOperationException("ArraySet is immutable");
    }

    @Override
    public boolean removeAll(@NotNull Collection<?> c) {
        throw new UnsupportedOperationException("ArraySet is immutable");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("ArraySet is immutable");
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (!(obj instanceof Collection)) return false;
        if (obj instanceof Set && size() != ((Set<?>)obj).size()) return false;
        if (obj instanceof ArraySet) {
            int oHash = ((ArraySet<?>) obj).hash;
            if (hash != 0 && oHash != 0 && hash != oHash) return false;
        }
        if (obj instanceof SortedSet) {
            Iterator<?> it = ((SortedSet<?>) obj).iterator();
            for (int i = 0; i < size; i++) {
                assert it.hasNext(); // same size already checked
                if (!Objects.equals(array[i], it.next()))
                    return false;
            }
            return true; //no mismatches
        }
        Collection<?> coll = (Collection<?>) obj;
        return containsAll(coll) && (coll.size() == size || coll.containsAll(this));
    }

    @Override
    public int hashCode() {
        int local = this.hash;
        if (local == 0) {
            local = 17;
            for (int i = 0; i < size; i++)
                local = local * 37 + array[i].hashCode();
            this.hash = local;
        }
        return local;
    }
}
