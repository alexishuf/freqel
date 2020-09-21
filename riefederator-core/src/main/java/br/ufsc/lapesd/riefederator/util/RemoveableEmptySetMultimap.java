package br.ufsc.lapesd.riefederator.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.SetMultimap;
import org.checkerframework.checker.nullness.qual.Nullable;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class RemoveableEmptySetMultimap<K, V> implements SetMultimap<K, V> {
    public static final @Nonnull RemoveableEmptySetMultimap<?, ?> INSTANCE
            = new RemoveableEmptySetMultimap<>();

    @SuppressWarnings("unchecked") public static @Nonnull <K, V> RemoveableEmptySetMultimap<K, V> get() {
        return (RemoveableEmptySetMultimap<K, V>) INSTANCE;
    }

    @Override public Set<V> get(@Nullable K key) {
        return RemoveableEmptySet.get();
    }

    @Override public Set<K> keySet() {
        return RemoveableEmptySet.get();
    }

    @Override public Multiset<K> keys() {
        return RemoveableEmptyMultiset.get();
    }

    @Override public Collection<V> values() {
        return RemoveableEmptySet.get();
    }

    @Override public Set<V> removeAll(@Nullable Object key) {
        return null;
    }

    @Override public void clear() {
    }

    @Override public int size() {
        return 0;
    }

    @Override public boolean isEmpty() {
        return true;
    }

    @Override public boolean containsKey(@Nullable Object key) {
        return false;
    }

    @Override public boolean containsValue(@Nullable Object value) {
        return false;
    }

    @Override public boolean containsEntry(@Nullable Object key, @Nullable Object value) {
        return false;
    }

    @Override public boolean put(@Nullable K key, @Nullable V value) {
        throw new UnsupportedOperationException();
    }

    @Override public boolean remove(@Nullable Object key, @Nullable Object value) {
        return false;
    }

    @Override public boolean putAll(@Nullable K key, @Nonnull Iterable<? extends V> values) {
        throw new UnsupportedOperationException();
    }

    @Override public boolean putAll(@Nonnull Multimap<? extends K, ? extends V> multimap) {
        throw new UnsupportedOperationException();
    }

    @Override public Set<V> replaceValues(@Nonnull K key,
                                          @Nonnull Iterable<? extends V> values) {
        return null;
    }

    @Override public Set<Map.Entry<K, V>> entries() {
        return RemoveableEmptySet.get();
    }

    @Override public Map<K, Collection<V>> asMap() {
        return ImmutableMap.of();
    }

    @Override public int hashCode() {
        return 0;
    }

    @Override public boolean equals(Object o) {
        return o == this || (o instanceof Multimap && asMap().equals(((Multimap<?, ?>) o).asMap()));
    }
}
