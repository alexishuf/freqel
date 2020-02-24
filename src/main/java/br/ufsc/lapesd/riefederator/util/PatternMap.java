package br.ufsc.lapesd.riefederator.util;

import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.function.Predicate;

public class PatternMap<T> {
    public static final @Nonnull Predicate<String> ANY = s -> true;

    @VisibleForTesting
    static class Entry<U> {
        @Nonnull Predicate<String> predicate1, predicate2;
        @Nonnull U value;

        public Entry(@Nonnull Predicate<String> predicate1,
                     @Nonnull Predicate<String> predicate2, @Nonnull U value) {
            this.predicate1 = predicate1;
            this.predicate2 = predicate2;
            this.value = value;
        }

        public boolean matches(@Nonnull String s1, @Nonnull String s2) {
            return predicate1.test(s1) && predicate2.test(s2);
        }
    }

    private @Nonnull final List<Entry<T>> list = new ArrayList<>();

    public @Nullable T add(@Nonnull Predicate<String> predicate1,
                           @Nonnull Predicate<String> predicate2, @Nonnull T value) {
        for (Entry<T> entry : list) {
            if (entry.predicate1.equals(predicate1) && entry.predicate2.equals(predicate2)) {
                T old = entry.value;
                entry.value = value;
                return old;
            }
        }
        list.add(new Entry<>(predicate1, predicate2, value));
        return null;
    }

    public @Nullable T remove(@Nullable Predicate<String> predicate1,
                              @Nullable Predicate<String> predicate2) {
        T last = null;
        for (Iterator<Entry<T>> it = list.iterator(); it.hasNext(); ) {
            Entry<T> entry = it.next();
            if (predicate1 != null && !entry.predicate1.equals(predicate1))
                continue;
            if (predicate2 != null && !entry.predicate2.equals(predicate2))
                continue;
            it.remove();
            last = entry.value;
        }
        return last;
    }

    public @Nullable T getLast(@Nonnull String s1, @Nonnull String s2) {
        Entry<T> last = null;
        for (ListIterator<Entry<T>> it = list.listIterator(list.size()); it.hasPrevious(); ) {
            Entry<T> entry = it.previous();
            if (entry.matches(s1, s2)) {
                last = entry;
                break;
            }
        }
        return last == null ? null : last.value;
    }
}
