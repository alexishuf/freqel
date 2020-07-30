package br.ufsc.lapesd.riefederator.model.term.std;

import br.ufsc.lapesd.riefederator.model.term.AbstractBlank;
import br.ufsc.lapesd.riefederator.model.term.Blank;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicLong;

@Immutable
public class StdBlank extends AbstractBlank {
    private static final AtomicLong nextId = new AtomicLong();
    private final Object id;
    private final @Nullable String name;

    public StdBlank(@Nullable String name) {
        this.name = name;
        this.id = nextId.incrementAndGet();
    }
    public StdBlank(@Nonnull String name, @Nonnull Object id) {
        this.name = name;
        this.id = id;
    }

    public StdBlank() {
        this(null);
    }

    public static @Nonnull Blank blank() {
        return new StdBlank();
    }

    @Override
    public @Nonnull Object getId() {
        return id;
    }

    @Override
    public @Nullable String getName() {
        return name;
    }

    @Override
    public @Nonnull String toString() {
        if (name != null) return "_:"+name;
        assert id instanceof Long;
        //noinspection RedundantCast
        return String.format("_:0x%x", (Long)id);
    }
}
