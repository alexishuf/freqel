package br.ufsc.lapesd.riefederator.model.term.std;

import br.ufsc.lapesd.riefederator.model.term.AbstractBlank;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicLong;

@Immutable
public class StdBlank extends AbstractBlank {
    private static AtomicLong nextId = new AtomicLong();
    private final long id;
    private final @Nullable String name;

    public StdBlank(@Nullable String name) {
        this.name = name;
        this.id = nextId.incrementAndGet();
    }

    public StdBlank() {
        this(null);
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
        return String.format("_:0x%x", id);
    }
}
