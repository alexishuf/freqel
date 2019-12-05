package br.ufsc.lapesd.riefederator.rdf.term.std;

import br.ufsc.lapesd.riefederator.rdf.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.rdf.term.Blank;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicLong;

@Immutable
public class StdBlank implements Blank {
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
    public Type getType() {
        return Type.BLANK;
    }

    @Override
    public @Nonnull String toString() {
        if (name != null) return "_:"+name;
        return String.format("_:0x%x", id);
    }

    @Override
    public @Nonnull String toString(@Nonnull PrefixDict dict) {
        return toString();
    }
}
