package br.ufsc.lapesd.riefederator.util.indexed;

import javax.annotation.Nonnull;
import java.util.BitSet;

public class NotInParentException extends IllegalArgumentException {
    public Object value;
    public IndexSet<?> parent;
    public BitSet bitSet;

    public NotInParentException(@Nonnull Object value, @Nonnull IndexSet<?> parent) {
        super(String.format("Value %s is not present in parent IndexSet %s", value, parent));
        this.value = value;
        this.parent = parent;
    }

    public NotInParentException(@Nonnull BitSet bitSet, @Nonnull IndexSet<?> parent) {
        super(String.format("Bitset %s has indices beyond size of parent %s", bitSet, parent));
        this.parent = parent;
        this.bitSet = bitSet;
    }
}
