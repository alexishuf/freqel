package br.ufsc.lapesd.riefederator.query.modifiers;

import br.ufsc.lapesd.riefederator.query.endpoint.Capability;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;

public class ModifierUtils {
    public static @Nullable Modifier getFirst(@Nonnull Capability capability,
                                              @Nonnull Collection<? extends Modifier> coll) {
        for (Modifier mod : coll) {
            if (mod.getCapability() == capability)
                return mod;
        }
        return null;
    }

    public static @Nullable  <T extends Modifier>
    T getFirst(@Nonnull Class<T> aClass, @Nonnull Collection<? extends Modifier> coll) {
        for (Modifier mod : coll) {
            if (aClass.isAssignableFrom(mod.getClass())) {
                //noinspection unchecked
                return (T) mod;
            }
        }
        return null;
    }


    /**
     * @throws IllegalArgumentException if endpoint does not support all
     *                                  capabilities of required ({@link Modifier}
     *                                  <code>.isRequired()</code>) modifiers.
     */
    public static void check(@Nonnull TPEndpoint endpoint,
                             @Nonnull Collection<Modifier> modifiers) {
        StringBuilder b = new StringBuilder();
        b.append("Modifiers not supported by ").append(endpoint).append(": ");
        boolean  ok = true;
        for (Modifier mod : modifiers) {
            if (!endpoint.hasCapability(mod.getCapability()) && mod.isRequired()) {
                ok = false;
                b.append(mod);
            }
        }
        if (!ok)
            throw new IllegalArgumentException(b.toString());
    }
}
