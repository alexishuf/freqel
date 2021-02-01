package br.ufsc.lapesd.freqel.query.modifiers;

import br.ufsc.lapesd.freqel.query.endpoint.Capability;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.query.modifiers.filter.SPARQLFilter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ModifierUtils {
    private static final Map<Class<? extends Modifier>, Capability> cls2cap;

    static {
        Map<Class<? extends Modifier>, Capability> map = new HashMap<>();
        map.put(Ask.class, Capability.ASK);
        map.put(Projection.class, Capability.PROJECTION);
        map.put(Distinct.class, Capability.DISTINCT);
        map.put(Limit.class, Capability.LIMIT);
        map.put(SPARQLFilter.class, Capability.SPARQL_FILTER);
        map.put(ValuesModifier.class, Capability.VALUES);
        cls2cap = map;
    }

    public static @Nonnull Capability getCapability(@Nonnull Class<? extends Modifier> modClass) {
        Capability capability = cls2cap.get(modClass);
        if (capability == null) throw new IllegalArgumentException("No Capability for "+modClass);
        return capability;
    }

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
            if (!endpoint.hasCapability(mod.getCapability())) {
                ok = false;
                b.append(mod);
            }
        }
        if (!ok)
            throw new IllegalArgumentException(b.toString());
    }
}
