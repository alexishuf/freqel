package br.ufsc.lapesd.riefederator.query.modifiers;

import br.ufsc.lapesd.riefederator.query.Capability;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;

@Immutable
public interface Modifier {
    @Nonnull Capability getCapability();

    /**
     * If true, an Endpoint that cannot implement this modifier, will thrown an exception.
     * If false, it will silently drop the modifier.
     */
    boolean isRequired();
}
