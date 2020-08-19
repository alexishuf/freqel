package br.ufsc.lapesd.riefederator.query.modifiers;

import br.ufsc.lapesd.riefederator.query.endpoint.Capability;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;

@Immutable
public interface Modifier {
    @Nonnull Capability getCapability();
}
