package br.ufsc.lapesd.riefederator.federation;

import br.ufsc.lapesd.riefederator.description.Description;
import br.ufsc.lapesd.riefederator.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nonnull;

/**
 * A Source is a {@link TPEndpoint} (may also be a {@link CQEndpoint}) with a {@link Description}.
 */
public class Source {
    private final @Nonnull String name;
    private final @Nonnull Description description;
    private final @Nonnull TPEndpoint endpoint;
    private boolean closeEndpoint = false;

    public Source(@Nonnull Description description, @Nonnull TPEndpoint endpoint) {
        this(description, endpoint, description.toString() + "@" + endpoint);
    }

    public Source(@Nonnull Description description, @Nonnull TPEndpoint endpoint,
                  @Nonnull String name) {
        this.name = name;
        this.description = description;
        this.endpoint = endpoint;
    }

    public @Nonnull String getName() {
        return name;
    }

    public @Nonnull Description getDescription() {
        return description;
    }

    public @Nonnull TPEndpoint getEndpoint() {
        return endpoint;
    }

    public boolean getCloseEndpoint() {
        return closeEndpoint;
    }

    @CanIgnoreReturnValue
    public @Nonnull Source setCloseEndpoint(boolean closeEndpoint) {
        this.closeEndpoint = closeEndpoint;
        return this;
    }

    @Override
    public @Nonnull String toString() {
        return name;
    }
}
