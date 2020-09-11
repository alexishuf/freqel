package br.ufsc.lapesd.riefederator.query.endpoint;

import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nonnull;

public enum  Capability {
    ASK,
    PROJECTION,
    DISTINCT,
    LIMIT,
    SPARQL_FILTER,
    VALUES,
    OPTIONAL;

    public boolean isUniqueModifier() {
        switch (this) {
            case ASK:
            case LIMIT:
            case DISTINCT:
            case PROJECTION:
            case VALUES:
            case OPTIONAL:
                return true;
            default:
                return false;
        }
    }

    /**
     * Indicates whether it is safe to add a modifier from a query a into query b when query
     * b does not have such modifier or has it with a different value.
     *
     * - OPTIONAL is unsafe as doing so would apply OPTIONAL semantics to a query that was
     * not OPTIONAL
     * - LIMIT is unsafe as the number of results from the join cannot be foreseen
     * - ASK is unsafe since the receiving query was not under ASK semantics and results are lost
     * - DISTINCT is unsafe since it can cause result eliminations
     */
    public boolean isMergeUnsafe() {
        switch (this) {
            case VALUES:
            case PROJECTION:
            case SPARQL_FILTER:
                return false;
            default:
                return true;
        }
    }

    /**
     * Indicates if modifiers have parameters, such as values, bindings, etc.
     */
    public boolean hasParameter() {
        switch (this) {
            case ASK:
            case DISTINCT:
                return false;
            default:
                return true;
        }
    }

    @CanIgnoreReturnValue
    public @Nonnull TPEndpoint requireFrom(@Nonnull TPEndpoint ep) throws MissingCapabilityException {
        if (!ep.hasCapability(this))
            throw new MissingCapabilityException(this, ep);
        return ep;
    }

    @CanIgnoreReturnValue
    public @Nonnull TPEndpoint
    requireFromRemote(@Nonnull TPEndpoint ep) throws MissingCapabilityException {
        if (!ep.hasRemoteCapability(this))
            throw new MissingCapabilityException(this, ep);
        return ep;
    }
}
