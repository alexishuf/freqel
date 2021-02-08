package br.ufsc.lapesd.freqel.algebra.util;

import br.ufsc.lapesd.freqel.algebra.InnerOp;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.inner.CartesianOp;
import br.ufsc.lapesd.freqel.algebra.inner.ConjunctionOp;
import br.ufsc.lapesd.freqel.algebra.inner.JoinOp;
import br.ufsc.lapesd.freqel.algebra.inner.UnionOp;
import br.ufsc.lapesd.freqel.algebra.leaf.*;
import br.ufsc.lapesd.freqel.query.endpoint.Capability;
import br.ufsc.lapesd.freqel.query.endpoint.DQEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.DisjunctiveProfile;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DQPushChecker {
    protected @Nonnull DisjunctiveProfile profile;
    protected @Nullable DQEndpoint shared = null;

    public DQPushChecker(@Nonnull DisjunctiveProfile profile) {
        this.profile = profile;
    }

    public void reset() {
        shared = null;
    }

    public @Nonnull DQPushChecker setEndpoint(DQEndpoint endpoint) {
        shared = endpoint;
        return this;
    }

    public enum Role {
        OUTER,
        ANY,
        INNER,
        NONE;

        public boolean canOuter() {
            return this == OUTER || this == ANY;
        }
        public boolean canInner() {
            return this == INNER || this == ANY;
        }
    }

    public boolean canPush(@Nonnull Op op) {
        Role role = allowedTreeRole(op);
        return role == Role.ANY || role == Role.OUTER;
    }

    public @Nonnull Role allowedRole(@Nonnull Op op) {
        if (shared == null)
            throw new IllegalStateException("No shared DQEndpoint set");
        Role allowed = Role.ANY;
        if (op instanceof InnerOp) {
            if (op instanceof UnionOp && !profile.allowsUnion()) return Role.NONE;
            if ((op instanceof JoinOp || op instanceof ConjunctionOp) && !profile.allowsJoin())
                return Role.NONE;
            if (op instanceof CartesianOp) {
                if (!profile.allowsProduct())
                    return Role.NONE;
                if (op.modifiers().filters().isEmpty()) {
                    if (!profile.allowsUnfilteredProduct())
                        return Role.NONE;
                    if (!profile.allowsUnfilteredProductRoot())
                        allowed = Role.INNER;
                }
            }
        } else {
            if (isUnassigned(op)) {
                if (!profile.allowsUnassignedQuery()) return Role.NONE;
            } else if (!isEmpty(op)) {
                if (getEndpoint(op) != shared) return Role.NONE;
            }
        }
        if (!op.modifiers().filters().stream().allMatch(profile::allowsFilter)) {
            // assert allowed != Role.NONE;
            if (shared.hasCapability(Capability.SPARQL_FILTER)) {
                if (allowed == Role.INNER) return Role.NONE;
                else                       allowed = Role.OUTER;
            } else {
                return Role.NONE;
            }
        }
        if (op.modifiers().optional() != null) { // allowed != Role.NONE;
            if (!profile.allowsOptional()) {
                if (allowed == Role.INNER)                           return Role.NONE;
                else if (!shared.hasCapability(Capability.OPTIONAL)) return Role.NONE;
                else                                                 allowed = Role.OUTER;
            }
        }
        return allowed;
    }

    public @Nonnull Role allowedTreeRole(@Nonnull Op op) {
        if (op instanceof InnerOp) {
            for (Op child : op.getChildren()) {
                Role role = allowedTreeRole(child);
                if (!role.canInner()) return Role.NONE;
            }
        }
        if (shared == null)
            return Role.NONE;
        return allowedRole(op);
    }

    protected static boolean isUnassigned(@Nonnull Op op) {
        return op instanceof QueryOp && !(op instanceof EndpointQueryOp);
    }

    protected static boolean isEmpty(@Nonnull Op op) {
        return op instanceof EmptyOp;
    }

    protected static @Nullable DQEndpoint getEndpoint(@Nonnull Op op) {
        TPEndpoint ep = null;
        if (op instanceof SPARQLValuesTemplateOp)
            ep = ((SPARQLValuesTemplateOp) op).getEndpoint().getEffective();
        else if (op instanceof EndpointOp)
            ep = ((EndpointOp) op).getEffectiveEndpoint();
        return ep instanceof DQEndpoint ? (DQEndpoint)ep : null;
    }
}
