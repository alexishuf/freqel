package br.ufsc.lapesd.riefederator.federation.cardinality.impl;

import br.ufsc.lapesd.riefederator.federation.cardinality.CardinalityComparator;
import br.ufsc.lapesd.riefederator.federation.cardinality.CardinalityUtils;
import br.ufsc.lapesd.riefederator.federation.cardinality.InnerCardinalityComputer;
import br.ufsc.lapesd.riefederator.federation.tree.CartesianNode;
import br.ufsc.lapesd.riefederator.federation.tree.JoinNode;
import br.ufsc.lapesd.riefederator.federation.tree.MultiQueryNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.Cardinality.Reliability;
import br.ufsc.lapesd.riefederator.query.CardinalityAdder;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.math.BigInteger;

import static br.ufsc.lapesd.riefederator.query.Cardinality.Reliability.*;

public class DefaultInnerCardinalityComputer implements InnerCardinalityComputer {
    private final @Nonnull CardinalityComparator comparator;
    private final @Nonnull CardinalityAdder adder;

    @Inject
    public DefaultInnerCardinalityComputer(@Nonnull CardinalityComparator comparator,
                                           @Nonnull CardinalityAdder adder) {
        this.comparator = comparator;
        this.adder = adder;
    }

    @Override
    public @Nonnull Cardinality compute(JoinNode n) {
        Cardinality lc = n.getLeft().getCardinality(), rc = n.getRight().getCardinality();
        if (lc.equals(Cardinality.EMPTY) || rc.equals(Cardinality.EMPTY))
            return Cardinality.EMPTY;
        return CardinalityUtils.worstAvg(comparator, lc, rc);
    }

    @Override
    public @Nonnull Cardinality compute(CartesianNode n) {
        // special cases
        if (n.getChildren().isEmpty()) return Cardinality.EMPTY;
        if (n.getChildren().stream().anyMatch(m -> m.getCardinality().equals(Cardinality.EMPTY)))
            return Cardinality.EMPTY;

        // general case:
        Reliability r = null;
        long v = 0;
        for (PlanNode child : n.getChildren()) {
            Cardinality c = child.getCardinality();
            if (r == null) {
                r = c.getReliability();
                v = c.getValue(-1);
            } else if (r == UNSUPPORTED && c.getReliability() == UNSUPPORTED) {
                v = -1;
            } else if (r.isAtMost(LOWER_BOUND)) {
                // get best reliability limited to LOWER_BOUND
                if (c.getReliability().isAtMost(LOWER_BOUND))
                    r = r.isAtLeast(c.getReliability()) ? r : c.getReliability();
                assert c.getValue(-1) >= 0;
                BigInteger ov = BigInteger.valueOf(c.getValue(v)); // if unsupported, square
                BigInteger m = BigInteger.valueOf(v).multiply(ov);
                v = m.bitLength() <= 63 ? m.longValue() : Long.MAX_VALUE;
            } else {
                assert r.isAtLeast(UPPER_BOUND);
                // get worst reliability
                r = c.getReliability().isAtMost(r) ? c.getReliability() : r;
                BigInteger ov = BigInteger.valueOf(c.getValue(v)); // if unsupported, square
                BigInteger m = BigInteger.valueOf(v).multiply(ov);
                v = m.bitLength() <= 63 ? m.longValue() : Long.MAX_VALUE;
            }
        }
        assert r != null; //due to special case
        assert r == UNSUPPORTED || v >= 0;
        return r == UNSUPPORTED ? Cardinality.UNSUPPORTED : new Cardinality(r, v);
    }

    @Override
    public @Nonnull Cardinality compute(MultiQueryNode n) {
        return n.getChildren().stream().map(PlanNode::getCardinality)
                              .reduce(adder).orElse(Cardinality.UNSUPPORTED);
    }
}
