package br.ufsc.lapesd.freqel.cardinality.impl;

import br.ufsc.lapesd.freqel.V;
import br.ufsc.lapesd.freqel.algebra.Cardinality;
import br.ufsc.lapesd.freqel.cardinality.CardinalityHeuristic;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.model.term.std.StdURI;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.util.Bitset;
import br.ufsc.lapesd.freqel.util.bitset.Bitsets;
import br.ufsc.lapesd.freqel.util.bitset.DynamicBitset;
import br.ufsc.lapesd.freqel.util.indexed.FullIndexSet;
import br.ufsc.lapesd.freqel.util.indexed.IndexSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class QuickSelectivityHeuristic implements CardinalityHeuristic {
    private static final Term ground = new StdURI("urn:freqel:ground");
    private static final Term type = V.RDF.type;
    private static final Term sameAs = V.OWL.sameAs;
    private static final ThreadLocal<State> state = ThreadLocal.withInitial(State::new);

    private static final int[] GENERIC_COST = {
                     // SPO free
                  1, // 000 ___
                 50, // 001 __O
                 20, // 010 _P_
                250, // 011 _PO
                500, // 100 S__
             250000, // 101 S_O
             125000, // 110 SP_
            1000000, // 111 SPO
    };

    static int cost(@Nonnull Triple t) {
        return cost(t.getSubject(), t.getPredicate(), t.getObject());
    }

    static int cost(@Nonnull Term s, @Nonnull Term p, @Nonnull Term o) {
        int idx = (s.isGround() ? 0 : 0x4) | (p.isGround() ? 0 : 0x2) | (o.isGround() ? 0 : 0x1);
        int cost = GENERIC_COST[idx];
        if (idx != 0) {
            if (type.equals(p)) {
                if      (idx == 0x1) cost = 20;     //O free, 20 classes per instance
                else if (idx == 0x4) cost = 250000; //S free 500 --> 250k (S_O + 10%)
                else                 cost = 500000; //idx == 0x5 (SO free). 250k -> 500k
            } else if (sameAs.equals(p)) {
                if      (idx == 0x5) cost = 500000; //SO free. 250k -> 500k
                else                 cost = 100;    //S,O free. 500,50 -> 100
            }
        }
        return cost;
    }

    static int join(int a, int b) {
        int sum = a + b;
        return (sum >> 1) + (sum >> 4) + 1; // avg + 6.25% of sum + 1
    }

    @Override
    public @Nonnull Cardinality estimate(@Nonnull CQuery query, @Nullable TPEndpoint endpoint) {
        int size = query.size();
        if (size == 1)
            return Cardinality.guess(cost(query.get(0)));
        return Cardinality.guess(state.get().estimate(query));
    }

    private static class State {
        private int nObjects;
        private float groundObjectsRate;
        private boolean connected;
        private final Bitset openStars = new DynamicBitset(), closedStars = new DynamicBitset(),
                             rootStars = new DynamicBitset();
        private CQuery query;
        private final IndexSet<Term> starCores = new FullIndexSet<>(16);
        private final IndexSet<Term> objects = new FullIndexSet<>(16);
        private final List<Bitset> starTriples = new ArrayList<>(32);
        private final List<Bitset> obj2Stars = new ArrayList<>(32);
        private int[] triple2NextStar = new int[16], starCost = new int[16];

        public int estimate(@Nonnull CQuery query) {
            setQuery(query);
            return queryCost();
        }

        public void setQuery(@Nonnull CQuery query) {
            this.query = query;
            int nTriples = query.size();
            cleanForTriples(nTriples);

            // for every triple, get the star index and set the triple bit in starTriples
            for (int i = 0; i < nTriples; i++) {
                Triple triple = query.get(i);
                int starIdx = starCores.indexOfAdd(triple.getSubject());
                setBit(starTriples, starIdx, i, nTriples);
            }

            // for every object, find the obj -> subj joined star and set the obj2star map
            for (int i = 0; i < nTriples; i++) {
                Triple triple = query.get(i);
                Term subj = triple.getSubject(), obj = triple.getObject();
                int starIdx = starCores.indexOf(obj);
                triple2NextStar[i] = starIdx;
                int objIdx = objects.indexOfAdd(obj);
                setBit(obj2Stars, objIdx, starCores.indexOf(subj), nTriples);
            }
        }

        private void cleanForTriples(int nTriples) {
            if (starCost.length < nTriples)
                starCost = new int[nTriples];
            if (triple2NextStar.length < nTriples)
                triple2NextStar = new int[nTriples];
            Arrays.fill(starCost, 0);
            for (Bitset bitset : starTriples)
                bitset.clear();
            for (Bitset bitset : obj2Stars)
                bitset.clear();
            starCores.clear();
            objects.clear();
        }

        private void setBit(@Nonnull List<Bitset> bitsetsList, int idx, int bit, int bitsetSize) {
            Bitset bitset;
            if (idx >= bitsetsList.size()) {
                assert idx == bitsetsList.size();
                bitsetsList.add(bitset = Bitsets.create(bitsetSize));
            } else {
                bitset = bitsetsList.get(idx);
            }
            bitset.set(bit);
        }

        private boolean fillRootStars(int starIdx) {
            if (closedStars.get(starIdx))
                return false; //abort
            if (!openStars.compareAndSet(starIdx)) {
                rootStars.set(starIdx);
                return true; //cycle
            }
            Bitset triples = starTriples.get(starIdx);
            boolean root = true;
            for (int i = triples.nextSetBit(0); i >= 0; i = triples.nextSetBit(i+1)) {
                Term subj = query.get(i).getSubject();
                int objIdx = objects.indexOf(subj);
                if (objIdx >= 0) {
                    Bitset prevStars = obj2Stars.get(objIdx);
                    for (int j = prevStars.nextSetBit(0); j >= 0; j = prevStars.nextSetBit(j + 1)) {
                        if (j != starIdx) {
                            if (!fillRootStars(j))
                                return false; //abort
                            root = false;
                        }
                    }
                }
            }
            if (root)
                rootStars.set(starIdx);
            return true; //not aborted, maybe marked rootStars
        }

        private void fillRootStars() {
            rootStars.clear();
            closedStars.clear();
            for (int i = 0, nStars = starCores.size(); i < nStars; i++) {
                openStars.clear();
                fillRootStars(i);
                closedStars.or(openStars);
            }
            assert !rootStars.isEmpty();
        }

        public int queryCost() {
            groundObjectsRate = nObjects = 0;
            if (starCores.size() == 1)
                return computeStarCost(0, false);
            int total = 0, count = 0;
            fillRootStars();
            closedStars.clear();
            for (int i = rootStars.nextSetBit(0); i >= 0; i = rootStars.nextSetBit(i+1)) {
                if (closedStars.get(i))
                    continue;
                openStars.clear();
                connected = false;
                int cost = starCost(i, false);
                if (!connected && count > 0) { // bad cartesian product
                    // note: if i == 0, connected = true means cycle and false means nothing
                    // total*penalty grows too quickly and may be unfair
                    total = (total + cost) * 2;
                } else {
                    total = total == 0 ? cost : join(total, cost);
                }
                closedStars.or(openStars);
                ++count;
            }
            return total;
        }

        private int starCost(int starIdx, boolean groundSubject) {
            if (starCost[starIdx] > 0) {
                if (closedStars.get(starIdx))
                    connected = true; //hit a closed star, we do not have a product
                return starCost[starIdx];
            }
            if (!openStars.compareAndSet(starIdx))
                return 0; // intra-star cycle
            int oldNObjects = nObjects;
            float oldGroundObjectsRate = groundObjectsRate;
            groundObjectsRate = nObjects = 0;
            int cost = computeStarCost(starIdx, groundSubject);
            groundObjectsRate = (oldGroundObjectsRate*oldNObjects + groundObjectsRate)
                              / (oldNObjects + nObjects);
            nObjects += oldNObjects;
            return starCost[starIdx] = cost;
        }

        private int computeObjObjAverageCost(int starIdx, Term obj, boolean groundSubject) {
            assert obj != ground;
            int objIdx = objects.indexOf(obj), sum = 0, count = 0;
            assert objIdx >= 0;
            Bitset stars = obj2Stars.get(objIdx);
            boolean nextGroundSubject = groundSubject || obj.isGround();
            for (int i = stars.nextSetBit(0); i >= 0; i = stars.nextSetBit(i+1)) {
                if (i != starIdx) {
                    int cost = starCost(i, nextGroundSubject);
                    if (cost > 0) {
                        sum += cost;
                        ++count;
                    }
                }
            }
            return sum == 0 ? 0 : sum / count + 1;
        }

        private int computeStarCost(int starIdx, boolean groundSubject) {
            Bitset triples = starTriples.get(starIdx);
            assert !triples.isEmpty();
            int total = 0, count = 0, groundObjects = 0;
            for (int i = triples.nextSetBit(0); i >= 0; i = triples.nextSetBit(i+1), ++count) {
                Triple t = query.get(i);
                Term subj = groundSubject ? ground : t.getSubject(), obj = t.getObject();
                if (obj.isGround() && !t.getPredicate().equals(type)) ++groundObjects;

                // compute tailCost from obj-obj or obj-subj joins
                int tailCost = computeObjObjAverageCost(starIdx, obj, groundSubject);
                // compute cost of a obj-subj join (and replace if better than obj-obj)
                int nextStar = triple2NextStar[i];
                if (nextStar >= 0) {
                    boolean nextGroundSubj = groundSubject || t.getSubject().isGround();
                    int cost = starCost(nextStar, nextGroundSubj);
                    if (tailCost == 0 || cost < tailCost) tailCost = cost;
                    if (groundObjectsRate >= 0.1) obj = ground;
                }

                // merge tail and triple cost
                int tripleCost = cost(subj, t.getPredicate(), obj);
                if (tailCost > 0)
                    tripleCost = join(tripleCost, tailCost);
                total += tripleCost;
            }
            assert count > 0;
            // update the rate of ground objects
            groundObjectsRate = (groundObjectsRate*nObjects + groundObjects)/(nObjects + count);
            nObjects += count;
            if (count > 1) {
                // average the cost of each triple in star
                double dCost = total / (double) count;
                // Make larger stars cheaper discounting 2% * (count-1), up until 30%.
                total = (int) Math.ceil(dCost * (1 - Math.min(0.3, 0.04 * (count - 1))));
            }
            assert total > 0;
            return total;
        }

    }

}
