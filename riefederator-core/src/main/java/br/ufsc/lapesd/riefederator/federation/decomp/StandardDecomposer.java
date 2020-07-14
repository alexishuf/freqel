package br.ufsc.lapesd.riefederator.federation.decomp;

import br.ufsc.lapesd.riefederator.description.CQueryMatch;
import br.ufsc.lapesd.riefederator.description.Description;
import br.ufsc.lapesd.riefederator.description.semantic.SemanticCQueryMatch;
import br.ufsc.lapesd.riefederator.description.semantic.SemanticDescription;
import br.ufsc.lapesd.riefederator.federation.PerformanceListener;
import br.ufsc.lapesd.riefederator.federation.Source;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.Metrics;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.TimeSampler;
import br.ufsc.lapesd.riefederator.federation.planner.Planner;
import br.ufsc.lapesd.riefederator.federation.tree.TreeUtils;
import br.ufsc.lapesd.riefederator.federation.tree.proto.ProtoQueryNode;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.*;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

public class StandardDecomposer extends SourcesListAbstractDecomposer {
    private static final Logger logger = LoggerFactory.getLogger(StandardDecomposer.class);

    @Inject
    public StandardDecomposer(@Nonnull Planner planner, @Nonnull PerformanceListener performance) {
        super(planner, performance);
    }

    private static  @Nonnull ImmutablePair<TPEndpoint, CQueryMatch>
    match(@Nonnull Source source, @Nonnull CQuery query) {
        Description description = source.getDescription();
        if (description instanceof SemanticDescription) {
            SemanticDescription semanticDescription = (SemanticDescription) description;
            return ImmutablePair.of(source.getEndpoint(), semanticDescription.semanticMatch(query));
        }
        return ImmutablePair.of(source.getEndpoint(), description.match(query));
    }

    @Override
    protected @Nonnull List<ProtoQueryNode> decomposeIntoProtoQNs(@Nonnull CQuery query) {
        List<ProtoQueryNode> qns = new ArrayList<>();
        // save known EGs and  map triple -> endpoint
        Multimap<Triple, TPEndpoint> ne2ep = HashMultimap.create();
        Map<ImmutablePair<Triple, TPEndpoint>, Set<CQuery>> ne2alts = new HashMap<>();

        try (TimeSampler ignored = Metrics.SELECTION_MS.createThreadSampler(performance)) {
            (sources.size() > 8 ? sources.parallelStream() : sources.stream())
                    .map(src -> match(src, query))
                    .forEachOrdered(p -> {
                        CQueryMatch m = p.right;
                        if (m instanceof SemanticCQueryMatch) {
                            SemanticCQueryMatch sm = (SemanticCQueryMatch) m;
                            for (CQuery eg : sm.getKnownExclusiveGroups())
                                qns.add(new ProtoQueryNode(p.left, eg, sm.getAlternatives(eg)));
                            for (Triple t : sm.getNonExclusiveRelevant()) {
                                ne2ep.put(t, p.left);
                                Set<CQuery> alternatives = sm.getAlternatives(t);
                                if (alternatives.size() > 1)
                                    ne2alts.put(ImmutablePair.of(t, p.left), alternatives);
                            }
                        } else {
                            m.getKnownExclusiveGroups()
                                    .forEach(eg -> qns.add(new ProtoQueryNode(p.left, eg)));
                            m.getNonExclusiveRelevant().forEach(t -> ne2ep.put(t, p.left));
                        }
                    });
        }

        try (TimeSampler ignored = Metrics.AGGLUTINATION_MS.createThreadSampler(performance)) {
            // if a triple occur in only one endpoint, it will be part of an EG, else create a QN
            Multimap<TPEndpoint, Triple> protoEGs = HashMultimap.create();
            for (Triple triple : ne2ep.keySet()) {
                Collection<TPEndpoint> eps = ne2ep.get(triple);
                if (eps.size() == 1) {
                    protoEGs.put(eps.iterator().next(), triple);
                } else {
                    for (TPEndpoint ep : eps) {
                        Set<CQuery> alts = ne2alts.get(ImmutablePair.of(triple, ep));
                        alts = alts == null ? Collections.emptySet() : alts;
                        qns.add(new ProtoQueryNode(ep, CQuery.from(triple), alts));
                    }
                }
            }
            ne2ep.clear();

            // for single-endpoint triples, build the EGs
            for (TPEndpoint ep : protoEGs.keySet()) {
                Collection<Triple> triples = protoEGs.get(ep);
                List<Triple> triplesWithAlts = triples.stream()
                        .filter(t -> {
                            Set<CQuery> set = ne2alts.get(ImmutablePair.of(t, ep));
                            return set != null && set.size() > 1;
                        })
                        .collect(toList());
                if (triplesWithAlts.isEmpty()) { // no special work to do ...
                    qns.add(new ProtoQueryNode(ep, CQuery.from(triples)));
                } else if (triplesWithAlts.size() == 1) {
                    // a single triple has alternatives, uses those to build the
                    // alternatives by combining them with the other triples which are fixed
                    Set<CQuery> alts = ne2alts.get(ImmutablePair.of(triplesWithAlts.get(0), ep));
                    triples.remove(triplesWithAlts.get(0));
                    CQuery fixed = CQuery.from(triples);
                    List<CQuery> egAlts = alts.stream().map(q -> CQuery.merge(q, fixed))
                                              .collect(toList());
                    CQuery matched = CQuery.merge(CQuery.from(triplesWithAlts.get(0)), fixed);
                    qns.add(new ProtoQueryNode(ep, matched, egAlts));
                } else {
                    // Chaos ensues if we try to to build a single node for the EG
                    // (factorial explosion). Build a node for each member that has
                    // alternatives and a single node for all that have none
                    for (Triple triple : triplesWithAlts) {
                        qns.add(new ProtoQueryNode(ep, CQuery.from(triple),
                                                   ne2alts.get(ImmutablePair.of(triple, ep))));
                    }
                    Set<Triple> safeTriples = TreeUtils.setMinus(triples, triplesWithAlts);
                    if (!safeTriples.isEmpty())
                        qns.add(new ProtoQueryNode(ep, CQuery.from(safeTriples)));
                }
            }
            assert qns.stream().distinct().count() == qns.size();
            return qns;
        }
    }

    @Override
    protected @Nonnull List<ProtoQueryNode> minimizeQueryNodes(@Nonnull CQuery query,
                                                               @Nonnull List<ProtoQueryNode> nodes) {
        SetMultimap<Signature, ProtoQueryNode> sig2pn = groupAndDedup(query, nodes);
        if (logger.isDebugEnabled() && sig2pn.values().size() < nodes.size()) {
            logger.debug("Discarded {} nodes due to duplicate  endpoint/triple/inputs signatures",
                    nodes.size() - sig2pn.values().size());
        }

        List<Signature> signatures = new ArrayList<>(sig2pn.keySet());
        List<ProtoQueryNode> reduced = new ArrayList<>(signatures.size());
        Map<TPEndpoint, List<ProtoQueryNode>> ep2pn = new HashMap<>();
        BitSet exclusive = getExclusiveSignatures(signatures);
        for (int i = 0, size = signatures.size(); i < size; i++) {
            Signature s = signatures.get(i);
            if (!exclusive.get(i) || !s.inputs.isEmpty()) {
                reduced.addAll(sig2pn.get(s));
            } else {
                for (ProtoQueryNode pn : sig2pn.get(s)) {
                    if (!pn.hasAlternatives())
                        ep2pn.computeIfAbsent(s.endpoint, k -> new LinkedList<>()).add(pn);
                    else
                        reduced.add(pn);
                }
            }
        }

        for (Map.Entry<TPEndpoint, List<ProtoQueryNode>> e : ep2pn.entrySet()) {
            List<ProtoQueryNode> list = e.getValue();
            while (!list.isEmpty()) {
                if (!tryMerge(list))
                    reduced.add(list.remove(0));
            }
        }
        return reduced;
    }

    private boolean tryMerge(@Nonnull List<ProtoQueryNode> list) {
        if (list.size() <= 1)
            return false;
        boolean hasMerge = false;
        Iterator<ProtoQueryNode> it = list.iterator();
        ProtoQueryNode left = it.next();
        assert !left.hasAlternatives();
        while (it.hasNext()) {
            ProtoQueryNode right = it.next();
            assert right.getEndpoint().equals(left.getEndpoint());
            assert !right.hasAlternatives();
            CQuery merged = MergeHelper.tryMerge(left.getMatchedQuery(), right.getMatchedQuery());
            if (merged != null) {
                hasMerge = true;
                left = new ProtoQueryNode(left.getEndpoint(), merged);
                it.remove();
            }
        }
        if (hasMerge)
            list.set(0, left); //update first element with merged
        return hasMerge;

    }

    private @Nonnull BitSet getExclusiveSignatures(@Nonnull List<Signature> list) {
        BitSet bad = new BitSet();
        int chunks = Runtime.getRuntime().availableProcessors();
        int chunkSize = list.size()/chunks;
        IntStream.range(0, chunks).parallel().forEach(chunk -> {
            BitSet partial = new BitSet(), tmp = new BitSet();
            int last = chunk == chunks-1 ? list.size() : chunkSize*(chunk+1);
            for (int i = chunkSize*chunk; i < last; i++) {
                BitSet mine = list.get(i).triples;
                for (int j = 0; j < i; j++) {
                    tmp.clear();
                    tmp.or(mine);
                    tmp.and(list.get(j).triples);
                    if (!tmp.isEmpty()) {
                        partial.set(i);
                        partial.set(j);
                    }
                }
            }
            synchronized (bad) {
                bad.or(partial);
            }
        });
        bad.flip(0, list.size());
        return bad;
    }

    @Override
    public @Nonnull String toString() {
        return "StandardDecomposer";
    }
}
