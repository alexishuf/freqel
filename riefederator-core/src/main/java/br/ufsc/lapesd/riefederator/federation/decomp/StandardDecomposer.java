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
import com.google.common.collect.*;
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
                    List<CQuery> egAlts = alts.stream().map(q -> CQuery.union(q, fixed))
                                              .collect(toList());
                    CQuery matched = CQuery.union(CQuery.from(triplesWithAlts.get(0)), fixed);
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
        SetMultimap<Signature, ProtoQueryNode> sig2pn = HashMultimap.create();
        nodes.forEach(pn -> sig2pn.put(new Signature(pn, query.getSet(), query.getVars()), pn));
        List<Signature> signatures = new ArrayList<>(sig2pn.keySet());
        if (signatures.size() < nodes.size()) {
            logger.debug("Discarded {} nodes due to duplicate  endpoint/triple/inputs signatures",
                    nodes.size() - signatures.size());
        }

        List<ProtoQueryNode> reduced = new ArrayList<>(signatures.size());
        ListMultimap<TPEndpoint, Signature> ep2sig =
                MultimapBuilder.hashKeys().arrayListValues().build();
        BitSet exclusive = getExclusiveSignatures(signatures);
        for (int i = 0, size = signatures.size(); i < size; i++) {
            Signature s = signatures.get(i);
            if (exclusive.get(i) && s.inputs.isEmpty()
                    && !sig2pn.get(s).iterator().next().hasAlternatives()) {
                ep2sig.put(s.endpoint, s);
            } else {
                reduced.add(sig2pn.get(s).iterator().next());
            }
        }

        for (TPEndpoint ep : ep2sig.keySet()) {
            List<Signature> list = ep2sig.get(ep);
            if (list.size() == 1) {
                reduced.add(sig2pn.get(list.get(0)).iterator().next());
            } else if (list.size() > 1) {
                CQuery.Builder b = CQuery.builder();
                for (Signature sig : list) {
                    ProtoQueryNode pn = sig2pn.get(sig).iterator().next();
                    assert !pn.hasAlternatives();
                    CQuery matchedQuery = pn.getMatchedQuery();
                    b.addAll(matchedQuery);
                    b.copyAnnotations(matchedQuery);
                    b.copyModifiers(matchedQuery);
                }
                reduced.add(new ProtoQueryNode(ep, b.build()));
            }
        }
        return reduced;
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
        List<Signature> exclusive = new ArrayList<>(bad.cardinality());
        for (int i = 0, size = list.size(); i < size; i++) {
            if (!bad.get(i))
                exclusive.add(list.get(i));
        }
        bad.flip(0, list.size());
        return bad;
    }

    @Override
    public @Nonnull String toString() {
        return "StandardDecomposer";
    }
}
