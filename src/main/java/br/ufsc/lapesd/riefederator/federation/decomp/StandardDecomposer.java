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
import org.apache.commons.lang3.tuple.ImmutablePair;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.*;

import static java.util.stream.Collectors.toList;

public class StandardDecomposer extends SourcesListAbstractDecomposer {
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
    public @Nonnull String toString() {
        return "StandardDecomposer";
    }
}
