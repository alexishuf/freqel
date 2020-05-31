package br.ufsc.lapesd.riefederator.federation.decomp;

import br.ufsc.lapesd.riefederator.description.CQueryMatch;
import br.ufsc.lapesd.riefederator.federation.PerformanceListener;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.Metrics;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.TimeSampler;
import br.ufsc.lapesd.riefederator.federation.planner.Planner;
import br.ufsc.lapesd.riefederator.federation.tree.proto.ProtoQueryNode;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.lang3.tuple.ImmutablePair;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class StandardDecomposer extends SourcesListAbstractDecomposer {
    @Inject
    public StandardDecomposer(@Nonnull Planner planner, @Nonnull PerformanceListener performance) {
        super(planner, performance);
    }

    @Override
    protected @Nonnull List<ProtoQueryNode> decomposeIntoProtoQNs(@Nonnull CQuery query) {
        List<ProtoQueryNode> qns = new ArrayList<>();
        // save known EGs and  map triple -> endpoint
        Multimap<Triple, TPEndpoint> ne2ep = HashMultimap.create();

        try (TimeSampler ignored = Metrics.SELECTION_MS.createThreadSampler(performance)) {
            (sources.size() > 8 ? sources.parallelStream() : sources.stream())
                    .map(src -> ImmutablePair.of(src.getEndpoint(), src.getDescription().match(query)))
                    .forEachOrdered(p -> {
                        CQueryMatch m = p.right;
                        m.getKnownExclusiveGroups().forEach(eg -> qns.add(createPQN(p.left, eg)));
                        m.getNonExclusiveRelevant().forEach(t  -> ne2ep.put(t, p.left));
                    });
        }

        try (TimeSampler ignored = Metrics.AGGLUTINATION_MS.createThreadSampler(performance)) {
            // if a triple occur in only one endpoint, it will be part of an EG, else create a QN
            Multimap<TPEndpoint, Triple> protoEGs = HashMultimap.create();
            for (Triple triple : ne2ep.keySet()) {
                Collection<TPEndpoint> eps = ne2ep.get(triple);
                if (eps.size() == 1)
                    protoEGs.put(eps.iterator().next(), triple);
                else
                    eps.forEach(ep -> qns.add(createPQN(ep, triple)));
            }
            ne2ep.clear();

            // for single-endpoint triples, build the EGs
            for (TPEndpoint ep : protoEGs.keySet())
                qns.add(createPQN(ep, CQuery.from(protoEGs.get(ep))));
            assert qns.stream().distinct().count() == qns.size();
            return qns;
        }
    }

    @Override
    public @Nonnull String toString() {
        return "StandardDecomposer";
    }
}
