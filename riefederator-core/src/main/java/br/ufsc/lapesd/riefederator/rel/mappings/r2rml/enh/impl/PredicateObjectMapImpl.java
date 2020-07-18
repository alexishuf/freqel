package br.ufsc.lapesd.riefederator.rel.mappings.r2rml.enh.impl;

import br.ufsc.lapesd.riefederator.rel.mappings.r2rml.RR;
import br.ufsc.lapesd.riefederator.rel.mappings.r2rml.enh.*;
import br.ufsc.lapesd.riefederator.rel.mappings.r2rml.exceptions.InvalidRRException;
import org.apache.jena.enhanced.EnhGraph;
import org.apache.jena.enhanced.EnhNode;
import org.apache.jena.enhanced.Implementation;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.rdf.model.impl.ResourceImpl;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;

public class PredicateObjectMapImpl extends ResourceImpl implements PredicateObjectMap {
    public static final @Nonnull Implementation factory = new Implementation() {
        @Override
        public boolean canWrap(Node node, EnhGraph eg) {
            Graph graph = eg.asGraph();
            boolean hasP = graph.contains(node, RR.predicate.asNode(), null)
                    || graph.contains(node, RR.predicateMap.asNode(), null);
            boolean hasO = graph.contains(node, RR.object.asNode(), null)
                    || graph.contains(node, RR.objectMap.asNode(), null);
            return (hasP && hasO) || graph.contains(null, RR.predicateObjectMap.asNode(), node);
        }

        @Override
        public EnhNode wrap(Node node, EnhGraph eg) {
            if (!canWrap(node, eg))
                throw new InvalidRRException(node, eg.asGraph(), PredicateObjectMap.class);
            return new PredicateObjectMapImpl(node, eg);
        }
    };

    public PredicateObjectMapImpl(Node n, EnhGraph m) {
        super(n, m);
    }

    private <T extends RDFNode> void
    addTo(@Nonnull Collection<T> collection, @Nonnull Property property,
          @Nonnull Predicate<RDFNode> condition, @Nonnull Class<? extends T> aClass) {
        StmtIterator it = this.listProperties(property);
        while (it.hasNext()) {
            RDFNode o = it.next().getObject();
            if (condition.test(o))
                collection.add(o.as(aClass));
        }
    }

    @Override
    public @Nonnull Collection<PredicateMap> getPredicateMaps() {
        List<PredicateMap> list = new ArrayList<>();
        addTo(list, RR.predicate, RDFNode::isURIResource, ShortcutPredicateMap.class);
        addTo(list, RR.predicateMap, RDFNode::isResource, PredicateMap.class);
        if (list.isEmpty()) {
            throw new InvalidRRException(this, RR.predicateMap, "No rr:predicate/" +
                                         "predicateMap lead to a resource");
        }
        return list;
    }

    @Override
    public @Nonnull Collection<ObjectMap> getObjectMaps() {
        List<ObjectMap> list = new ArrayList<>();
        addTo(list, RR.object, n -> true, ShortcutObjectMap.class);
        addTo(list, RR.objectMap, RDFNode::isResource, ObjectMap.class);
        if (list.isEmpty()) {
            throw new InvalidRRException(this, RR.objectMap, "No rr:object/" +
                                         "objectMap lead to a resource");
        }
        return list;
    }
}
