package br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh.impl.shortcut;

import br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh.ShortcutPredicateMap;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.exceptions.InvalidRRException;
import org.apache.jena.enhanced.EnhGraph;
import org.apache.jena.enhanced.EnhNode;
import org.apache.jena.enhanced.Implementation;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.impl.ModelCom;

import javax.annotation.Nonnull;

public class ShortcutPredicateMapImpl extends ShortcutMapImpl implements ShortcutPredicateMap {
    public static @Nonnull final Implementation factory = new Implementation() {
        @Override
        public EnhNode wrap(Node node, EnhGraph eg) {
            if (!canWrap(node, eg))
                throw new InvalidRRException(node, eg.asGraph(), ShortcutPredicateMap.class);
            return new ShortcutPredicateMapImpl(node, eg, eg.getNodeAs(node, Resource.class));
        }

        @Override
        public boolean canWrap(Node node, EnhGraph eg) {
            return node.isURI();
        }
    };

    public ShortcutPredicateMapImpl(Node n, EnhGraph m, @Nonnull RDFNode constant) {
        super(n, m, constant);
    }
    public ShortcutPredicateMapImpl(RDFNode n, @Nonnull RDFNode constant) {
        super(n.asNode(), (ModelCom)n.getModel(), constant);
    }
}
