package br.ufsc.lapesd.riefederator.rel.mappings.r2rml.enh.impl.shortcut;

import br.ufsc.lapesd.riefederator.rel.mappings.r2rml.enh.ShortcutObjectMap;
import br.ufsc.lapesd.riefederator.rel.mappings.r2rml.exceptions.InvalidRRException;
import org.apache.jena.enhanced.EnhGraph;
import org.apache.jena.enhanced.EnhNode;
import org.apache.jena.enhanced.Implementation;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.impl.ModelCom;

import javax.annotation.Nonnull;

public class ShortcutObjectMapImpl extends ShortcutMapImpl implements ShortcutObjectMap {
    public static @Nonnull final Implementation factory = new Implementation() {
        @Override
        public EnhNode wrap(Node node, EnhGraph eg) {
            if (!canWrap(node, eg))
                throw new InvalidRRException(node, eg.asGraph(), ShortcutObjectMap.class);
            return new ShortcutObjectMapImpl(node, eg, eg.getNodeAs(node, RDFNode.class));
        }

        @Override
        public boolean canWrap(Node node, EnhGraph eg) {
            return true;
        }
    };

    public ShortcutObjectMapImpl(Node n, EnhGraph m, @Nonnull RDFNode constant) {
        super(n, m, constant);
    }
    public ShortcutObjectMapImpl(RDFNode node, @Nonnull RDFNode constant) {
        super(node.asNode(), (ModelCom)node.getModel(), constant);
    }
}
