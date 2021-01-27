package br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh.impl;

import br.ufsc.lapesd.freqel.rel.mappings.r2rml.RR;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh.BaseTableOrView;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.exceptions.InvalidRRException;
import org.apache.jena.enhanced.EnhGraph;
import org.apache.jena.enhanced.EnhNode;
import org.apache.jena.enhanced.Implementation;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.impl.ResourceImpl;

import javax.annotation.Nonnull;

public class BaseTableOrViewImpl extends ResourceImpl implements BaseTableOrView {
    public static final Implementation factory = new ImplementationByProperties(RR.tableName) {
        @Override
        public EnhNode wrap(Node node, EnhGraph eg) {
            if (!canWrap(node, eg))
                throw new InvalidRRException(node, eg.asGraph(), BaseTableOrView.class);
            return new BaseTableOrViewImpl(node, eg);
        }
    };

    public BaseTableOrViewImpl(Node n, EnhGraph m) {
        super(n, m);
    }

    @Override
    public @Nonnull String getTableName() {
        return RRUtils.getStringNN(this, RR.tableName);
    }
}
