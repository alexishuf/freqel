package br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh.impl;

import br.ufsc.lapesd.freqel.rel.mappings.r2rml.RR;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh.R2RMLView;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.exceptions.InvalidRRException;
import org.apache.jena.enhanced.EnhGraph;
import org.apache.jena.enhanced.EnhNode;
import org.apache.jena.enhanced.Implementation;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.impl.ResourceImpl;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class R2RMLViewImpl extends ResourceImpl implements R2RMLView {
    public static final Implementation factory = new ImplementationByProperties(RR.sqlQuery) {
        @Override
        public EnhNode wrap(Node node, EnhGraph eg) {
            if (!canWrap(node, eg))
                throw new InvalidRRException(node, eg.asGraph(), R2RMLViewImpl.class);
            return new R2RMLViewImpl(node, eg);
        }
    };

    public R2RMLViewImpl(Node n, EnhGraph m) {
        super(n, m);
    }

    @Override
    public @Nonnull String getSql() {
        return RRUtils.getStringNN(this, RR.sqlQuery);
    }

    @Override
    public @Nullable Resource getSqlVersion() {
        return RRUtils.getURIResource(this, RR.sqlVersion);
    }
}
