package br.ufsc.lapesd.riefederator.rel.mappings.r2rml.enh.impl;

import br.ufsc.lapesd.riefederator.rel.mappings.r2rml.RR;
import br.ufsc.lapesd.riefederator.rel.mappings.r2rml.enh.SubjectMap;
import br.ufsc.lapesd.riefederator.rel.mappings.r2rml.enh.TermType;
import br.ufsc.lapesd.riefederator.rel.mappings.r2rml.exceptions.InvalidRRException;
import org.apache.jena.enhanced.EnhGraph;
import org.apache.jena.enhanced.EnhNode;
import org.apache.jena.enhanced.Implementation;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.StmtIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Spliterators.spliteratorUnknownSize;

public class SubjectMapImpl extends TermMapImpl implements SubjectMap {
    private static final Logger logger = LoggerFactory.getLogger(SubjectMapImpl.class);

    public static final Implementation factory = new ImplementationByObject(RR.subjectMap) {
        @Override
        public EnhNode wrap(Node node, EnhGraph eg) {
            if (!canWrap(node, eg))
                throw new InvalidRRException(node, eg.asGraph(), SubjectMap.class);
            return new SubjectMapImpl(node, eg);
        }
    };

    public SubjectMapImpl(Node n, EnhGraph m) {
        super(n, m, SubjectMap.class);
    }

    @Override
    public @Nonnull TermType getTermType() {
        TermType type = parseTermType();
        if (type == TermType.Literal) {
            throw new InvalidRRException(this, RR.termType,
                    "Bad rr:termType: "+type+". rr:Literal is not allowed for subject maps");
        }
        return type != null ? type : TermType.IRI;
    }

    @Override
    public @Nonnull Iterator<Resource> getClassesIterator() {
        StmtIterator stmtIt = getModel().listStatements(this, RR.rrClass, (RDFNode) null);
        return new Iterator<Resource>() {
            private @Nullable Resource current = null;

            @Override
            public boolean hasNext() {
                while (stmtIt.hasNext() && current == null) {
                    RDFNode obj = stmtIt.next().getObject();
                    if (!obj.isURIResource())
                        logger.warn("Non-URI object of rr:class for {}: {}", this, obj);
                    else
                        current = obj.asResource();
                }
                return current != null;
            }

            @Override
            public @Nonnull Resource next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                assert this.current != null;
                Resource resource = this.current;
                this.current = null;
                return resource;
            }
        };
    }

    @Override
    public @Nonnull Iterable<Resource> getClasses() {
        return new Iterable<Resource>() {
            @Override
            public @Nonnull Iterator<Resource> iterator() {
                return getClassesIterator();
            }
        };
    }

    @Override
    public @Nonnull Stream<Resource> streamClasses() {
        int chars = Spliterator.DISTINCT | Spliterator.NONNULL | Spliterator.IMMUTABLE;
        Iterator<Resource> it = getClassesIterator();
        return StreamSupport.stream(spliteratorUnknownSize(it, chars), false);
    }
}
