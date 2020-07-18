package br.ufsc.lapesd.riefederator.rel.mappings.r2rml.enh.impl;

import br.ufsc.lapesd.riefederator.rel.mappings.r2rml.RR;
import br.ufsc.lapesd.riefederator.rel.mappings.r2rml.enh.*;
import br.ufsc.lapesd.riefederator.rel.mappings.r2rml.exceptions.InvalidRRException;
import org.apache.jena.enhanced.EnhGraph;
import org.apache.jena.enhanced.EnhNode;
import org.apache.jena.enhanced.Implementation;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.rdf.model.impl.ResourceImpl;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Spliterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Spliterators.spliteratorUnknownSize;

public class TriplesMapImpl extends ResourceImpl implements TriplesMap {
    private static final @Nonnull Logger logger = LoggerFactory.getLogger(TriplesMap.class);

    public static @Nonnull final Implementation factory =
            new ImplementationByProperties(RR.logicalTable) {
        @Override
        public EnhNode wrap(Node node, EnhGraph eg) {
            if (!canWrap(node, eg))
                throw new InvalidRRException(node, eg.asGraph(), TriplesMap.class);
            return new TriplesMapImpl(node, eg);
        }
    };

    public TriplesMapImpl(Node n, EnhGraph m) {
        super(n, m);
    }

    @Override
    public @Nonnull SubjectMap getSubjectMap() {
        return RRUtils.getResourceNN(this, RR.subjectMap).as(SubjectMap.class);
    }

    @Override
    public @Nonnull Iterator<PredicateObjectMap> getPredicateObjectMapIterator() {
        StmtIterator stmtIt = listProperties(RR.predicateObjectMap);
        return new Iterator<PredicateObjectMap>() {
            private @Nullable PredicateObjectMap current = null;

            @Override
            public boolean hasNext() {
                while (current == null && stmtIt.hasNext()) {
                    RDFNode o = stmtIt.next().getObject();
                    if (o.canAs(PredicateObjectMap.class)) {
                        current = o.as(PredicateObjectMap.class);
                    } else {
                        logger.warn("{} rr:predicateObjectMap {} is not a PredicateObjectMap",
                                    this, o);
                    }
                }
                return current != null;
            }

            @Override
            public @Nonnull PredicateObjectMap next() {
                if (!hasNext()) throw new NoSuchElementException();
                assert  current != null;
                PredicateObjectMap result = current;
                current = null;
                return result;
            }
        };
    }

    @Override
    public @Nonnull Iterable<PredicateObjectMap> getPredicateObjectMap() {
        return new Iterable<PredicateObjectMap>() {
            @Override
            public @Nonnull Iterator<PredicateObjectMap> iterator() {
                return getPredicateObjectMapIterator();
            }
        };
    }

    @Override
    public @Nonnull Stream<PredicateObjectMap> streamPredicateObjectMap() {
        int chars = Spliterator.NONNULL | Spliterator.IMMUTABLE | Spliterator.DISTINCT;
        return StreamSupport.stream(
                spliteratorUnknownSize(getPredicateObjectMapIterator(), chars), false);
    }

    @Override
    public @Nonnull LogicalTable getLogicalTable() {
        return RRUtils.getResourceNN(this, RR.logicalTable).as(LogicalTable.class);
    }

    @Override
    public @Nonnull Stream<Resource> streamClasses() {
        return Stream.concat(
                getSubjectMap().streamClasses(),
                streamPredicateObjectMap()
                        .flatMap(po -> po.getPredicateMaps().stream()
                                .filter(pm -> Objects.equals(pm.getConstant(), RDF.type))
                                .flatMap(pm -> po.getObjectMaps().stream()
                                        .map(TermMap::getConstant)
                                        .filter(c -> c != null && c.isURIResource()))
                                .map(RDFNode::asResource))
        ).distinct();
    }
}
