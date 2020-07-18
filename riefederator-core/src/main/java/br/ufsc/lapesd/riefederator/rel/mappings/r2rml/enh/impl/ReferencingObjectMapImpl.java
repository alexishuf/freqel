package br.ufsc.lapesd.riefederator.rel.mappings.r2rml.enh.impl;

import br.ufsc.lapesd.riefederator.rel.mappings.r2rml.RR;
import br.ufsc.lapesd.riefederator.rel.mappings.r2rml.enh.JoinCondition;
import br.ufsc.lapesd.riefederator.rel.mappings.r2rml.enh.ReferencingObjectMap;
import br.ufsc.lapesd.riefederator.rel.mappings.r2rml.enh.TermType;
import br.ufsc.lapesd.riefederator.rel.mappings.r2rml.enh.TriplesMap;
import br.ufsc.lapesd.riefederator.rel.mappings.r2rml.exceptions.InvalidRRException;
import org.apache.jena.enhanced.EnhGraph;
import org.apache.jena.enhanced.EnhNode;
import org.apache.jena.enhanced.Implementation;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
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

public class ReferencingObjectMapImpl extends ObjectMapImpl implements ReferencingObjectMap {
    private static final Logger logger = LoggerFactory.getLogger(ReferencingObjectMapImpl.class);

    public static @Nonnull final Implementation factory =
            new ImplementationByProperties(RR.parentTriplesMap) {
        @Override
        public EnhNode wrap(Node node, EnhGraph eg) {
            if (!canWrap(node, eg))
                throw new InvalidRRException(node, eg.asGraph(), ReferencingObjectMap.class);
            return new ReferencingObjectMapImpl(node, eg);
        }
    };

    public ReferencingObjectMapImpl(Node n, EnhGraph m) {
        super(n, m, ReferencingObjectMap.class);
    }

    @Override
    public @Nonnull TermType getTermType() {
        TermType type = parseTermType();
        if (type != null && type != TermType.IRI)
            throw new InvalidRRException(this, RR.termType, "Expected null or rr:IRI, got "+type);
        return TermType.IRI;
    }

    @Override
    public @Nullable String getLanguage() {
        String language = super.getLanguage();
        if (language != null)
            throw new InvalidRRException(this, RR.language, "Expected null, got "+language);
        return null;
    }

    @Override
    public @Nullable Resource getDatatype() {
        Resource dt = super.getDatatype();
        if (dt != null)
            throw new InvalidRRException(this, RR.datatype, "Expected null, got "+dt);
        return null;
    }

    @Override
    public @Nonnull TriplesMap getParentTriplesMap() {
        return RRUtils.getResourceNN(this, RR.parentTriplesMap).as(TriplesMap.class);
    }

    @Override
    public @Nonnull Iterable<JoinCondition> getJoinConditions() {
        return new Iterable<JoinCondition>() {
            @Override
            public @Nonnull Iterator<JoinCondition> iterator() {
                return getJoinConditionsIterator();
            }
        };
    }

    @Override
    public @Nonnull Iterator<JoinCondition> getJoinConditionsIterator() {
        StmtIterator stmtIt = listProperties(RR.joinCondition);
        return new Iterator<JoinCondition>() {
            private @Nullable JoinCondition current = null;

            @Override
            public boolean hasNext() {
                while (current == null && stmtIt.hasNext()) {
                    Statement stmt = stmtIt.next();
                    RDFNode o = stmt.getObject();
                    if (o.canAs(JoinCondition.class))
                        current = o.as(JoinCondition.class);
                    else
                        logger.warn("{} rr:joinCondition {} is not a JoinCondition", this, o);
                }
                return current != null;
            }

            @Override
            public @Nonnull JoinCondition next() {
                if (!hasNext()) throw new NoSuchElementException();
                assert current != null;
                JoinCondition joinCondition = current;
                current = null;
                return joinCondition;
            }
        };
    }

    @Override
    public @Nonnull Stream<JoinCondition> streamJoinConditions() {
        int chars = Spliterator.DISTINCT | Spliterator.IMMUTABLE | Spliterator.NONNULL;
        return StreamSupport.stream(
                spliteratorUnknownSize(getJoinConditionsIterator(), chars), false);
    }
}
