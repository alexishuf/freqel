package br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh;

import br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh.impl.*;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh.impl.shortcut.ShortcutObjectMapImpl;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh.impl.shortcut.ShortcutPredicateMapImpl;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.exceptions.InvalidRRException;
import org.apache.jena.enhanced.*;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.sys.JenaSystem;

import javax.annotation.Nonnull;
import java.util.concurrent.atomic.AtomicBoolean;

public class RRFactory {
    private static final AtomicBoolean globallyInstalled = new AtomicBoolean(false);

    private static @Nonnull final Implementation logicalTableFactory = new Implementation() {
        @Override
        public EnhNode wrap(Node node, EnhGraph eg) {
            if (BaseTableOrViewImpl.factory.canWrap(node, eg))
                return BaseTableOrViewImpl.factory.wrap(node, eg);
            if (R2RMLViewImpl.factory.canWrap(node, eg))
                return R2RMLViewImpl.factory.wrap(node, eg);
            throw new InvalidRRException(node, eg.asGraph(), LogicalTable.class);
        }

        @Override
        public boolean canWrap(Node node, EnhGraph eg) {
            return BaseTableOrViewImpl.factory.canWrap(node, eg)
                    || R2RMLViewImpl.factory.canWrap(node, eg);
        }
    };

    private static @Nonnull final Implementation termMapFactory = new Implementation() {
        @Override
        public EnhNode wrap(Node node, EnhGraph eg) {
            if (SubjectMapImpl.factory.canWrap(node, eg))
                return SubjectMapImpl.factory.wrap(node, eg);
            if (PredicateMapImpl.factory.canWrap(node, eg))
                return PredicateMapImpl.factory.wrap(node, eg);
            if (ObjectMapImpl.factory.canWrap(node, eg))
                return ObjectMapImpl.factory.wrap(node, eg);
            throw new InvalidRRException(node, eg.asGraph(), TermMap.class);
        }

        @Override
        public boolean canWrap(Node node, EnhGraph eg) {
            return SubjectMapImpl.factory.canWrap(node, eg)
                    || PredicateMapImpl.factory.canWrap(node, eg)
                    || ObjectMapImpl.factory.canWrap(node, eg);
        }
    };

    private static @Nonnull final Implementation resourceFactory = new Implementation() {
        @Override
        public EnhNode wrap(Node node, EnhGraph eg) {
            if (termMapFactory.canWrap(node, eg))
                return termMapFactory.wrap(node, eg);
            if (PredicateObjectMapImpl.factory.canWrap(node, eg))
                return PredicateObjectMapImpl.factory.wrap(node, eg);
            if (logicalTableFactory.canWrap(node, eg))
                return logicalTableFactory.wrap(node, eg);
            if (JoinConditionImpl.factory.canWrap(node, eg))
                return JoinConditionImpl.factory.wrap(node, eg);
            if (TriplesMapImpl.factory.canWrap(node, eg))
                return TriplesMapImpl.factory.wrap(node, eg);
            throw new IllegalArgumentException(node+" is not a valid RRResource");
        }

        @Override
        public boolean canWrap(Node node, EnhGraph eg) {
            if (termMapFactory.canWrap(node, eg))
                return true;
            if (PredicateObjectMapImpl.factory.canWrap(node, eg))
                return true;
            if (logicalTableFactory.canWrap(node, eg))
                return true;
            if (JoinConditionImpl.factory.canWrap(node, eg))
                return true;
            return TriplesMapImpl.factory.canWrap(node, eg);
        }
    };

    static {
        install();
    }

    public static void install(@Nonnull Personality<RDFNode> p) {
        p.add(BaseTableOrView.class,      BaseTableOrViewImpl.factory);
        p.add(R2RMLView.class,            R2RMLViewImpl.factory);
        p.add(TriplesMap.class,           TriplesMapImpl.factory);
        p.add(SubjectMap.class,           SubjectMapImpl.factory);
        p.add(ObjectMap.class,            ObjectMapImpl.factory);
        p.add(ReferencingObjectMap.class, ReferencingObjectMapImpl.factory);
        p.add(PredicateMap.class,         PredicateMapImpl.factory);
        p.add(PredicateObjectMap.class,   PredicateObjectMapImpl.factory);
        p.add(ShortcutPredicateMap.class, ShortcutPredicateMapImpl.factory);
        p.add(ShortcutObjectMap.class,    ShortcutObjectMapImpl.factory);
        p.add(JoinCondition.class,        JoinConditionImpl.factory);

        p.add(LogicalTable.class, logicalTableFactory);
        p.add(TermMap.class,      termMapFactory);
        p.add(RRResource.class,   resourceFactory);
    }

    public static void install() {
        if (globallyInstalled.compareAndSet(false, true)) {
            JenaSystem.init();
            install(BuiltinPersonalities.model);
        }
    }


}
