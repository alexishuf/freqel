package br.ufsc.lapesd.riefederator.rel.mappings.r2rml.impl;

import br.ufsc.lapesd.riefederator.rel.mappings.r2rml.enh.ObjectMap;
import br.ufsc.lapesd.riefederator.rel.mappings.r2rml.enh.PredicateMap;
import br.ufsc.lapesd.riefederator.rel.mappings.r2rml.enh.PredicateObjectMap;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.concurrent.LazyInit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Predicate;

public class PredicateObjectContext {
    private @Nonnull final PredicateObjectMap root;
    private @Nullable @LazyInit ImmutableSet<String> columnNames;

    public PredicateObjectContext(@Nonnull PredicateObjectMap root) {
        this.root = root;
    }

    public @Nonnull PredicateObjectMap getRoot() {
        return root;
    }

    public @Nonnull Iterable<PredicateObjectPairContext> getPairs() {
        return new Iterable<PredicateObjectPairContext>() {
            @Override
            public @Nonnull Iterator<PredicateObjectPairContext> iterator() {
                return getPairsIterator(pm -> true);
            }
        };
    }

    private @Nonnull Iterator<PredicateObjectPairContext>
    getPairsIterator(@Nonnull Predicate<PredicateMap> filter) {
        Iterator<PredicateMap> pIt = root.getPredicateMaps().iterator();
        Collection<ObjectMap> objectMaps = root.getObjectMaps();
        return new Iterator<PredicateObjectPairContext>() {
            private PredicateMap pm = pIt.hasNext() ? pIt.next() : null;
            Iterator<ObjectMap> oIt = objectMaps.iterator();

            @Override
            public boolean hasNext() {
                if (oIt.hasNext()) return true;
                while (pIt.hasNext()) {
                    pm = pIt.next();
                    oIt = objectMaps.iterator();
                    if (filter.test(pm))
                        break; // break here to ensure at least one pIt.next() call
                }
                return oIt.hasNext();
            }

            @Override
            public PredicateObjectPairContext next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                assert pm != null;
                assert oIt.hasNext();
                return new PredicateObjectPairContext(pm, oIt.next());
            }
        };
    }

    public @Nonnull Set<String> getColumnNames() {
        if (columnNames == null) {
            Set<String> names = new HashSet<>();
            for (PredicateObjectPairContext pair : getPairs()) {
                names.addAll(pair.getPredicate().getColumnNames());
                names.addAll(pair.getObject().getColumnNames());
            }
            columnNames = ImmutableSet.copyOf(names);
        }
        return columnNames;
    }
}
