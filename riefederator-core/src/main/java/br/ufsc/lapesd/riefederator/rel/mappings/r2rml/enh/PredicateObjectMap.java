package br.ufsc.lapesd.riefederator.rel.mappings.r2rml.enh;

import javax.annotation.Nonnull;
import java.util.Collection;

public interface PredicateObjectMap extends RRResource {
    @Nonnull Collection<PredicateMap> getPredicateMaps();
    @Nonnull Collection<ObjectMap> getObjectMaps();
}
