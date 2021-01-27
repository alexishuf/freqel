package br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh;

import javax.annotation.Nonnull;
import java.util.Collection;

public interface PredicateObjectMap extends RRResource {
    @Nonnull Collection<PredicateMap> getPredicateMaps();
    @Nonnull Collection<ObjectMap> getObjectMaps();
}
