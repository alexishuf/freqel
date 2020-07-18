package br.ufsc.lapesd.riefederator.rel.mappings.tags;

import br.ufsc.lapesd.riefederator.description.molecules.tags.HybridTag;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;

@Immutable
public class PostRelationalTag implements HybridTag {
    public static final @Nonnull PostRelationalTag INSTANCE = new PostRelationalTag();
}
