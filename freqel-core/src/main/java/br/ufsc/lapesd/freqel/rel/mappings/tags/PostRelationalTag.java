package br.ufsc.lapesd.freqel.rel.mappings.tags;

import br.ufsc.lapesd.freqel.description.molecules.tags.HybridTag;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;

@Immutable
public class PostRelationalTag implements HybridTag {
    public static final @Nonnull PostRelationalTag INSTANCE = new PostRelationalTag();

    @Override public @Nonnull String shortDisplayName() {
        return "postRelational";
    }
}
