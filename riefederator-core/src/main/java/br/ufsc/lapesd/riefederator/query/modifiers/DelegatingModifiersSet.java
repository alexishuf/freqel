package br.ufsc.lapesd.riefederator.query.modifiers;

import javax.annotation.Nonnull;

public class DelegatingModifiersSet extends ModifiersSet {
    protected final @Nonnull ModifiersSet delegate;

    public DelegatingModifiersSet(@Nonnull ModifiersSet delegate, boolean locked) {
        super(delegate, locked);
        this.delegate = delegate;
    }

    @Override
    protected void added(@Nonnull Modifier modifier) {
        delegate.added(modifier);
    }

    @Override
    protected void removed(@Nonnull Modifier modifier) {
        delegate.removed(modifier);
    }
}
