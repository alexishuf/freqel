package br.ufsc.lapesd.freqel.description.semantic;

import br.ufsc.lapesd.freqel.description.Description;
import br.ufsc.lapesd.freqel.description.SelectDescription;
import br.ufsc.lapesd.freqel.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.freqel.reason.tbox.TBox;

import javax.annotation.Nonnull;

public class SemanticSelectDescriptionTest  extends SemanticDescriptionTestBase {
    @Override
    protected @Nonnull Description createNonSemantic(@Nonnull CQEndpoint ep, boolean fetchClasses) {
        return new SelectDescription(ep, fetchClasses);
    }

    @Override protected @Nonnull SemanticSelectDescription
    createSemantic(@Nonnull CQEndpoint ep, boolean fetchClasses, @Nonnull TBox tBox) {
        return new SemanticSelectDescription(ep, fetchClasses, tBox);
    }
}