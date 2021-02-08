package br.ufsc.lapesd.freqel.description.semantic;

import br.ufsc.lapesd.freqel.description.AskDescription;
import br.ufsc.lapesd.freqel.description.Description;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.freqel.reason.tbox.TBox;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

import static br.ufsc.lapesd.freqel.query.parse.CQueryContext.createQuery;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;

public class SemanticAskDescriptionTest extends SemanticDescriptionTestBase {

    @Override protected @Nonnull List<List<Object>> getBaseData() {
        List<List<Object>> base = new ArrayList<>(super.getBaseData());
        base.addAll(asList(
                asList(createQuery(x, p11, U11), singleton(new Triple(x, p11, U11))), //explicit
                //entail p1 from p11
                asList(createQuery(x, p1, U11), singleton(new Triple(x, p1, U11))),
                asList(createQuery(x, p1, U12), singleton(new Triple(x, p1, U12))),
                // do not entail p1 from p2
                asList(createQuery(x, p1, U2), emptySet()),
                // do not entail p1 from p
                asList(createQuery(x, p1, U0), emptySet())
        ));
        return base;
    }

    @Override
    protected @Nonnull Description createNonSemantic(@Nonnull CQEndpoint ep, boolean fetchClasses) {
        return new AskDescription(ep);
    }

    @Override protected @Nonnull SemanticDescription
    createSemantic(@Nonnull CQEndpoint ep, boolean fetchClasses, @Nonnull TBox tBox) {
        return new SemanticAskDescription(ep, tBox);
    }
}