package br.ufsc.lapesd.riefederator.rel.sql.impl;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.description.molecules.Molecule;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.std.StdBlank;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.rel.common.SelectorFactory;
import br.ufsc.lapesd.riefederator.rel.common.StarsHelper;
import br.ufsc.lapesd.riefederator.rel.mappings.Column;
import br.ufsc.lapesd.riefederator.rel.mappings.tags.ColumnsTag;
import br.ufsc.lapesd.riefederator.rel.mappings.tags.TableTag;
import br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.*;

public class SqlSelectorFactoryTest implements TestContext {
    private static final Column cu = new Column("T", "cu");
    private static final Column cv = new Column("T", "cv");
    private static final Column co = new Column("T", "co");
    private static final Column ku = new Column("U", "ku");
    private static final Column kv = new Column("U", "kv");

    private static final Atom atomT = Molecule.builder("T").tag(new TableTag("T")).buildAtom();
    private static final Atom atomCu = Molecule.builder("Cu").tag(ColumnsTag.direct(cu)).buildAtom();
    private static final Atom atomCv = Molecule.builder("Cv").tag(ColumnsTag.direct(cv)).buildAtom();
    private static final Atom atomCo = Molecule.builder("Co").tag(ColumnsTag.direct(co)).buildAtom();

    private static final AtomAnnotation aaT = AtomAnnotation.of(atomT);
    private static final AtomAnnotation aaCu = AtomAnnotation.of(atomCu);
    private static final AtomAnnotation aaCv = AtomAnnotation.of(atomCv);
    private static final AtomAnnotation aaCo = AtomAnnotation.of(atomCo);

    private static class DummyContext implements SelectorFactory.Context {
        private @Nonnull final CQuery query;

        private DummyContext(@Nonnull CQuery query) {
            this.query = query;
        }

        @Override
        public @Nonnull CQuery getQuery() {
            return query;
        }

        @Override
        public @Nonnull Collection<Column> getColumns(@Nonnull Term term) {
            return StarsHelper.getColumns(query, null, term);
        }

        @Override
        public @Nullable Column getDirectMapped(@Nonnull Term term, @Nullable Triple triple) {
            Collection<Column> columns = getColumns(term);
            if (columns.size() == 1) return columns.iterator().next();
            return null;
        }
    }

    @Test
    public void testToSelector() {
        StarSqlWriter w = new StarSqlWriter();
        StdBlank blank = new StdBlank();
        CQuery query = createQuery(x, aaT, name, u, aaCu,
                                   x, aaT, age, lit(23), aaCv,
                                   x, aaT, knows, blank, aaCo);
        DummyContext ctx = new DummyContext(query);

        SqlSelectorFactory fac = new SqlSelectorFactory(DefaultSqlTermWriter.INSTANCE);
        assertNull(fac.create(ctx, query.get(0)));

        SqlSelector eqSelector = (SqlSelector) fac.create(ctx, query.asList().get(1));
        assertNotNull(eqSelector);
        assertTrue(eqSelector.hasCondition());
        assertTrue(eqSelector.getCondition().matches("\\(?T.cv = 23\\)?"));
        assertEquals(eqSelector.getColumns(), singletonList(cv));
        assertEquals(eqSelector.getTerms(), singletonList(lit(23)));
        assertEquals(eqSelector.getSparqlVars(), emptySet());

        // blank node translates into IS NOT NULL
        SqlSelector nnSelector = (SqlSelector) fac.create(ctx, query.asList().get(2));
        assertNotNull(nnSelector);
        assertEquals(nnSelector.getColumns(), singletonList(co));
        assertEquals(nnSelector.getTerms(), singletonList(blank));
        assertEquals(nnSelector.getSparqlVars(), emptySet());
        assertTrue(nnSelector.hasCondition());
        assertTrue(nnSelector.getCondition().matches("\\(?T.co IS NOT NULL\\)?"));
    }

    @DataProvider
    public static @Nonnull
    Object[][] filterData() {
        return Stream.of(
                asList(SPARQLFilter.build("regex(?u, \"a.*\")"), null),
                asList(SPARQLFilter.build("?v > 23"), "(T.cv > 23)"),
                asList(SPARQLFilter.build("?v > 23 && ?v < 30"), "((T.cv > 23) AND (T.cv < 30))"),
                asList(SPARQLFilter.build("?v > 23 && (?v < 30 || ?o > 60)"),
                        "((T.cv > 23) AND ((T.cv < 30) OR (T.co > 60)))")
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "filterData")
    public void testFilterSelector(@Nonnull SPARQLFilter filter, @Nullable String expected) {
        CQuery query = createQuery(x, aaT, name, u, aaCu,
                                   x, aaT, age, v, aaCv,
                                   x, aaT, ageEx, o, aaCo);
        SqlSelectorFactory fac = new SqlSelectorFactory(DefaultSqlTermWriter.INSTANCE);
        DummyContext ctx = new DummyContext(query);
        SqlSelector selector =  (SqlSelector) fac.create(ctx, filter);

        if (expected == null) {
            assertNull(selector);
        } else {
            assertNotNull(selector);
            assertEquals(selector.getSparqlVars(), filter.getVarNames());
            assertEquals(new HashSet<>(selector.getTerms()), filter.getVars());

            Set<Column> expectedColumns = filter.getVarNames().stream()
                    .map(n -> new Column("T", "c" + n)).collect(toSet());
            assertEquals(new HashSet<>(selector.getColumns()), expectedColumns);
            assertTrue(selector.hasCondition());
            assertEquals(selector.getCondition(), expected);
        }
    }

}