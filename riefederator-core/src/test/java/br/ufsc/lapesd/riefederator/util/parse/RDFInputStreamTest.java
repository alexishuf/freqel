package br.ufsc.lapesd.riefederator.util.parse;

import br.ufsc.lapesd.riefederator.TestContext;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.RDF;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.io.*;
import java.util.List;
import java.util.stream.Stream;

import static br.ufsc.lapesd.riefederator.jena.JenaWrappers.toJena;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;

@Test(groups = {"fast"})
public class RDFInputStreamTest extends RDFSourceAbstractTest implements TestContext {

    private @Nonnull Model createDummyModel() {
        Model m = ModelFactory.createDefaultModel();
        m.setNsPrefix("rdf", RDF.getURI());
        m.setNsPrefix("foaf", FOAF.getURI());
        m.add(toJena(Alice), RDF.type, FOAF.Person);
        return m;
    }

    @DataProvider public @Nonnull Object[][] guessSyntaxData() throws IOException {
        List<List<Object>> lists = asList(
                asList("", RDFSyntax.TTL),
                asList("<a> <" + type + "> <" + Person + "> .", RDFSyntax.NT),
                asList("<a> <" + type + "> <" + Person + ">.", RDFSyntax.NT),
                asList("<" + Alice + "> <" + type + "> <" + Person + ">.", RDFSyntax.NT),
                asList("_:Alice <" + type + "> <" + Person + ">.", RDFSyntax.NT),
                asList("_:Alice <" + type + "> <" + Person + ">.", RDFSyntax.NT),
                asList("[] <" + type + "> <" + Person + ">.", RDFSyntax.TTL),
                asList("[ <" + type + "> <" + Person + "> ] .", RDFSyntax.TTL),
                asList("@prefix rdf: <" + RDF.getURI() + "> .\n" +
                        "[ rdf:type <" + Person + "> ] .", RDFSyntax.TTL),
                asList("@prefix foaf: <" + FOAF.getURI() + "> .\n" +
                        "[ a foaf:Person ] .", RDFSyntax.TTL),
                asList("@prefix foaf: <" + FOAF.getURI() + "> .\n" +
                        "_:Alice a foaf:Person .", RDFSyntax.TTL),
                asList("@base <" + EX + "> .\n" +
                        "<Alice> a foaf:Person .", RDFSyntax.TTL)
        );
        Model m = createDummyModel();
        for (Lang lang : RDFLanguages.getRegisteredLanguages()) {
            RDFSyntax syntax = RDFSyntax.fromJenaLang(lang);
            if (syntax == null)
                syntax = RDFSyntax.UNKNOWN;
            try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
                RDFDataMgr.write(os, m, lang);
                lists.add(asList(os.toString("UTF-8"), syntax));
            } catch (RuntimeException ignored) {}
        }

        return lists.stream().map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "guessSyntaxData")
    public void testGuessSyntax(@Nonnull String contents, @Nonnull RDFSyntax expected) {
        ByteArrayInputStream is = new ByteArrayInputStream(contents.getBytes(UTF_8));
        RDFInputStream ris = new RDFInputStream(is);
        assertEquals(ris.getSyntaxOrGuess(), expected);
    }

    @DataProvider public @Nonnull Object[][] guessHDTSyntaxData() throws Exception {
        Model m = createDummyModel();
        File hdt = createTempFile(".hdt");
        File hdtIndexed = createTempFile(".hdt");
        File hdtImplicit = createTempFile("");
        File hdtImplicitIndexed = createTempFile("");

        saveHDT(hdt, createHDT(m)).close();
        saveIndexedHDT(hdtIndexed, createHDT(m)).close();
        saveHDT(hdtImplicit, createHDT(m)).close();
        saveIndexedHDT(hdtImplicitIndexed, createHDT(m)).close();

        return Stream.of(hdt, hdtIndexed, hdtImplicit, hdtImplicitIndexed)
                     .map(file -> new Object[] {file, RDFSyntax.HDT}).toArray(Object[][]::new);
    }

    @Test(dataProvider = "guessHDTSyntaxData")
    public void testGuessHDTSyntax(@Nonnull File file,
                                   @Nonnull RDFSyntax expected) throws IOException {
        try (FileInputStream inputStream = new FileInputStream(file)) {
            RDFInputStream ris = new RDFInputStream(inputStream);
            assertEquals(ris.getSyntaxOrGuess(), expected);
        }
    }
}