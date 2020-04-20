package br.ufsc.lapesd.riefederator.linkedator.strategies;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.description.molecules.Molecule;
import br.ufsc.lapesd.riefederator.federation.Source;
import br.ufsc.lapesd.riefederator.linkedator.LinkedatorResult;
import br.ufsc.lapesd.riefederator.linkedator.strategies.impl.AtomSignature;
import br.ufsc.lapesd.riefederator.model.term.URI;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.model.term.std.StdPlain;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.model.term.std.TemplateLink;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.SimplePath;
import br.ufsc.lapesd.riefederator.webapis.TransparencyService;
import br.ufsc.lapesd.riefederator.webapis.TransparencyServiceTestContext;
import br.ufsc.lapesd.riefederator.webapis.WebAPICQEndpoint;
import br.ufsc.lapesd.riefederator.webapis.description.APIMolecule;
import br.ufsc.lapesd.riefederator.webapis.requests.impl.UriTemplateExecutor;
import com.google.common.collect.Sets;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.OWL2;
import org.apache.jena.vocabulary.RDF;
import org.glassfish.jersey.uri.UriTemplate;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static br.ufsc.lapesd.riefederator.jena.JenaWrappers.fromJena;
import static br.ufsc.lapesd.riefederator.query.TemplateExpander.expandTemplates;
import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.testng.Assert.*;

public class APIMoleculeInputsLinkedatorStrategyTest implements TestContext,
        TransparencyServiceTestContext {
    private static final Molecule Person, Book, ISBN;
    private static final Molecule Contract, Procurement;
    private static final APIMolecule ContractById, ProcurementByNumber;

    static {
        Person = Molecule.builder("Person")
                .out(name, new Atom("name"))
                .out(mbox, new Atom("email"))
                .out(age, new Atom("age"))
                .exclusive().closed().build();
        Atom PersonHandle = Molecule.builder("PersonHandle")
                .out(mbox, new Atom("email"))
                .buildAtom();
        Book = Molecule.builder("Book")
                .out(author, PersonHandle)
                .in(isAuthorOf, PersonHandle)
                .out(title, new Atom("title"))
                .out(genre, Molecule.builder("Genre")
                        .out(genreName, new Atom("genreName")).buildAtom())
                .exclusive().closed().build();
        ISBN = Molecule.builder("ISBN")
                .out(author, Person.getCore())
                .out(title, new Atom("title"))
                .exclusive().closed().build();

        Atom DimCompra = Molecule.builder("DimCompra")
                .out(numero, new Atom("DimCompra/numero"))
                .out(objeto, new Atom(("DimCompra/objeto"))).buildAtom();
        Atom ModalidadeCompra = Molecule.builder("ModalidadeCompra")
                .out(descricao, new Atom("ModalidadeCompra/descricao"))
                .out(codigo, new Atom("ModalidadeCompra/codigo")).buildAtom();
        Atom OrgaoVinculado = Molecule.builder("OrgaoVinculado")
                .out(codigoSIAFI, new Atom("OrgaoVinculado/codigoSIAFI"))
                .out(nome, new Atom("OrgaoVinculado/nome")).buildAtom();
        Atom UnidadeGestora = Molecule.builder("UnidadeGestora")
                .out(codigo, new Atom("UnidadeGestora/codigo"))
                .out(nome, new Atom("UnidadeGestora/nome"))
                .out(orgaoVinculado, OrgaoVinculado).buildAtom();


        Contract = Molecule.builder("Contrato")
                .out(contrato, Molecule.builder("DimContrato")
                        .out(numero, new Atom("DimContrato/numero"))
                        .buildAtom())
                .out(dataInicioVigencia, new Atom("Contrato/dataInicioVigencia"))
                .out(dataFimVigencia, new Atom("Contrato/dataFimVigencia"))
                .out(dimCompra, DimCompra)
                .out(id, new Atom("Contrato/id"))
                .out(modalidadeCompra, ModalidadeCompra)
                .out(unidadeGestora, UnidadeGestora)
                .exclusive().closed().build();
        Procurement = Molecule.builder("Licitacao")
                .out(dataAbertura, new Atom("Licitacao/dataAbertura"))
                .out(id, new Atom("Licitacao/id"))
                .out(licitacao, DimCompra)
                .out(modalidadeLicitacao, ModalidadeCompra)
                .out(unidadeGestora, UnidadeGestora)
                .exclusive().closed().build();

        Map<String, String> el2in = new HashMap<>();
        UriTemplateExecutor executor;
        executor = UriTemplateExecutor.from(new UriTemplate(EX + "/contract/{id}"))
                                      .withRequired("id").build();
        el2in.put("Contract/id", "id");
        ContractById = new APIMolecule(Contract, executor, el2in);

        executor = UriTemplateExecutor.from(new UriTemplate(
                EX+"/procurement/{?codUG,codMod,numero}")
        ).withRequired("codUG", "codMod", "numero").build();
        el2in.clear();
        el2in.put("UnidadeGestora/codigo", "codUG");
        el2in.put("ModalidadeCompra/codigo", "codMod");
        el2in.put("DimCompra/numero", "numero");
        ProcurementByNumber = new APIMolecule(Procurement, executor, el2in);
    }

    @DataProvider
    public static Object[][] localNameData() {
        return Stream.of(
                asList(FOAF.name.getURI(), "name"),
                asList(RDF.type.getURI(), "type"),
                asList(EX+"/dir/subdir/onto#name", "name"),
                asList(EX+"/dir/subdir/onto.ttl#name", "name"),
                asList(EX+"/dir/subdir/onto.ttl#name/subname", "name/subname"),
                asList("Book", "Book")
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "localNameData")
    public void testLocalName(String uri, String local) {
        assertEquals(APIMoleculeInputsLinkedatorStrategy.localName(uri), local);
    }

    @DataProvider
    public static Object[][] prefixData() {
        return Stream.of(
                asList(FOAF.name.getURI(), FOAF.NS),
                asList(RDF.type.getURI(), RDF.getURI()),
                asList(EX+"/dir/subdir/onto#name", EX+"/dir/subdir/onto#"),
                asList(EX+"/dir/subdir/onto.ttl#name", EX+"/dir/subdir/onto.ttl#"),
                asList(EX+"/dir/subdir/onto.ttl#name/subname", EX+"/dir/subdir/onto.ttl#"),
                asList("Book", StdPlain.URI_PREFIX)
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "prefixData")
    public void testPrefix(String uri, String prefix) {
        assertEquals(APIMoleculeInputsLinkedatorStrategy.prefix(uri), prefix);
    }

    @DataProvider
    public static Object[][] commonPrefixData() {
        return Stream.of(
                asList(singleton(FOAF.name.getURI()), FOAF.NS),
                asList(singleton("Book"), StdPlain.URI_PREFIX),
                asList(Sets.newHashSet(FOAF.name.getURI(), RDF.type.getURI(), FOAF.age.getURI(),
                                       "Book"),
                       FOAF.NS),
                asList(Sets.newHashSet(FOAF.name.getURI(), RDF.type.getURI(),
                                       new StdPlain("Person").getURI(), "Book"),
                        StdPlain.URI_PREFIX)
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "commonPrefixData")
    public void testFindMostCommonPrefix(Set<String> uris, String expected) {
        assertEquals(APIMoleculeInputsLinkedatorStrategy.findMostCommonPrefix(uris), expected);
    }

    @DataProvider
    public static @Nonnull Object[][] createSuggestionData() {
        Map<String, SimplePath> map = new HashMap<>();
        map.put("email", SimplePath.to(mbox).build());
        AtomSignature in1 = new AtomSignature(Book, "PersonHandle", SimplePath.to(author).build(),
                                              singleton("email"), map);
        AtomSignature out1 = new AtomSignature(Person, "Person", SimplePath.EMPTY, emptySet(), map);

        map.clear();
        map.put("email", SimplePath.to(author).to(mbox).build());
        map.put("title", SimplePath.to(title).build());
        AtomSignature in2 = new AtomSignature(ISBN, "ISBN", SimplePath.EMPTY,
                Sets.newHashSet("email", "title"), map);
        AtomSignature out2 = new AtomSignature(Book, "Book", SimplePath.EMPTY, emptySet(), map);

        Var tpl1 = new StdVar("tpl1");
        Var tpl2 = new StdVar("tpl2");

        return Stream.of(
                asList(in1, out1, fromJena(OWL2.sameAs),
                       createQuery(x, mbox, tpl1, y, author, tpl2, tpl2, mbox, tpl1)),
                asList(in2, out2, new StdURI(EX+"hasISBN"),
                       null) //null since hashes change and test would break
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "createSuggestionData")
    public void testCreateSuggestion(AtomSignature inSig, AtomSignature outSig,
                                     URI relation, CQuery expected) {
        APIMoleculeInputsLinkedatorStrategy strategy = new APIMoleculeInputsLinkedatorStrategy();
        LinkedatorResult result = strategy.createSuggestion(inSig, outSig);
        TemplateLink template = result.getTemplateLink();
        assertEquals(template, relation);
        CQuery expanded = expandTemplates(createQuery(x, template, y));
        if (expected != null)
            assertEquals(expanded, expected);
        assertNotNull(expanded);
    }

    @DataProvider
    public static @Nonnull Object[][] getSuggestionsData() {
        Source contractById = new WebAPICQEndpoint(ContractById).asSource();
        Source procurementByNumber = new WebAPICQEndpoint(ProcurementByNumber).asSource();
        return Stream.of(
                asList(asList(contractById, procurementByNumber),
                       singleton(new StdPlain("hasLicitacao")))
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "getSuggestionsData")
    public void testGetSuggestions(Collection<Source> sources,
                                   Collection<URI> expectedRelations) {
        APIMoleculeInputsLinkedatorStrategy strategy = new APIMoleculeInputsLinkedatorStrategy();
        Collection<LinkedatorResult> suggestions = strategy.getSuggestions(sources);
        Set<URI> relations = suggestions.stream().map(r -> r.getTemplateLink().asURI())
                                        .collect(Collectors.toSet());
        assertTrue(relations.containsAll(expectedRelations));
    }

    @Test
    public void testTransparencyServices() throws IOException {
        WebTarget target = ClientBuilder.newClient().target("http://example.org");
        WebAPICQEndpoint contracts = TransparencyService.getContractsClient(target);
        WebAPICQEndpoint contractsById = TransparencyService.getContractByIdClient(target);
        WebAPICQEndpoint procurements = TransparencyService.getProcurementsClient(target);
        WebAPICQEndpoint procurementsByNumber = TransparencyService.getProcurementByNumberClient(target);
        WebAPICQEndpoint procurementsById = TransparencyService.getProcurementsByIdClient(target);
        WebAPICQEndpoint organizations = TransparencyService.getOrgaosSiafiClient(target);
        List<Source> sources = Stream.of(
                contracts, contractsById, procurements, procurementsById, procurementsByNumber
        ).map(WebAPICQEndpoint::asSource).collect(Collectors.toList());
        APIMoleculeInputsLinkedatorStrategy strategy = new APIMoleculeInputsLinkedatorStrategy();
        Collection<LinkedatorResult> suggestions = strategy.getSuggestions(sources);
        Set<URI> relations = suggestions.stream().map(r -> r.getTemplateLink().asURI())
                                        .collect(Collectors.toSet());
        assertTrue(relations.contains(new StdPlain("hasLicitacao")));
        assertFalse(suggestions.isEmpty());
    }

}