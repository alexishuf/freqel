package br.ufsc.lapesd.riefederator.query;

import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.RDF;

import javax.annotation.Nonnull;

import static br.ufsc.lapesd.riefederator.model.term.std.StdLit.fromUnescaped;

public class EndpointTestBase {
    public static final @Nonnull StdURI ALICE = new StdURI("http://example.org/Alice");
    public static final @Nonnull StdURI BOB = new StdURI("http://example.org/Bob");
    public static final @Nonnull StdURI CHARLIE = new StdURI("http://example.org/Charlie");
    public static final @Nonnull StdURI DAVE = new StdURI("http://example.org/Dave");
    public static final @Nonnull StdURI TYPE = new StdURI(RDF.type.getURI());
    public static final @Nonnull StdURI KNOWS = new StdURI(FOAF.knows.getURI());
    public static final @Nonnull StdURI AGE = new StdURI(FOAF.age.getURI());
    public static final @Nonnull StdURI NAME = new StdURI(FOAF.name.getURI());
    public static final @Nonnull StdURI PERSON = new StdURI(FOAF.Person.getURI());
    public static final @Nonnull StdURI INT = new StdURI(XSDDatatype.XSDint.getURI());
    public static final @Nonnull StdLit A_AGE = fromUnescaped("23", INT);
    public static final @Nonnull StdLit A_NAME = fromUnescaped("alice", "en");
    public static final @Nonnull StdLit B_NAME1 = fromUnescaped("bob", "en");
    public static final @Nonnull StdLit B_NAME2 = fromUnescaped("beto", "pt");
    public static final @Nonnull StdVar X = new StdVar("X");
    public static final @Nonnull StdVar S = new StdVar("S");
    public static final @Nonnull StdVar P = new StdVar("P");
    public static final @Nonnull StdVar O = new StdVar("O");

    public static class Fixture<T extends TPEndpoint> implements AutoCloseable {
        public final @Nonnull T endpoint;

        public Fixture(@Nonnull T endpoint) {
            this.endpoint = endpoint;
        }

        @Override
        public void close() { }
    }
}
