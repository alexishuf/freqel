package br.ufsc.lapesd.riefederator;

import br.ufsc.lapesd.riefederator.model.term.Lit;
import br.ufsc.lapesd.riefederator.model.term.URI;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;

import javax.annotation.Nonnull;

public interface TestContext {
    @Nonnull URI Alice   = new StdURI("http://example.org/Alice");
    @Nonnull URI Alice2   = new StdURI("http://example.org/Alice-2");
    @Nonnull URI Bob     = new StdURI("http://example.org/Bob");
    @Nonnull URI Charlie = new StdURI("http://example.org/Charlie");
    @Nonnull URI Dave    = new StdURI("http://example.org/Dave");

    @Nonnull URI type = new StdURI(RDF.type.getURI());

    @Nonnull URI subPropertyOf = new StdURI(RDFS.subPropertyOf.getURI());

    @Nonnull URI xsdInt    = new StdURI(XSDDatatype.XSDint.getURI());
    @Nonnull URI xsdString = new StdURI(XSDDatatype.XSDstring.getURI());

    default @Nonnull Lit lit(int value) {
        return StdLit.fromUnescaped(String.valueOf(value), xsdInt);
    }
    default @Nonnull Lit lit(String value) {
        return StdLit.fromUnescaped(String.valueOf(value), xsdString);
    }

    @Nonnull URI Person   = new StdURI(FOAF.Person.getURI());
    @Nonnull URI Document = new StdURI(FOAF.Document.getURI());

    @Nonnull URI knows        = new StdURI(FOAF.knows.getURI());
    @Nonnull URI name         = new StdURI(FOAF.name.getURI());
    @Nonnull URI primaryTopic = new StdURI(FOAF.primaryTopic.getURI());
    @Nonnull URI age          = new StdURI(FOAF.age.getURI());

    @Nonnull URI Consumer = new StdURI("http://example.org/Consumer");

    @Nonnull URI p1         = new StdURI("http://example.org/p1");
    @Nonnull URI p2         = new StdURI("http://example.org/p2");
    @Nonnull URI p3         = new StdURI("http://example.org/p3");
    @Nonnull URI p4         = new StdURI("http://example.org/p4");
    @Nonnull URI p5         = new StdURI("http://example.org/p5");
    @Nonnull URI p6         = new StdURI("http://example.org/p6");
    @Nonnull URI p7         = new StdURI("http://example.org/p7");
    @Nonnull URI p8         = new StdURI("http://example.org/p8");
    @Nonnull URI p9         = new StdURI("http://example.org/p9");
    @Nonnull URI nameEx     = new StdURI("http://example.org/name");
    @Nonnull URI mainName   = new StdURI("http://example.org/mainName");
    @Nonnull URI authorName = new StdURI("http://example.org/authorName");
    @Nonnull URI likes      = new StdURI("http://example.org/likes");
    @Nonnull URI bornIn     = new StdURI("http://example.org/bornIn");
    @Nonnull URI author     = new StdURI("http://example.org/author");
    @Nonnull URI mainAuthor = new StdURI("http://example.org/mainAuthor");
    @Nonnull URI cites      = new StdURI("http://example.org/cites");
    @Nonnull URI manages    = new StdURI("http://example.org/manages");
    @Nonnull URI title      = new StdURI("http://example.org/title");
    @Nonnull URI genre      = new StdURI("http://example.org/genre");
    @Nonnull URI genreName  = new StdURI("http://example.org/genreName");
    @Nonnull URI result     = new StdURI("http://example.org/result");
    @Nonnull URI total      = new StdURI("http://example.org/total");

    @Nonnull Var x = new StdVar("x");
    @Nonnull Var y = new StdVar("y");
    @Nonnull Var z = new StdVar("z");
    @Nonnull Var w = new StdVar("w");
    @Nonnull Var u = new StdVar("u");
    @Nonnull Var v = new StdVar("v");
    @Nonnull Var s = new StdVar("s");
    @Nonnull Var p = new StdVar("p");
    @Nonnull Var o = new StdVar("o");

}
