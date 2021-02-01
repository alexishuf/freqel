package br.ufsc.lapesd.freqel;

import br.ufsc.lapesd.freqel.model.term.Lit;
import br.ufsc.lapesd.freqel.model.term.URI;
import br.ufsc.lapesd.freqel.model.term.Var;
import br.ufsc.lapesd.freqel.model.term.std.StdLit;
import br.ufsc.lapesd.freqel.model.term.std.StdPlain;
import br.ufsc.lapesd.freqel.model.term.std.StdURI;
import br.ufsc.lapesd.freqel.model.term.std.StdVar;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.OWL2;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;

import javax.annotation.Nonnull;

public interface TestContext {
    @Nonnull String EX = "http://example.org/";
    @Nonnull String DBO = "http://dbpedia.org/ontology/";
    @Nonnull String DUL = "http://www.ontologydesignpatterns.org/ont/dul/DUL.owl#";

    @Nonnull URI Alice   = new StdURI(EX+"Alice");
    @Nonnull URI Alice2  = new StdURI(EX+"Alice-2");
    @Nonnull URI Bob     = new StdURI(EX+"Bob");
    @Nonnull URI Charlie = new StdURI(EX+"Charlie");
    @Nonnull URI Dave    = new StdURI(EX+"Dave");

    @Nonnull URI Class1    = new StdURI(EX+"Class1");
    @Nonnull URI Class2    = new StdURI(EX+"Class2");
    @Nonnull URI Class3    = new StdURI(EX+"Class3");

    @Nonnull URI type = new StdURI(RDF.type.getURI());
    @Nonnull URI sameAs = new StdURI(OWL2.sameAs.getURI());

    @Nonnull URI subPropertyOf = new StdURI(RDFS.subPropertyOf.getURI());

    @Nonnull URI xsdInt     = new StdURI(XSDDatatype.XSDint.getURI());
    @Nonnull URI xsdInteger = new StdURI(XSDDatatype.XSDinteger.getURI());
    @Nonnull URI xsdDecimal = new StdURI(XSDDatatype.XSDdecimal.getURI());
    @Nonnull URI xsdDouble = new StdURI(XSDDatatype.XSDdouble.getURI());
    @Nonnull URI xsdBoolean = new StdURI(XSDDatatype.XSDboolean.getURI());
    @Nonnull URI xsdString  = new StdURI(XSDDatatype.XSDstring.getURI());
    @Nonnull URI xsdDate  = new StdURI(XSDDatatype.XSDdate.getURI());

    default @Nonnull StdPlain plain(@Nonnull String local) {
        return new StdPlain(local);
    }

    default @Nonnull Lit lit(int value) {
        return StdLit.fromUnescaped(String.valueOf(value), xsdInt);
    }
    default @Nonnull Lit integer(int value) {
        return StdLit.fromEscaped(String.valueOf(value), xsdInteger);
    }
    default @Nonnull Lit lit(double value) {
        return StdLit.fromEscaped(String.valueOf(value), xsdDouble);
    }
    default @Nonnull Lit lit(String value) {
        return StdLit.fromUnescaped(String.valueOf(value), xsdString);
    }
    default @Nonnull Lit lit(String value, String langTag) {
        return StdLit.fromUnescaped(String.valueOf(value), langTag);
    }
    default @Nonnull Lit date(String iso) {
        return StdLit.fromEscaped(iso, xsdDate);
    }

    @Nonnull URI Person   = new StdURI(FOAF.Person.getURI());
    @Nonnull URI Document = new StdURI(FOAF.Document.getURI());
    @Nonnull URI Professor = new StdURI(EX+"Professor");
    @Nonnull URI University = new StdURI(EX+"University");
    @Nonnull URI Paper = new StdURI(EX+"Paper");
    @Nonnull URI Authorship = new StdURI(EX+"Authorship");

    @Nonnull URI knows            = new StdURI(FOAF.knows.getURI());
    @Nonnull URI name             = new StdURI(FOAF.name.getURI());
    @Nonnull URI made            = new StdURI(FOAF.made.getURI());
    @Nonnull URI mbox             = new StdURI(FOAF.mbox.getURI());
    @Nonnull URI primaryTopic     = new StdURI(FOAF.primaryTopic.getURI());
    @Nonnull URI isPrimaryTopicOf = new StdURI(FOAF.isPrimaryTopicOf.getURI());
    @Nonnull URI age              = new StdURI(FOAF.age.getURI());
    @Nonnull URI foafTitle        = new StdURI(FOAF.title.getURI());

    @Nonnull URI Consumer = new StdURI(EX+"Consumer");

    @Nonnull URI idEx = new StdURI(EX+"id");
    @Nonnull URI p1            = new StdURI(EX+"p1");
    @Nonnull URI p2            = new StdURI(EX+"p2");
    @Nonnull URI p3            = new StdURI(EX+"p3");
    @Nonnull URI p4            = new StdURI(EX+"p4");
    @Nonnull URI p5            = new StdURI(EX+"p5");
    @Nonnull URI p6            = new StdURI(EX+"p6");
    @Nonnull URI p7            = new StdURI(EX+"p7");
    @Nonnull URI p8            = new StdURI(EX+"p8");
    @Nonnull URI p9            = new StdURI(EX+"p9");
    @Nonnull URI titleEx       = new StdURI(EX+"title");
    @Nonnull URI nameEx        = new StdURI(EX+"name");
    @Nonnull URI mainName      = new StdURI(EX+"mainName");
    @Nonnull URI authorName    = new StdURI(EX+"authorName");
    @Nonnull URI ageEx         = new StdURI(EX+"age");
    @Nonnull URI university    = new StdURI(EX+"university");
    @Nonnull URI university_id = new StdURI(EX+"university_id");
    @Nonnull URI supervisor    = new StdURI(EX+"supervisor");
    @Nonnull URI paper_id      = new StdURI(EX+"paper_id");
    @Nonnull URI author_id     = new StdURI(EX+"author_id");
    @Nonnull URI likes      = new StdURI(EX+"likes");
    @Nonnull URI bornIn     = new StdURI(EX+"bornIn");
    @Nonnull URI author     = new StdURI(EX+"author");
    @Nonnull URI isAuthorOf = new StdURI(EX+"isAuthorOf");
    @Nonnull URI mainAuthor = new StdURI(EX+"mainAuthor");
    @Nonnull URI cites      = new StdURI(EX+"cites");
    @Nonnull URI manages    = new StdURI(EX+"manages");
    @Nonnull URI title      = new StdURI(EX+"title");
    @Nonnull URI genre      = new StdURI(EX+"genre");
    @Nonnull URI genreName  = new StdURI(EX+"genreName");
    @Nonnull URI result     = new StdURI(EX+"result");
    @Nonnull URI total      = new StdURI(EX+"total");

    @Nonnull Var x = new StdVar("x");
    @Nonnull Var y = new StdVar("y");
    @Nonnull Var z = new StdVar("z");
    @Nonnull Var w = new StdVar("w");
    @Nonnull Var u = new StdVar("u");
    @Nonnull Var v = new StdVar("v");
    @Nonnull Var s = new StdVar("s");
    @Nonnull Var p = new StdVar("p");
    @Nonnull Var o = new StdVar("o");

    @Nonnull Var u1 = new StdVar("u1");
    @Nonnull Var v1 = new StdVar("v1");
    @Nonnull Var s1 = new StdVar("s1");
    @Nonnull Var o1 = new StdVar("o1");
    @Nonnull Var u2 = new StdVar("u2");
    @Nonnull Var v2 = new StdVar("v2");
    @Nonnull Var s2 = new StdVar("s2");
    @Nonnull Var o2 = new StdVar("o2");
    @Nonnull Var u3 = new StdVar("u3");
    @Nonnull Var v3 = new StdVar("v3");
    @Nonnull Var s3 = new StdVar("s3");
    @Nonnull Var o3 = new StdVar("o3");

    @Nonnull Var x0 = new StdVar("x0");
    @Nonnull Var x1 = new StdVar("x1");
    @Nonnull Var x2 = new StdVar("x2");
    @Nonnull Var x3 = new StdVar("x3");
    @Nonnull Var x4 = new StdVar("x4");

    @Nonnull Var y1 = new StdVar("y1");
    @Nonnull Var y2 = new StdVar("y2");
    @Nonnull Var y3 = new StdVar("y3");
    @Nonnull Var y4 = new StdVar("y4");

    @Nonnull Var z1 = new StdVar("z1");
    @Nonnull Var z2 = new StdVar("z2");

    @Nonnull Var spHidden0 = new StdVar("parserPathHiddenVar0");
    @Nonnull Var spHidden1 = new StdVar("parserPathHiddenVar1");
    @Nonnull Var spHidden2 = new StdVar("parserPathHiddenVar2");
    @Nonnull Var spHidden3 = new StdVar("parserPathHiddenVar3");
}
