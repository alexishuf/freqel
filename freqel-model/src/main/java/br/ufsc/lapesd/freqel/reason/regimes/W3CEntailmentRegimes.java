package br.ufsc.lapesd.freqel.reason.regimes;

import br.ufsc.lapesd.freqel.V;
import br.ufsc.lapesd.freqel.model.term.std.StdURI;
import br.ufsc.lapesd.freqel.reason.rules.RuleSet;
import br.ufsc.lapesd.freqel.reason.rules.RuleSetRepresentation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableSet;

public class W3CEntailmentRegimes {
    private static final @Nonnull Map<String, EntailmentRegime> iri2regime;
    private static final @Nonnull Map<String, StdURI> iri2graph;
    public static final @Nonnull String NS = "http://www.w3.org/ns/entailment/";

    public static @Nullable EntailmentRegime getByIRI(@Nonnull String iri) {
        return iri2regime.getOrDefault(iri, null);
    }

    public static class W3CRegime extends EntailmentRegime {
        public W3CRegime(@Nonnull String iri, @Nonnull String name, @Nonnull String description,
                         @Nullable String profileIRI,
                         @Nonnull Collection<RuleSetRepresentation> representations,
                         @Nullable RuleSet ruleSet) {
            super(iri, name, description, profileIRI, representations, ruleSet);
        }

        public static @Nonnull W3CRegimeBuilder builder() {
            return new W3CRegimeBuilder();
        }

        public static class W3CRegimeBuilder extends EntailmentRegimeBuilder {
            @Override public @Nonnull W3CRegimeBuilder iri(@Nullable String iri) { super.iri(iri); return this; }
            @Override public @Nonnull W3CRegimeBuilder name(@Nullable String name) { super.name(name); return this; }
            @Override public @Nonnull W3CRegimeBuilder description(@Nullable String description) { super.description(description); return this; }
            @Override public @Nonnull W3CRegimeBuilder profileIRI(@Nullable String profileIRI) { super.profileIRI(profileIRI); return this; }
            @Override public @Nonnull W3CRegimeBuilder rulesRepresentation(@Nonnull Object mediaType, @Nonnull String content) { super.rulesRepresentation(mediaType, content); return this; }
            @Override public @Nonnull W3CRegimeBuilder rulesRepresentation(@Nonnull RuleSetRepresentation ruleSetRepresentation) { super.rulesRepresentation(ruleSetRepresentation); return this; }
            @Override public @Nonnull W3CRegimeBuilder ruleSet(@Nullable RuleSet ruleSet) { super.ruleSet(ruleSet); return this; }

            @Override public @Nonnull W3CRegime build() {
                return new W3CRegime(getIri(), getName(), getDescription(), profileIRI,
                                     unmodifiableSet(ruleSetRepresentationRepresentations),
                                     ruleSet);
            }
        }

        public @Nonnull StdURI getGraphTerm() {
            StdURI term = iri2graph.get(iri());
            if (term == null) // static initializer block is broken!
                throw new RuntimeException("Built-in W3C EntailmentRegime has no graph IRI!");
            return term;
        }
        public @Nonnull String getGraphIRI() { return getGraphTerm().getURI(); }
    }

    public static @Nonnull W3CRegime SIMPLE = W3CRegime.builder()
            .iri(NS+"Simple").name("Simple")
            .description("Only subgraphs are entailed").build();
    public static @Nonnull W3CRegime RDF = W3CRegime.builder()
            .iri(NS+"RDF").name("RDF")
            .description("Subgraphs and RDF axioms are entailed. Does not include D-entailment").build();
    public static @Nonnull W3CRegime RDFS = W3CRegime.builder()
            .iri(NS+"RDFS").name("RDFS")
            .description("RDF axioms and RDFS rules. Does not include D-entailment").build();
    public static @Nonnull W3CRegime D = W3CRegime.builder()
            .iri(NS+"D").name("Datatype entailment")
            .description("Reasons whether two lexical forms represent the same value or even with distinct datatypes").build();
    /*
     * This is seldom useful on a SPARQL mediator, as one would ned to guaranteee the
     * union of all sources constitutes an OWL 2 DL ontology
     */
    public static @Nonnull W3CRegime OWL_DIRECT = W3CRegime.builder()
            .iri(NS+"OWL-Direct").name("OWL Direct")
            .description("Direct semantics entailment over sctrictly OWL 2 DL ontologies").build();
    public static @Nonnull W3CRegime OWL_RDF_BASED = W3CRegime.builder()
            .iri(NS+"OWL-RDF-Based").name("OWL RDF-Based")
            .description("RDF-Based entailment for OWL 2").build();
    public static @Nonnull W3CRegime RIF = W3CRegime.builder()
            .iri(NS+"RIF").name("RIF")
            .description("Entailment according to a set of RIF rules").build();

    static {
        Map<String, EntailmentRegime> i2r = new HashMap<>();
        Map<String, StdURI> i2t = new HashMap<>();
        for (Field field : W3CEntailmentRegimes.class.getFields()) {
            if (!Modifier.isStatic(field.getModifiers()))
                continue;
            if (!EntailmentRegime.class.isAssignableFrom(field.getType()))
                continue;
            try {
                EntailmentRegime regime = (EntailmentRegime) field.get(null);
                i2r.put(regime.iri(), regime);
                i2t.put(regime.iri(), new StdURI(V.Freqel.Entailment.Graph.NS+regime.name()));
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Cannot read public static fields from "+W3CEntailmentRegimes.class);
            }
        }
        iri2regime = i2r;
        iri2graph = i2t;
    }

}
