package br.ufsc.lapesd.freqel.description.semantic;

import br.ufsc.lapesd.freqel.V;
import br.ufsc.lapesd.freqel.description.MatchReasoning;
import br.ufsc.lapesd.freqel.description.SelectDescription;
import br.ufsc.lapesd.freqel.federation.spec.source.SourceCache;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.exceptions.MissingCapabilityException;
import br.ufsc.lapesd.freqel.reason.tbox.TBox;
import com.esotericsoftware.yamlbeans.YamlReader;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.*;

import static java.nio.charset.StandardCharsets.UTF_8;

public class SemanticSelectDescription extends SelectDescription implements SemanticDescription {
    private final @Nonnull TBox tBox;

    public SemanticSelectDescription(@Nonnull CQEndpoint endpoint, @Nonnull TBox tBox)
            throws MissingCapabilityException {
        super(endpoint);
        this.tBox = tBox;
    }

    public SemanticSelectDescription(@Nonnull CQEndpoint endpoint, boolean fetchClasses, @Nonnull TBox tBox)
            throws MissingCapabilityException {
        super(endpoint, fetchClasses);
        this.tBox = tBox;
    }

    public SemanticSelectDescription(@Nonnull CQEndpoint endpoint, @Nonnull State state, @Nonnull TBox tBox) {
        super(endpoint, state);
        this.tBox = tBox;
    }

    public static @Nullable SelectDescription
    fromCache(@Nonnull CQEndpoint endpoint, @Nonnull SourceCache cache,
              @Nonnull String endpointId, @Nonnull TBox tBox) throws IOException {
        File file = cache.getFile("select-description", endpointId);
        if (file != null)
            return fromYaml(endpoint, file, tBox);
        return null;
    }
    public static @Nonnull SelectDescription
    fromYaml(@Nonnull CQEndpoint endpoint, @Nonnull File file, @Nonnull TBox tBox) throws IOException {
        try (FileInputStream inputStream = new FileInputStream(file)) {
            return fromYaml(endpoint, inputStream, tBox);
        }
    }
    public static @Nonnull SelectDescription
    fromYaml(@Nonnull CQEndpoint endpoint, @Nonnull InputStream inputStream, @Nonnull TBox tBox) throws IOException {
        try (InputStreamReader reader = new InputStreamReader(inputStream, UTF_8)) {
            return fromYaml(endpoint, reader, tBox);
        }
    }
    public static @Nonnull SelectDescription
    fromYaml(@Nonnull CQEndpoint endpoint, @Nonnull Reader reader, @Nonnull TBox tBox) throws IOException {
        YamlReader yamlReader = new YamlReader(reader);
        State state = yamlReader.read(State.class);
        return new SemanticSelectDescription(endpoint, state, tBox);
    }

    @Override protected boolean match(@Nonnull Triple triple, @Nonnull MatchReasoning reasoning) {
        if (reasoning != MatchReasoning.TRANSPARENT)
            return super.match(triple, reasoning);
        assert predicates != null;
        Term p = triple.getPredicate();
        Term o = triple.getObject();
        if (classes != null && p.equals(V.RDF.type) && o.isGround()) {
            return tBox.withSubClasses(o).anyMatch(classes::contains);
        }
        return p.isVar() || tBox.withSubProperties(p).anyMatch(predicates::contains);
    }

    @Override public boolean supports(@Nonnull MatchReasoning mode) {
        return MatchReasoning.NONE.equals(mode) || MatchReasoning.TRANSPARENT.equals(mode);
    }
}
