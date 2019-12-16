package br.ufsc.lapesd.riefederator.model;

import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.Capability;
import br.ufsc.lapesd.riefederator.query.modifiers.Modifier;
import br.ufsc.lapesd.riefederator.query.modifiers.ModifierUtils;
import br.ufsc.lapesd.riefederator.query.modifiers.Projection;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import static br.ufsc.lapesd.riefederator.query.Capability.ASK;
import static br.ufsc.lapesd.riefederator.query.Capability.PROJECTION;

public class SPARQLString {
    public enum Type {
        ASK, SELECT
    }

    static final @Nonnull Pattern SPARQL_VAR_NAME = Pattern.compile("^[a-zA-Z_0-9\\-]+$");
    private final @Nonnull Type type;
    private final @Nonnull String string;
    private final @Nonnull Set<String> varNames;

    public SPARQLString(@Nonnull Collection<Triple> triples, @Nonnull PrefixDict dict) {
        this(triples, dict, ImmutableList.of());
    }

    public SPARQLString(@Nonnull Collection<Triple> triples, @Nonnull PrefixDict dict,
                        @Nonnull Collection<Modifier> modifiers) {
        Preconditions.checkArgument(!triples.isEmpty(), "triples cannot be empty");
        // find var names
        varNames = new HashSet<>(triples.size() * 2);
        for (Triple triple : triples)
            triple.forEach(t -> {if (t.isVar()) varNames.add(t.asVar().getName());});

        // add prefixes
        StringBuilder b = new StringBuilder(triples.size()*32);
        dict.forEach((name, uri) -> {
            if (SPARQL_VAR_NAME.matcher(name).matches())
                b.append("@prefix ").append(name).append(": <").append(uri).append("> .\n");
        });
        if (b.length() > 0) b.append('\n');

        // add query command
        type = varNames.isEmpty() || ModifierUtils.getFirst(ASK, modifiers) != null
                ? Type.ASK : Type.SELECT;
        if (type == Type.ASK) {
            b.append("ASK {\n");
        } else {
            b.append("SELECT");
            if (ModifierUtils.getFirst(Capability.DISTINCT, modifiers) != null)
                b.append(" DISTINCT");
            Projection project = (Projection)ModifierUtils.getFirst(PROJECTION, modifiers);
            if (project != null) {
                for (String name : project.getVarNames()) {
                    if (varNames.contains(name))
                        b.append(" ?").append(name);
                }
            } else {
                for (String name : varNames) b.append(" ?").append(name);
            }
            b.append(" WHERE {\n");
        }

        // add BGP
        for (Triple triple : triples) {
            triple.forEach(t -> b.append(term2SPARQL(t, dict)).append(" "));
            b.append(".\n");
        }
        this.string = b.append("}\n").toString();
    }


    static @Nonnull String term2SPARQL(@Nonnull Term t, @Nonnull PrefixDict dict) {
        if (t.isBlank()) {
            String name = t.asBlank().getName();
            return name != null && SPARQL_VAR_NAME.matcher(name).matches() ? "_:"+name : "[]";
        } else if (t.isVar()) {
            String name = t.asVar().getName();
            Preconditions.checkArgument(SPARQL_VAR_NAME.matcher(name).matches(),
                    name+" cannot be used as a SPARQL variable name");
            return "?"+name;
        } else if (t.isLiteral()) {
            return RDFUtils.toTurtle(t.asLiteral(), dict);
        } else if (t.isURI()) {
            return RDFUtils.toTurtle(t.asURI(), dict);
        }
        throw new IllegalArgumentException("Cannot represent "+t+" in SPARQL");
    }

    public @Nonnull Type getType() {
        return type;
    }
    public @Nonnull String getString() {
        return string;
    }
    public @Nonnull Set<String> getVarNames() {
        return varNames;
    }

    @Override
    public @Nonnull String toString() {
        return string;
    }
}
