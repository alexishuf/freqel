package br.ufsc.lapesd.riefederator.model.term.std;

import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.query.CQuery;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

@Immutable
public class TemplateLink extends StdURI {
    public static final @Nonnull String NID = "plain";
    public static final @Nonnull String URI_PREFIX = "urn:"+NID+":";

    private final @Nonnull CQuery template;
    private final @Nonnull Term subject, object;

    public TemplateLink(@Nonnull String name, @Nonnull CQuery template,
                        @Nonnull Term sub, @Nonnull Term obj) {
        super(URI_PREFIX+name);
        Set<Term> vars = template.streamTerms(Var.class).collect(Collectors.toSet());
        checkArgument(vars.contains(sub), "Subject "+sub+" missing from template "+template);
        checkArgument(vars.contains(obj), "Object " +obj+" missing from template "+template);
        this.template = template;
        this.subject = sub;
        this.object = obj;
    }

    public @Nonnull CQuery getTemplate() {
        return template;
    }

    public @Nonnull Term getSubject() {
        return subject;
    }

    public @Nonnull Term getObject() {
        return object;
    }

    @Override
    public @Nonnull String toString(@Nonnull PrefixDict dict) {
        return String.format("StdTemplateLink(%s %s %s -> %s)",
                getSubject().toString(dict), super.toString(dict), getObject().toString(dict),
                getTemplate().toString(dict));
    }
}
