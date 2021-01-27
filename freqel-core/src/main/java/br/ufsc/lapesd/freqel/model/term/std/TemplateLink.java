package br.ufsc.lapesd.freqel.model.term.std;

import br.ufsc.lapesd.freqel.model.prefix.PrefixDict;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.model.term.Var;
import br.ufsc.lapesd.freqel.query.CQuery;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import java.util.Set;

@Immutable
public class TemplateLink extends StdURI {
    private final @Nonnull CQuery template;
    private final @Nonnull Term subject, object;

    public TemplateLink(@Nonnull String uri, @Nonnull CQuery template,
                        @Nonnull Term sub, @Nonnull Term obj) {
        super(uri);
        Set<Var> vars = template.attr().allVars();
        if (!(sub instanceof Var) || !vars.contains(sub.asVar()))
            throw new IllegalArgumentException("Subject "+sub+" missing from template "+template);
        if (!(obj instanceof Var) || !vars.contains(obj.asVar()))
            throw new IllegalArgumentException("Object " +obj+" missing from template "+template);
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
