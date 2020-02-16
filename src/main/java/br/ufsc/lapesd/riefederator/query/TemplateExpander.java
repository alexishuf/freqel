package br.ufsc.lapesd.riefederator.query;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.model.term.std.TemplateLink;
import com.google.errorprone.annotations.CheckReturnValue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TemplateExpander implements Function<CQuery, CQuery> {
    private static final @Nonnull Pattern TPL_RX = Pattern.compile("tpl(\\d+)");

    public static final @Nonnull TemplateExpander INSTANCE = new TemplateExpander();

    @Override
    public @Nonnull CQuery apply(@Nonnull CQuery query) {
        return apply(query, null);
    }

    public @Nonnull CQuery apply(@Nonnull CQuery query, @Nullable int[] lastId) {
        if (query.stream().noneMatch(t -> t.getPredicate() instanceof TemplateLink))
            return query; // no template
        if (lastId == null)
            lastId = new int[]{getLastId(query)};
        CQuery.Builder builder = CQuery.builder(query.size() + 10);
        Map<Term, Term> var2Safe = new HashMap<>();
        for (Triple triple : query) {
            expandTo(query, builder, lastId, var2Safe, triple);
            var2Safe.clear();
        }
        // copy annotations for terms in query triple annotations are copied in expandTo
        query.forEachTermAnnotation(builder::annotate);
        return builder.build();
    }

    private void expandTo(@Nonnull CQuery inQuery, @Nonnull CQuery.Builder builder,
                          @Nonnull int[] lastId, @Nonnull Map<Term, Term> var2Safe,
                          @Nonnull Triple input) {
        assert var2Safe.isEmpty();
        if (!(input.getPredicate() instanceof TemplateLink)) {
            builder.add(input);
            inQuery.getTripleAnnotations(input).forEach(a -> builder.annotate(input, a));
            return;
        }
        TemplateLink templateLink = (TemplateLink) input.getPredicate();
        Term sub = input.getSubject(), obj = input.getObject();
        CQuery tplQuery = apply(templateLink.getTemplate(), lastId);
        for (Triple triple : tplQuery) {
            Term s = getCanon(lastId, var2Safe, triple.getSubject(), templateLink, input);
            Term o = getCanon(lastId, var2Safe, triple.getObject() , templateLink, input);
            Triple rewritten = new Triple(s, triple.getPredicate(), o);

            // add triple and triple annotations (from tplQuery)
            builder.add(rewritten);
            tplQuery.getTripleAnnotations(triple).forEach(a -> builder.annotate(rewritten, a));
        }
        //transfer term annotations from tplQuery
        for (Map.Entry<Term, Term> e : var2Safe.entrySet())
            tplQuery.getTermAnnotations(e.getKey()).forEach(a -> builder.annotate(e.getValue(), a));
    }

    private @Nonnull Term getCanon(@Nonnull int[] lastId, @Nonnull Map<Term, Term> var2Id,
                                   @Nonnull Term tplTerm,
                                   @Nonnull TemplateLink tpl, @Nonnull Triple input) {
        if (!tplTerm.isVar()) return tplTerm;
        if (tplTerm.equals(tpl.getSubject())) return input.getSubject();
        if (tplTerm.equals(tpl.getObject())) return input.getObject();
        return var2Id.computeIfAbsent(tplTerm, x -> new StdVar("tpl" + ++lastId[0]));
    }

    private @Nonnull Integer getLastId(@Nonnull CQuery query) {
        return query.streamTerms(Var.class).map(Var::getName).map(n -> {
            Matcher matcher = TPL_RX.matcher(n);
            if (!matcher.matches()) return null;
            return Integer.parseInt(matcher.group(1));
        }).filter(Objects::nonNull).max(Integer::compareTo).orElse(0);
    }

    @CheckReturnValue
    public static @Nonnull CQuery expandTemplates(@Nonnull CQuery query) {
        return INSTANCE.apply(query);
    }
}
