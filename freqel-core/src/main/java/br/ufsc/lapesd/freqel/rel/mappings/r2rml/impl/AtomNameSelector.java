package br.ufsc.lapesd.freqel.rel.mappings.r2rml.impl;

import br.ufsc.lapesd.freqel.rel.mappings.r2rml.RR;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh.PredicateMap;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.exceptions.InvalidRRException;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Collections.singleton;

@NotThreadSafe
public class AtomNameSelector {
    private static final Logger logger = LoggerFactory.getLogger(AtomNameSelector.class);

    private final @Nonnull Multimap<Resource, TriplesMapContext> cls2context;
    private final @Nonnull Multimap<String, TriplesMapContext> table2context;
    private final @Nonnull Set<String> uniquePredicates;
    private final Map<String, Integer> uses = new HashMap<>();
    private final Map<TriplesMapContext, String> nameCache = new HashMap<>();

    public AtomNameSelector(@Nonnull Collection<TriplesMapContext> allContexts) {
        cls2context = HashMultimap.create();
        table2context = HashMultimap.create();

        Map<String, Integer> predicateCounter = new HashMap<>();
        for (TriplesMapContext ctx : allContexts) {
            ctx.getRoot().streamClasses().forEach(cls -> cls2context.put(cls, ctx));
            table2context.put(ctx.getTable(), ctx);
            for (PredicateObjectContext po : ctx.getPredicateObjects()) {
                for (PredicateMap pm : po.getRoot().getPredicateMaps()) {
                    RDFNode node = pm.getConstant();
                    if (node != null && node.isURIResource()) {
                        String uri = node.asResource().getURI();
                        predicateCounter.put(uri, predicateCounter.getOrDefault(uri, 0)+1);
                    }
                }
            }
        }

        uniquePredicates = new HashSet<>();
        for (Map.Entry<String, Integer> e : predicateCounter.entrySet()) {
            assert e.getValue() > 0;
            if (e.getValue() < 2)
                uniquePredicates.add(e.getKey());
        }
    }


    private @Nonnull String getSuffixed(@Nonnull String base) {
        int id = uses.computeIfAbsent(base, k -> 0) + 1;
        uses.put(base, id);
        return base + "-" + id;
    }

    private @Nonnull String compute(TriplesMapContext ctx) {
        TreeSet<String> identifying = ctx.getRoot().streamClasses()
                .filter(cls -> cls2context.get(cls).equals(singleton(ctx)))
                .map(Resource::getURI)
                .collect(Collectors.toCollection(TreeSet::new));
        if (identifying.size() > 1) {
            logger.info("{} classes identify TriplesMap {}, chose {} by alphabetic order.",
                    identifying.size(), ctx.getRoot(), identifying.iterator().next());
        }
        if (!identifying.isEmpty())
            return identifying.iterator().next();
        // try unique tableName
        if (table2context.get(ctx.getTable()).size() == 1)
            return ctx.getTable();
        // try non-identifying class adding a -{counter} suffix
        Resource first = ctx.getRoot().streamClasses().sorted().findFirst().orElse(null);
        if (first != null)
            return getSuffixed(first.getURI());
        // fallback to Table-{counter}
        return getSuffixed(ctx.getTable());
    }

    public @Nonnull String get(TriplesMapContext ctx) {
        return nameCache.computeIfAbsent(ctx, this::compute);
    }

    public @Nonnull String get(@Nonnull TriplesMapContext ctx,
                               @Nonnull PredicateObjectPairContext po) {
        RDFNode constant = po.getPredicate().getRoot().getConstant();
        if (constant == null) {
            return getSuffixed(get(ctx));
        } else if (!constant.isURIResource()) {
            throw new InvalidRRException(po.getPredicate().getRoot(), RR.constant,
                        "predicate map has a rr:constant value that is not an IRI");
        }
        String uri = constant.asResource().getURI();
        return uniquePredicates.contains(uri) ? uri : getSuffixed(uri);
    }
}
