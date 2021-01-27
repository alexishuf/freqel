package br.ufsc.lapesd.freqel.rel.mappings.r2rml;

import br.ufsc.lapesd.freqel.rel.common.RelationalTermParser;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh.TermMap;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh.TermType;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.exceptions.RRMappingException;
import br.ufsc.lapesd.freqel.rel.mappings.r2rml.exceptions.RRTemplateException;
import br.ufsc.lapesd.freqel.rel.sql.impl.NaturalSqlTermParser;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.vocabulary.XSD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class RRTemplate {
    private static final Logger logger = LoggerFactory.getLogger(RRTemplate.class);
    private static final Pattern RX_SCHEME = Pattern.compile("^[a-zA-Z][a-zA-z0-9+.\\-]*:");
    private static final Pattern RX_VAR = Pattern.compile("(?<!\\\\)\\{((?:[^\\\\}]|\\\\.)+)}");
    private static final Pattern RX_BRACE = Pattern.compile("\\\\([{}])");

    private final @Nonnull String rawTemplate;
    private final @Nonnull String template;
    private final @Nonnull LinkedHashMap<String, Segment> placeholders;
    private final ImmutableList<String> orderedColumnNames;
    private final ImmutableSet<String> columnNames;

    public RRTemplate(@Nonnull String template) throws RRTemplateException {
        this.placeholders = new LinkedHashMap<>();
        this.rawTemplate = template;
        Matcher matcher = RX_VAR.matcher(template);
        while (matcher.find()) {
            String name = matcher.group(1).replaceAll("\\\\([{}])", "$1");
            placeholders.put(name, new Segment(matcher.start(), matcher.end()));
        }
        if (placeholders.isEmpty())
            logger.warn("Template {} has no placeholders", template);
        orderedColumnNames = ImmutableList.copyOf(placeholders.keySet());
        columnNames = ImmutableSet.copyOf(placeholders.keySet());
        matcher = RX_BRACE.matcher(template);
        StringBuffer templateBuffer = new StringBuffer(template.length());
        int consumed = 0;
        while (matcher.find()) {
            for (Segment segment : placeholders.values()) {
                if (segment.begin >= matcher.start())
                    --segment.begin;
                if (segment.end >= matcher.start())
                    --segment.end;
            }
            matcher.appendReplacement(templateBuffer, "$1");
            consumed = matcher.end();
        }
        templateBuffer.append(template.subSequence(consumed, template.length()));
        this.template = templateBuffer.toString();
        assertPlaceholdersValid();
    }

    private void assertPlaceholdersValid() {
        if (!getClass().desiredAssertionStatus()) return;
        List<Integer> begins = placeholders.values().stream().map(segment -> segment.begin)
                                                             .collect(toList());
        assert new HashSet<>(begins).size() == begins.size()
                : "There are duplicate placeholder begin positions";
        ArrayList<Segment> segments = new ArrayList<>(placeholders.values());
        for (int i = 1, size = segments.size(); i < size; i++) {
            Segment segment = segments.get(i);
            for (int j = 0; j < i; j++)
                assert segment.begin >= segments.get(j).end : "Segment "+j+" overlaps with "+i;
        }
        List<Integer> list = orderedColumnNames.stream().map(n -> placeholders.get(n).begin)
                                                        .collect(toList());
        for (int i = 1, size = list.size(); i < size; i++) {
            int begin = list.get(i);
            for (int j = 0; j < i; j++)
                assert begin > list.get(j) : i+"-th column does not start after "+j+"-th column";
        }
    }

    /** Get the template string, as given in the R2RML RDF */
    public @Nonnull String getTemplate() {
        return rawTemplate;
    }

    /** Get the parameter names of this template */
    public @Nonnull ImmutableSet<String> getColumnNames() {
        return columnNames;
    }
    public @Nonnull ImmutableList<String> getOrderedColumnNames() {
        return orderedColumnNames;
    }

    /**
     * See {@link RRTemplate#tryExpand(Map, TermMap, RelationalTermParser, String)}.
     */
    public @Nullable RDFNode tryExpand(@Nonnull Map<String, ?> assignments,
                                       @Nonnull TermMap termMap) throws RRTemplateException {
        return tryExpand(assignments, termMap, NaturalSqlTermParser.INSTANCE, "");
    }

    /**
     * Implements the expansion algorithm from [1]
     *
     * [1]: https://www.w3.org/TR/r2rml/#dfn-template-valued-term-map
     *
     * @param assignments Assignments for the column names in template. If the column is
     *                    quoted within the placeholder, it should be quoted as well in the map
     * @param termMap definitions for term type and, in case of rr:Literal, datatype/language.
     * @param termParser converts Java Objects into {@link RDFNode}s
     * @param baseURI Base URI prefix to prepend if <code>termMap.getType()</code> is IRI and the
     *                result IRI after template expansion is an relative IRI. An empty string
     *                means "do not prepend anything even if relative"
     * @throws RRTemplateException if all required placeholders are assigned, but the resulting
     *                           expansion is not valid for the required type.
     * @return the expanded {@link RDFNode} or null if there are missing assignments for
     *         some placeholders
     */
    public @Nullable RDFNode tryExpand(@Nonnull Map<String, ?> assignments,
                                       @Nonnull TermMap termMap,
                                       @Nonnull RelationalTermParser termParser,
                                       @Nonnull String baseURI)
            throws RRTemplateException {
        TermType type = termMap.getTermType();
        String string = tryExpandToString(assignments, type, termParser);
        if (string == null) {
            return null;
        } else if (type.equals(TermType.Literal)) {
            String language = termMap.getLanguage();
            if (language != null)
                return ResourceFactory.createLangLiteral(string, language);
            Resource dtURI = termMap.getDatatype();
            if (dtURI == null)
                dtURI = XSD.xstring;
            RDFDatatype dt = TypeMapper.getInstance().getTypeByName(dtURI.getURI());
            if (dt == null)
                throw new RRTemplateException(template, "Unknown datatype: "+dtURI.getURI());
            return ResourceFactory.createTypedLiteral(string, dt);
        } else if (type.equals(TermType.BlankNode)) {
            throw new RRTemplateException(template, "Cannot create a blank node from template");
        } else {
            assert type.equals(TermType.IRI) : "Expected type=IRI, got "+type;
            if (!baseURI.isEmpty() && !RX_SCHEME.matcher(string).find())
                string = baseURI + string;
            return ResourceFactory.createResource(string);
        }
    }

    /**
     * Same as {@link RRTemplate#tryExpand(Map, TermMap)}, but
     * throws {@link IllegalArgumentException} instead of returning null.
     */
    public @Nonnull RDFNode expand(@Nonnull Map<String, ?> assignments,
                                   @Nonnull TermMap termMap) {
        return expand(assignments, termMap, NaturalSqlTermParser.INSTANCE, "");
    }

    /**
     * Same as {@link RRTemplate#tryExpand(Map, TermMap, RelationalTermParser, String)}, but
     * throws {@link IllegalArgumentException} instead of returning null.
     */
    public @Nonnull RDFNode expand(@Nonnull Map<String, ?> assignments,
                                   @Nonnull TermMap termMap, @Nonnull RelationalTermParser termParser,
                                   @Nonnull String baseURI)
            throws RRTemplateException {
        RDFNode node = tryExpand(assignments, termMap, termParser, baseURI);
        if (node == null) {
            Set<String> missing = placeholders.keySet().stream()
                    .filter(k -> assignments.getOrDefault(k, null) == null).collect(toSet());
            if (!missing.isEmpty()) {
                throw new RRMappingException("Missing or null assignments for template " +
                                             this + ": " + missing);
            }
            throw new IllegalArgumentException("Some assignments could not be expanded");
        }
        return node;
    }

    /* --- --- --- Internals --- --- --- */

    private static final class Segment {
        int begin, end;

        public Segment(int begin, int end) {
            this.begin = begin;
            this.end = end;
        }

        @Override
        public String toString() {
            return String.format("%d:%d", begin, end);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Segment)) return false;
            Segment segment = (Segment) o;
            return begin == segment.begin &&
                    end == segment.end;
        }

        @Override
        public int hashCode() {
            return Objects.hash(begin, end);
        }
    }

    @VisibleForTesting
    @Nullable String tryExpandToString(@Nonnull Map<String, ?> assignments, @Nonnull TermType type,
                                       @Nonnull RelationalTermParser termParser) {
        StringBuilder b = new StringBuilder(template.length()*2);
        int nextCopy = 0;
        for (Map.Entry<String, Segment> e : placeholders.entrySet()) {
            Segment seg = e.getValue();
            b.append(template, nextCopy, seg.begin);
            nextCopy = seg.end;

            Object obj = assignments.getOrDefault(e.getKey(), null);
            if (obj == null) return null;
            RDFNode value = termParser.parseNode(obj);
            b.append(toString(value, type));
        }
        b.append(template, nextCopy, template.length());

        return b.toString();
    }

    @VisibleForTesting
    @Nonnull String toString(@Nonnull RDFNode value, @Nonnull TermType type) throws RRTemplateException {
        String string;
        if (value.isLiteral()) string = value.asLiteral().getLexicalForm();
        else if (value.isURIResource()) string = value.asResource().getURI();
        else throw new RRTemplateException(template, "Cannot expand placeholders with blank nodes");
        if (type == TermType.IRI)
            string = escape(string);
        return string;
    }

    private @Nonnull String escape(@Nonnull String string) {
        try {
            string = URLEncoder.encode(string, "UTF-8").replace("+", "%20");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Unexpected exception: Unsupported UTF-8", e);
        }
        return string;
    }

    /* --- --- --- Object methods --- --- --- */

    @Override
    public String toString() {
        return getTemplate();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RRTemplate)) return false;
        RRTemplate template1 = (RRTemplate) o;
        assert !getTemplate().equals(template1.getTemplate())
                || placeholders.equals(template1.placeholders)
                : "Same template but different placeholders map";
        return getTemplate().equals(template1.getTemplate());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getTemplate());
    }
}
