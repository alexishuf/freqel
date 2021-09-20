package br.ufsc.lapesd.freqel.federation;

import br.ufsc.lapesd.freqel.federation.FreqelConfig.ConfigSource;
import br.ufsc.lapesd.freqel.federation.FreqelConfig.InvalidValueException;
import br.ufsc.lapesd.freqel.reason.regimes.SourcedEntailmentRegime;
import br.ufsc.lapesd.freqel.reason.regimes.W3CEntailmentRegimes;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.*;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static br.ufsc.lapesd.freqel.federation.FreqelConfig.ConfigSource.JAVA_PROPERTIES;
import static br.ufsc.lapesd.freqel.federation.FreqelConfig.Key.*;
import static br.ufsc.lapesd.freqel.reason.regimes.EntailmentEvidences.SINGLE_SOURCE;
import static br.ufsc.lapesd.freqel.reason.regimes.EntailmentEvidences.SINGLE_SOURCE_ABOX;
import static com.google.common.collect.Sets.newHashSet;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.testng.Assert.*;

@SuppressWarnings("unchecked")
@Test(groups = {"fast"})
public class FreqelConfigTest {
    private File file;
    private File configProperties;
    private File configYaml;
    private File dir;

    @BeforeClass
    public void beforeClass() throws IOException {
        file = Files.createTempFile("freqel", "").toFile();
        configProperties = Files.createTempFile("freqel", ".properties").toFile();
        configYaml = Files.createTempFile("freqel", ".yaml").toFile();
        dir = Files.createTempDirectory("freqel").toFile();
    }

    @AfterClass
    public void afterClass() throws IOException {
        assertTrue(file.delete());
        assertTrue(configProperties.delete());
        assertTrue(configYaml.delete());
        FileUtils.deleteDirectory(dir);
    }

    @Test
    public void testConfigSourcePrecedence() {
        List<ConfigSource> list = asList(ConfigSource.fromLowestPrecedence());
        int env = list.indexOf(ConfigSource.ENVIRONMENT);
        int res = list.indexOf(ConfigSource.RESOURCE);
        int home = list.indexOf(ConfigSource.HOME_FILE);
        assertTrue(home < res);
        assertTrue(home < env);
        assertTrue(res < env);
    }

    @DataProvider public @Nonnull Object[][] boolData() {
        return Stream.of(
                asList("true", true),
                asList("True", true),
                asList("TRUE", true),
                asList("T", true),
                asList("yes", true),
                asList("Yes", true),
                asList("y", true),
                asList("1", true),
                asList("false", false),
                asList("False", false),
                asList("FALSE", false),
                asList("F", false),
                asList("no", false),
                asList("No", false),
                asList("NO", false),
                asList("N", false),
                asList("n", false),
                asList("0", false),
                asList(null, false),
                asList("bullshit", null)
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "boolData")
    public void testParseBool(@Nullable Object in, @Nullable Boolean expected) {
        try {
            assertEquals(ESTIMATE_ASK_LOCAL.parse(in), expected);
            assertNotNull(expected, "Expected a exception");
        } catch (InvalidValueException e) {
            assertNull(expected, "Expected "+expected+", got \""+e.getMessage()+"\"");
        }
    }

    @DataProvider public @Nonnull Object[][] setData() {
        return Stream.of(
                asList(newHashSet("1", "2"), newHashSet("1", "2")),
                asList(Collections.emptySet(), Collections.emptySet()),
                asList(Collections.emptyList(), Collections.emptySet()),
                asList(asList("4", "5"), newHashSet("4", "5")),
                asList(asList("4", "5", "5"), newHashSet("4", "5")),
                asList("asd, qwe", newHashSet("asd", "qwe")),
                asList(" asd ,   qwe ", newHashSet("asd", "qwe")),
                asList("[asd, qwe]", newHashSet("asd", "qwe")),
                asList("{asd, qwe}", newHashSet("asd", "qwe")),
                asList("(asd, qwe)", newHashSet("asd", "qwe")),
                asList("(asd)", Collections.singleton("asd")),
                asList("[asd]", Collections.singleton("asd")),
                asList("[a.s.d]", Collections.singleton("a.s.d")),
                asList(asList(1, 2), InvalidValueException.class),
                asList(1, Collections.singleton("1"))
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "setData")
    public void testParseSet(@Nullable Object in, @Nullable Object expected) throws InvalidValueException {
        if (InvalidValueException.class.equals(expected)) {
            assertThrows(InvalidValueException.class, () -> CARDINALITY_HEURISTICS.parse(in));
        } else {
            assert expected == null || expected instanceof Set;
            Set<String> actual = (Set<String>)CARDINALITY_HEURISTICS.parse(in);
            assertEquals(actual, (Set<String>) expected);
        }
    }

    @DataProvider public @Nonnull Object[][] parseDirData() throws MalformedURLException {
        File nonExisting = new File(dir, "sub");
        return Stream.of(
                asList(dir, dir),
                asList(dir.getAbsolutePath(), dir),
                asList(dir.getPath(), dir),
                asList(dir.toPath(), dir),
                asList(dir.toPath().toUri(), dir),
                asList(dir.toPath().toUri().toURL(), dir),
                asList("file://"+dir.getAbsolutePath().replace(" ", "%20"), dir),
                asList(file, InvalidValueException.class),
                asList(file.getAbsolutePath(), InvalidValueException.class),
                asList(file.toPath(), InvalidValueException.class),
                asList(file.toPath().toUri(), InvalidValueException.class),
                asList("file://"+file.getAbsolutePath(), InvalidValueException.class),
                asList(nonExisting, nonExisting),
                asList(nonExisting.getAbsolutePath(), nonExisting),
                asList(nonExisting.toPath(), nonExisting),
                asList(nonExisting.toPath().toUri(), nonExisting),
                asList("file://"+nonExisting.getAbsolutePath(), nonExisting)
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "parseDirData")
    public void testParseDir(@Nullable Object in, @Nullable Object expected) throws InvalidValueException {
        if (InvalidValueException.class.equals(expected))
            assertThrows(InvalidValueException.class, () -> TEMP_DIR.parse(in));
        else
            assertEquals(TEMP_DIR.parse(in), expected);
    }


    @Test
    public void testPrecedence() throws IOException, InvalidValueException {
        try (Writer w = new OutputStreamWriter(new FileOutputStream(configProperties), UTF_8)) {
            w.write("estimate-ask-local=T\n");         //not overridden
            w.write("resultsExecutorBufferSize=23"); //overridden
            w.write("sources_cache_dir=cache1");     //overridden
        }
        try (Writer w = new OutputStreamWriter(new FileOutputStream(configYaml), UTF_8)) {
            w.write("RESULTS_EXECUTOR_BUFFER_SIZE: 5\n"); // not overridden
            w.write("sourcesCacheDir: cache2\n");         // overridden
        }
        System.setProperty("sources.cache.dir", "cache3");
        try {
            String yamlUri = configYaml.toPath().toUri().toString();
            FreqelConfig cfg = new FreqelConfig(configProperties, yamlUri, JAVA_PROPERTIES);
            assertTrue(cfg.get(ESTIMATE_ASK_LOCAL, Boolean.class));
            assertEquals(cfg.get(RESULTS_EXECUTOR_BUFFER_SIZE, Integer.class), Integer.valueOf(5));
            assertEquals(cfg.get(SOURCES_CACHE_DIR, File.class), new File("cache3"));
        } finally {
            System.clearProperty("sources.cache.dir");
        }
    }

    @DataProvider
    public @Nonnull Object[][] parseAdvertisedReasoningData() {
        String rdfs = W3CEntailmentRegimes.RDFS.iri();
        return Stream.of(
                asList(null, null, false),
                asList("", null, true),
                asList("@", null, true),
                asList(rdfs,
                       new SourcedEntailmentRegime(SINGLE_SOURCE_ABOX, W3CEntailmentRegimes.RDFS),
                       false),
                asList("SINGLE_SOURCE@"+rdfs,
                       new SourcedEntailmentRegime(SINGLE_SOURCE, W3CEntailmentRegimes.RDFS),
                       false),
                asList("SINGLE-SOURCE@"+rdfs,
                       new SourcedEntailmentRegime(SINGLE_SOURCE, W3CEntailmentRegimes.RDFS),
                       false),
                asList("single-source-abox@"+rdfs,
                       new SourcedEntailmentRegime(SINGLE_SOURCE_ABOX, W3CEntailmentRegimes.RDFS),
                       false)
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "parseAdvertisedReasoningData")
    public void testParseAdvertisedReasoning(Object value, SourcedEntailmentRegime expected,
                                             boolean expectException) {
        try {
            assertEquals(ADVERTISED_REASONING.parse(value), expected);
        } catch (InvalidValueException e) {
            assertTrue(expectException, "Unexpected "+e);
        }
    }
}