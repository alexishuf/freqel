package br.ufsc.lapesd.freqel.federation.spec.source;

import org.testng.Assert;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.Map;

@Test(groups = {"fast"})
public class SourceLoadRegistryTest {

    @Test
    public void testSPI() {
        Assert.assertEquals(countLoaderInstances(SPARQLServiceLoader.class), 1); //freqel-core
        Assert.assertEquals(countLoaderInstances(SwaggerSourceLoader.class), 1); //freqel-webapis
    }

    private long countLoaderInstances(@Nonnull Class<? extends SourceLoader> cls) {
        Map<String, SourceLoader> map = SourceLoaderRegistry.getDefault().getLoaders();
        return map.values().stream().filter(cls::isInstance).count();
    }
}
