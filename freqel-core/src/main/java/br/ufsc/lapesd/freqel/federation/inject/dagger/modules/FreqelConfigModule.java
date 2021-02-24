package br.ufsc.lapesd.freqel.federation.inject.dagger.modules;

import br.ufsc.lapesd.freqel.federation.FreqelConfig;
import dagger.Module;
import dagger.Provides;
import dagger.Reusable;

import javax.annotation.Nullable;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.File;
import java.io.IOException;

@Module
public abstract class FreqelConfigModule {

    @Provides @Reusable @Named("tempDir") public static File
    tempDir(@Named("tempDirOverride") @Nullable File override, FreqelConfig config) {
        if (override != null)
            return override;
        File tempDir = config.get(FreqelConfig.Key.TEMP_DIR, File.class);
        assert tempDir != null;
        return tempDir;
    }

    @Provides @Singleton public static FreqelConfig
    config(@Named("override") @Nullable FreqelConfig override) {
        if (override != null)
            return override;
        try {
            return new FreqelConfig(); //load configs from default locations
        } catch (IOException | FreqelConfig.InvalidValueException e) {
            throw new RuntimeException("Failed to create default FreqelConfig", e);
        }
    }
}
