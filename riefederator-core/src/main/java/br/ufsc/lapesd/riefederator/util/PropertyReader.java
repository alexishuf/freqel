package br.ufsc.lapesd.riefederator.util;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public abstract class PropertyReader<T> {
    public enum PropertySource {
        JAVA,
        ENVIRONMENT
    }
    private @Nonnull String javaPropertyName, envVarName;
    private @Nonnull T defaultValue;
    private @Nullable T override = null;

    protected PropertyReader(@Nonnull String javaPropertyName, @Nonnull T defaultValue) {
        this.defaultValue = defaultValue;
        this.javaPropertyName = javaPropertyName;
        this.envVarName = javaPropertyName.replace('.', '_').toUpperCase();
    }
    protected abstract T parse(@Nonnull PropertySource source, @Nonnull String name,
                               @Nonnull String value);

    public void setOverride(@Nullable T override) {
        this.override = override;
    }

    public @Nonnull T get() {
        if (override != null)
            return override;
        T value = null;
        String string = System.getProperty(javaPropertyName);
        if (string != null)
            value = parse(PropertySource.JAVA, javaPropertyName, string);
        if (value != null)
            return value;
        string = System.getenv(envVarName);
        if (string != null)
            value = parse(PropertySource.ENVIRONMENT, envVarName, string);
        return value != null ? value : defaultValue;
    }
}
