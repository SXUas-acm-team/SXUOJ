package top.hcode.hoj.env;

import io.github.cdimascio.dotenv.Dotenv;
import io.github.cdimascio.dotenv.DotenvEntry;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.boot.context.config.ConfigFileApplicationListener;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.StandardEnvironment;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Loads the working directory's optional .env before application/bootstrap
 * configuration is resolved, using the Spring Boot 2.2 lifecycle API.
 */
public class DotenvEnvironmentPostProcessor implements EnvironmentPostProcessor, Ordered {

    static final String PROPERTY_SOURCE_NAME = "hojDotenv";

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        Dotenv dotenv = Dotenv.configure()
                .directory(environment.getProperty("hoj.dotenv.directory", "."))
                .filename(environment.getProperty("hoj.dotenv.filename", ".env"))
                .ignoreIfMissing()
                .load();

        Map<String, Object> values = new LinkedHashMap<>();
        for (DotenvEntry entry : dotenv.entries(Dotenv.Filter.DECLARED_IN_ENV_FILE)) {
            values.put(entry.getKey(), entry.getValue());
        }

        MutablePropertySources sources = environment.getPropertySources();
        MapPropertySource propertySource = new MapPropertySource(PROPERTY_SOURCE_NAME, values);
        if (sources.contains(PROPERTY_SOURCE_NAME)) {
            sources.replace(PROPERTY_SOURCE_NAME, propertySource);
        } else if (sources.contains(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME)) {
            // Existing OS variables and Java system properties override file values.
            sources.addAfter(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME, propertySource);
        } else {
            sources.addLast(propertySource);
        }
    }

    @Override
    public int getOrder() {
        return ConfigFileApplicationListener.DEFAULT_ORDER - 1;
    }
}
