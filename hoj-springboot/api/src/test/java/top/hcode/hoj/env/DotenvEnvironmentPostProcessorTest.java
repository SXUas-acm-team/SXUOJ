package top.hcode.hoj.env;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.env.SystemEnvironmentPropertySource;
import org.springframework.core.io.support.SpringFactoriesLoader;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DotenvEnvironmentPostProcessorTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void loadsDotenvAndResolvesPlaceholders() throws Exception {
        StandardEnvironment environment = environment("MYSQL_HOST=dotenv-host\nNACOS_ENABLED=false\n");

        process(environment);

        assertEquals("dotenv-host", environment.getProperty("MYSQL_HOST"));
        assertEquals("false", environment.resolveRequiredPlaceholders("${NACOS_ENABLED:true}"));
    }

    @Test
    public void operatingSystemAndJavaPropertiesOverrideFile() throws Exception {
        StandardEnvironment environment = environment("MYSQL_HOST=dotenv-host\nMYSQL_PORT=3306\n");
        environment.getPropertySources().replace(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME,
                new SystemEnvironmentPropertySource(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME,
                        Collections.<String, Object>singletonMap("MYSQL_HOST", "os-host")));
        environment.getPropertySources().replace(StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME,
                new MapPropertySource(StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME,
                        Collections.<String, Object>singletonMap("MYSQL_PORT", "3307")));

        process(environment);

        assertEquals("os-host", environment.getProperty("MYSQL_HOST"));
        assertEquals("3307", environment.getProperty("MYSQL_PORT"));
    }

    @Test
    public void optionalFileAndRepeatedProcessingAreSupported() throws Exception {
        StandardEnvironment environment = environment(null);

        process(environment);
        process(environment);

        int count = 0;
        for (org.springframework.core.env.PropertySource<?> source : environment.getPropertySources()) {
            if (DotenvEnvironmentPostProcessor.PROPERTY_SOURCE_NAME.equals(source.getName())) {
                count++;
            }
        }
        assertEquals(1, count);
    }

    @Test
    public void registersWithSpringBootEnvironmentLifecycle() {
        assertTrue(SpringFactoriesLoader.loadFactoryNames(EnvironmentPostProcessor.class,
                getClass().getClassLoader()).contains(DotenvEnvironmentPostProcessor.class.getName()));
    }

    private StandardEnvironment environment(String dotenvContent) throws Exception {
        File directory = temporaryFolder.newFolder();
        if (dotenvContent != null) {
            Files.write(new File(directory, ".env").toPath(), dotenvContent.getBytes(StandardCharsets.UTF_8));
        }
        StandardEnvironment environment = new StandardEnvironment();
        environment.getPropertySources().replace(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME,
                new SystemEnvironmentPropertySource(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME,
                        Collections.<String, Object>emptyMap()));
        environment.getPropertySources().replace(StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME,
                new MapPropertySource(StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME,
                        Collections.<String, Object>emptyMap()));
        Map<String, Object> options = new HashMap<>();
        options.put("hoj.dotenv.directory", directory.getAbsolutePath());
        environment.getPropertySources().addFirst(new MapPropertySource("testOptions", options));
        return environment;
    }

    private void process(StandardEnvironment environment) {
        new DotenvEnvironmentPostProcessor().postProcessEnvironment(environment, null);
    }
}
