package top.hcode.hoj.config;

import com.alibaba.nacos.api.config.ConfigService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

class NacosSwitchConfigTest {

    private Object previousInit;
    private Object previousConfigService;
    private Object previousSwitchConfig;
    private Object previousWebConfig;
    private NacosSwitchConfig config;

    @BeforeEach
    void setUp() {
        previousInit = ReflectionTestUtils.getField(NacosSwitchConfig.class, "init");
        previousConfigService = ReflectionTestUtils.getField(NacosSwitchConfig.class, "configService");
        previousSwitchConfig = ReflectionTestUtils.getField(NacosSwitchConfig.class, "switchConfig");
        previousWebConfig = ReflectionTestUtils.getField(NacosSwitchConfig.class, "webConfig");
        ReflectionTestUtils.setField(NacosSwitchConfig.class, "init", new AtomicBoolean(false));
        ReflectionTestUtils.setField(NacosSwitchConfig.class, "configService", null);
        ReflectionTestUtils.setField(NacosSwitchConfig.class, "switchConfig", null);
        ReflectionTestUtils.setField(NacosSwitchConfig.class, "webConfig", null);
        config = new NacosSwitchConfig();
        config.setNacosEnabled(false);
        config.setNACOS_URL("127.0.0.1:1");
        config.setNacosUsername("local-test");
        config.setNacosPassword("local-test");
        config.setSwitchConfigFileName("local-switch.yml");
        config.setWebConfigFileName("local-web.yml");
        config.setGroup("DEFAULT_GROUP");
    }

    @AfterEach
    void restoreSharedState() {
        ReflectionTestUtils.setField(NacosSwitchConfig.class, "init", previousInit);
        ReflectionTestUtils.setField(NacosSwitchConfig.class, "configService", previousConfigService);
        ReflectionTestUtils.setField(NacosSwitchConfig.class, "switchConfig", previousSwitchConfig);
        ReflectionTestUtils.setField(NacosSwitchConfig.class, "webConfig", previousWebConfig);
    }

    @Test
    void disabledNacosInitializesDefaultsWithoutCreatingAClient() {
        config.init();

        assertNotNull(config.getSwitchConfig());
        assertNotNull(config.getWebConfig());
        assertNull(ReflectionTestUtils.getField(NacosSwitchConfig.class, "configService"));
    }

    @Test
    void localChangesStayInMemoryWithoutPublishingToNacos() {
        config.init();
        ConfigService client = mock(ConfigService.class);
        ReflectionTestUtils.setField(NacosSwitchConfig.class, "configService", client);
        config.getWebConfig().setName("Local OJ");

        assertTrue(config.publishWebConfig());
        assertTrue(config.publishSwitchConfig());
        assertEquals("Local OJ", config.getWebConfig().getName());
        verifyNoInteractions(client);
    }
}
