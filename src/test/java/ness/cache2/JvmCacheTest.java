package ness.cache2;

import io.trumpet.config.Config;
import io.trumpet.config.guice.TestingConfigModule;
import io.trumpet.lifecycle.Lifecycle;

import org.easymock.EasyMock;
import org.junit.Before;

import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.kaching.platform.testing.AllowDNSResolution;

@AllowDNSResolution
public class JvmCacheTest extends BaseCachingTests {
    @Inject
    Lifecycle lifecycle;

    @Before
    public final void setUpClient() {

        final TestingConfigModule tcm = new TestingConfigModule(ImmutableMap.of("ness.cache", "JVM",
                                                                                "ness.cache.jmx", "false"));
        final Config config = tcm.getConfig();

        Guice.createInjector(tcm,
                             new CacheModule(config),
                             new AbstractModule() {
            @Override
            protected void configure() {
                requestInjection (JvmCacheTest.this);
                bind (Lifecycle.class).toInstance(EasyMock.createMock(Lifecycle.class));
            }
        });
    }
}
