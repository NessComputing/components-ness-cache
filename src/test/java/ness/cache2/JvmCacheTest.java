package ness.cache2;

import io.trumpet.config.Config;
import io.trumpet.config.guice.TestingConfigModule;
import io.trumpet.lifecycle.Lifecycle;

import org.easymock.EasyMock;
import org.junit.Before;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.util.Modules;
import com.kaching.platform.testing.AllowDNSResolution;

@AllowDNSResolution
public class JvmCacheTest extends BaseCachingTests {
    @Inject
    Lifecycle lifecycle;

    @Before
    public final void setUpClient() {

        final TestingConfigModule tcm = new TestingConfigModule();
        final Config config = tcm.getConfig();

        Guice.createInjector(tcm,
                             new AbstractModule() {
            @Override
            protected void configure() {
                requestInjection (JvmCacheTest.this);

                install (Modules.override(new CacheModule(config)).with(new AbstractModule()
                {
                    @Override
                    public void configure()
                    {
                        bind(CacheConfiguration.class).toInstance(new CacheConfiguration() {
                            @Override
                            public CacheType getCacheType() {
                                return CacheType.JVM;
                            }
                            @Override
                            public boolean isJmxEnabled() {
                                return false;
                            }
                        });
                    }
                }));

                bind (Lifecycle.class).toInstance(EasyMock.createMock(Lifecycle.class));
            }
        });
    }
}
