package ness.cache2;

import io.trumpet.config.Config;
import io.trumpet.config.guice.TestingConfigModule;
import io.trumpet.lifecycle.Lifecycle;
import io.trumpet.lifecycle.LifecycleStage;
import io.trumpet.lifecycle.guice.LifecycleModule;

import java.net.URI;
import java.util.Collections;
import java.util.List;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;

import ness.discovery.client.ReadOnlyDiscoveryClient;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import com.kaching.platform.testing.AllowDNSResolution;
import com.kaching.platform.testing.AllowNetworkAccess;

@AllowDNSResolution
@AllowNetworkAccess(endpoints = {"127.0.0.1:11212"})
public class CacheIntegrationTest extends BaseCacheIntegrationSetup {
    @Inject
    Lifecycle lifecycle;

    final CacheConfiguration configuration = new CacheConfiguration() {
        @Override
        public CacheType getCacheType() {
            return CacheType.MEMCACHE;
        }
        @Override
        public boolean isCacheSynchronous() {
            return true;
        }
        @Override
        public List<URI> getCacheUri() {
            return Collections.singletonList(URI.create("memcache://localhost:11212"));
        }
        @Override
        public boolean isJmxEnabled() {
            return false;
        }
    };

    @Before
    public final void setUpClient() {
        final TestingConfigModule tcm = new TestingConfigModule();
        final Config config = tcm.getConfig();

        final Module testModule = Modules.override(new CacheModule(config)).with(new AbstractModule() {
            @Override
            public void configure()
            {
                bind(CacheConfiguration.class).toInstance(configuration);
            }
        });


        Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                requestInjection (CacheIntegrationTest.this);

                install (tcm);
                install (testModule);
                install (new LifecycleModule());
                bind (ReadOnlyDiscoveryClient.class).toInstance(EasyMock.createNiceMock(ReadOnlyDiscoveryClient.class));
            }
        });
        lifecycle.executeTo(LifecycleStage.START_STAGE);
    }

    @After
    public final void stopLifecycle() {
        lifecycle.executeTo(LifecycleStage.STOP_STAGE);
    }
}
