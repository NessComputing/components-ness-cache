package ness.cache2;

import io.trumpet.config.Config;
import io.trumpet.config.guice.TestingConfigModule;
import io.trumpet.lifecycle.Lifecycle;
import io.trumpet.lifecycle.LifecycleStage;
import io.trumpet.lifecycle.guice.LifecycleModule;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;

import ness.discovery.client.ReadOnlyDiscoveryClient;

import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.kaching.platform.testing.AllowDNSResolution;
import com.kaching.platform.testing.AllowNetworkAccess;

@AllowDNSResolution
@AllowNetworkAccess(endpoints = {"127.0.0.1:11212"})
public class CacheIntegrationTest extends BaseCacheIntegrationSetup {
    @Inject
    Lifecycle lifecycle;

    @Before
    public final void setUpClient() {
        final TestingConfigModule tcm = new TestingConfigModule(ImmutableMap.of("ness.cache", "MEMCACHE",
                                                                                "ness.cache.synchronous", "true",
                                                                                "ness.cache.uri", "memcache://localhost:11212",
                                                                                "ness.ncache.jmx", "false"));
        final Config config = tcm.getConfig();

        Guice.createInjector(tcm,
                             new CacheModule(config, "test"),
                             new LifecycleModule(),
                             new AbstractModule() {
            @Override
            protected void configure() {
                requestInjection (CacheIntegrationTest.this);
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
