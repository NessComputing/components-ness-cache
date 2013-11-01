package com.nesscomputing.cache;

import com.nesscomputing.service.discovery.client.ReadOnlyDiscoveryClient;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;

import com.nesscomputing.cache.CacheModule;
import com.nesscomputing.config.Config;
import com.nesscomputing.lifecycle.Lifecycle;
import com.nesscomputing.lifecycle.LifecycleStage;
import com.nesscomputing.lifecycle.guice.LifecycleModule;
import com.nesscomputing.testing.lessio.AllowDNSResolution;
import com.nesscomputing.testing.lessio.AllowNetworkAccess;

@AllowDNSResolution
@AllowNetworkAccess(endpoints = {"127.0.0.1:11212"})
public class CacheIntegrationTest extends BaseCacheIntegrationSetup {
    @Inject
    Lifecycle lifecycle;

    @Before
    public final void setUpClient() {
        final Config config = Config.getFixedConfig("ness.cache", "MEMCACHE",
                                                    "ness.cache.synchronous", "true",
                                                    "ness.cache.uri", "memcache://localhost:11212",
                                                    "ness.ncache.jmx", "false");

        Guice.createInjector(new CacheModule("test"),
                             new LifecycleModule(),
                             new AbstractModule() {
            @Override
            protected void configure() {
                requestInjection (CacheIntegrationTest.this);
                bind (ReadOnlyDiscoveryClient.class).toInstance(EasyMock.createNiceMock(ReadOnlyDiscoveryClient.class));
                bind (Config.class).toInstance(config);
            }
        });
        lifecycle.executeTo(LifecycleStage.START_STAGE);
    }

    @After
    public final void stopLifecycle() {
        lifecycle.executeTo(LifecycleStage.STOP_STAGE);
    }
}
