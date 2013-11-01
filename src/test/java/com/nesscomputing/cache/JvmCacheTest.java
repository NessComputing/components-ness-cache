package com.nesscomputing.cache;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;

import org.easymock.EasyMock;
import org.junit.Before;

import com.nesscomputing.cache.CacheModule;
import com.nesscomputing.config.Config;
import com.nesscomputing.lifecycle.Lifecycle;
import com.nesscomputing.testing.lessio.AllowDNSResolution;

@AllowDNSResolution
public class JvmCacheTest extends BaseCachingTests {
    @Inject
    Lifecycle lifecycle;

    @Before
    public final void setUpClient() {

        final Config config = Config.getFixedConfig("ness.cache", "JVM",
                                                    "ness.cache.jmx", "false");

        Guice.createInjector(new CacheModule("test"),
                             new AbstractModule() {
            @Override
            protected void configure() {
                requestInjection (JvmCacheTest.this);
                bind (Lifecycle.class).toInstance(EasyMock.createMock(Lifecycle.class));

                bind (Config.class).toInstance(config);
            }
        });
    }
}
