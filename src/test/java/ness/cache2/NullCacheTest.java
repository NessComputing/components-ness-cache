package ness.cache2;

import static org.junit.Assert.assertTrue;
import io.trumpet.config.Config;
import io.trumpet.config.guice.TestingConfigModule;
import io.trumpet.lifecycle.Lifecycle;
import io.trumpet.lifecycle.LifecycleStage;
import io.trumpet.lifecycle.guice.LifecycleModule;

import java.util.Collection;
import java.util.Collections;

import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.name.Named;

public class NullCacheTest {

    @Inject
    @Named("test")
    Cache cache;

    @Inject
    Lifecycle lifecycle;

    @Before
    public final void setUpClient() {
        final TestingConfigModule tcm = new TestingConfigModule(ImmutableMap.of("ness.cache", "NONE",
                                                                                "ness.cache.jmx", "false"));
        final Config config = tcm.getConfig();

        Guice.createInjector(tcm,
                             new CacheModule(config, "test"),
                             new LifecycleModule(),
                             new AbstractModule() {
            @Override
            protected void configure() {
                requestInjection (NullCacheTest.this);
            }
        });

        lifecycle.executeTo(LifecycleStage.START_STAGE);
    }

    @After
    public final void stopLifecycle() {
        lifecycle.executeTo(LifecycleStage.STOP_STAGE);
    }

    @Test
    public void testNoCache() {
        String ns = "a";
        Collection<String> b = Collections.singleton("b");
        assertTrue(cache.get(ns, b).isEmpty());
        cache.set(ns, Collections.singleton(new CacheStore<byte []>("b", new byte[1], new DateTime().plusMinutes(1))));
        assertTrue(cache.get(ns, b).isEmpty());
        cache.clear(ns, b);
        assertTrue(cache.get(ns, b).isEmpty());
    }
}
