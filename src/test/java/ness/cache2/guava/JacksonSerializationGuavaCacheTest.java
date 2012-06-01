package ness.cache2.guava;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import ness.cache2.CacheModule;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.cache.Cache;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.nesscomputing.config.Config;
import com.nesscomputing.config.ConfigModule;
import com.nesscomputing.jackson.JacksonSerializerBinder;
import com.nesscomputing.jackson.Json;
import com.nesscomputing.jackson.NessJacksonModule;
import com.nesscomputing.lifecycle.junit.LifecycleRule;
import com.nesscomputing.lifecycle.junit.LifecycleRunner;
import com.nesscomputing.lifecycle.junit.LifecycleStatement;
import com.nesscomputing.testing.lessio.AllowDNSResolution;

@AllowDNSResolution
@RunWith(LifecycleRunner.class)
public class JacksonSerializationGuavaCacheTest {

    @LifecycleRule
    public LifecycleStatement lifecycleRule = LifecycleStatement.defaultLifecycle();

    @Inject
    @Named("test-ns")
    Cache<BigDecimal, DateTime> myCache;

    Config config = Config.getFixedConfig("ness.cache.test", "JVM");

    @Test
    public void testJacksonSerialization() throws Exception {

        Guice.createInjector(
                lifecycleRule.getLifecycleModule(),
                new ConfigModule(config),
                new CacheModule(config, "test"),
                new NessJacksonModule(),
                NessGuavaCaches.newModuleBuilder("test", "test-ns", BigDecimal.class, DateTime.class)
                    .withSerializers(Json.class)
                    .build(),
                new AbstractModule() {
                    @Override
                    protected void configure() {
                        JacksonSerializerBinder.bindSerializer(binder(), BigDecimal.class).build();
                        JacksonSerializerBinder.bindSerializer(binder(), DateTime.class).build();
                    }
                }
            ).injectMembers(this);

        assertNull(myCache.getIfPresent(BigDecimal.TEN));

        assertEquals(new DateTime(10, DateTimeZone.UTC), myCache.get(BigDecimal.TEN, new Callable<DateTime>() {
            @Override
            public DateTime call() throws Exception {
                return new DateTime(10, DateTimeZone.UTC);
            }
        }));

        assertEquals(new DateTime(10, DateTimeZone.UTC), myCache.get(BigDecimal.TEN, new Callable<DateTime>() {
            @Override
            public DateTime call() throws Exception {
                throw new UnsupportedOperationException();
            }
        }));

        assertNull(myCache.getIfPresent(BigDecimal.ZERO));

        myCache.invalidate(BigDecimal.ZERO);
        myCache.invalidate(BigDecimal.TEN);
        myCache.invalidate("foo");

        final AtomicBoolean called = new AtomicBoolean();

        assertEquals(new DateTime(10, DateTimeZone.UTC), myCache.get(BigDecimal.TEN, new Callable<DateTime>() {
            @Override
            public DateTime call() throws Exception {
                called.set(true);
                return new DateTime(10, DateTimeZone.UTC);
            }
        }));

        assertTrue(called.get());
    }
}
