package ness.cache2.guava;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.concurrent.Callable;

import ness.cache2.CacheModule;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.cache.Cache;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.nesscomputing.config.Config;
import com.nesscomputing.config.ConfigModule;
import com.nesscomputing.lifecycle.junit.LifecycleRule;
import com.nesscomputing.lifecycle.junit.LifecycleRunner;
import com.nesscomputing.lifecycle.junit.LifecycleStatement;
import com.nesscomputing.testing.lessio.AllowDNSResolution;

@AllowDNSResolution
@RunWith(LifecycleRunner.class)
public class CustomSerializationGuavaCacheTest {

    @LifecycleRule
    public LifecycleStatement lifecycleRule = LifecycleStatement.defaultLifecycle();

    @Inject
    @Named("test-ns")
    Cache<String, Integer> myCache;

    Config config = Config.getFixedConfig("ness.cache.test", "JVM");

    @Test
    public void testConversions() throws Exception {

        Function<Integer, byte[]> intSerializer = new Function<Integer, byte[]>() {
            @Override
            public byte[] apply(Integer input) {
                return new byte[] {input.byteValue()};
            }
        };

        Function<byte[], Integer> intDeserializer = new Function<byte[], Integer>() {
            @Override
            public Integer apply(byte[] input) {
                return (int) input[0];
            }
        };

        Guice.createInjector(
                lifecycleRule.getLifecycleModule(),
                new ConfigModule(config),
                new CacheModule(config, "test"),
                NessGuavaCaches.newModuleBuilder("test", "test-ns", String.class, Integer.class)
                    .withKeySerializer(Functions.toStringFunction())
                    .withValueSerializer(intSerializer, intDeserializer)
                    .build()
            ).injectMembers(this);

        assertNull(myCache.getIfPresent("foo"));

        assertEquals(3, (int) myCache.get("foo", new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return 3;
            }
        }));

        assertEquals(3, (int) myCache.get("foo", new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                throw new UnsupportedOperationException();
            }
        }));
    }
}
