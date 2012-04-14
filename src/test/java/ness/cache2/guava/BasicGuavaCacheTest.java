package ness.cache2.guava;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;

import java.util.concurrent.Callable;

import ness.cache2.CacheModule;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.base.Charsets;
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
public class BasicGuavaCacheTest {

    @LifecycleRule
    public LifecycleStatement lifecycleRule = LifecycleStatement.defaultLifecycle();

    @Inject
    @Named("test-ns")
    Cache<String, byte[]> myCache;

    Config config = Config.getFixedConfig("ness.cache.test", "JVM");

    @Test
    public void testBasicUsage() throws Exception {
        Guice.createInjector(
                lifecycleRule.getLifecycleModule(),
                new ConfigModule(config),
                new CacheModule(config, "test"),
                NessGuavaCaches.newModuleBuilder("test", "test-ns")
                    .build()
            ).injectMembers(this);

        assertNull(myCache.getIfPresent("foo"));

        assertArrayEquals("bar".getBytes(Charsets.UTF_8), myCache.get("foo", new Callable<byte[]>() {
            @Override
            public byte[] call() throws Exception {
                return "bar".getBytes(Charsets.UTF_8);
            }
        }));

        assertArrayEquals("bar".getBytes(Charsets.UTF_8), myCache.get("foo", new Callable<byte[]>() {
            @Override
            public byte[] call() throws Exception {
                throw new UnsupportedOperationException();
            }
        }));
    }
}
