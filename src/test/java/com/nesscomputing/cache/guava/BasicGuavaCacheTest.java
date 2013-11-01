package com.nesscomputing.cache.guava;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Collections;
import java.util.concurrent.Callable;


import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.base.Charsets;
import com.google.common.base.Functions;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import com.nesscomputing.cache.CacheModule;
import com.nesscomputing.cache.guava.NessGuavaCaches;
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

    @Before
    public void setUp() throws Exception {
        Guice.createInjector(
                lifecycleRule.getLifecycleModule(),
                new ConfigModule(config),
                new CacheModule("test"),
                NessGuavaCaches.newModuleBuilder("test", "test-ns", String.class, byte[].class)
                    .withKeySerializer(Functions.toStringFunction())
                    .withValueSerializer(Functions.<byte[]>identity(), Functions.<byte[]>identity())
                    .build()
            ).injectMembers(this);
    }

    @Test
    public void testBasicUsage() throws Exception {

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

    @Test
    public void testDoubleGet() throws Exception {
        assertArrayEquals("bar".getBytes(Charsets.UTF_8), myCache.get("foo", new Callable<byte[]>() {
            @Override
            public byte[] call() throws Exception {
                return "bar".getBytes(Charsets.UTF_8);
            }
        }));
        assertEquals(Collections.singleton("foo"), myCache.getAllPresent(ImmutableList.of("foo", "foo")).keySet());
    }
}
