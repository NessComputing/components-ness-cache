/**
 * Copyright (C) 2012 Ness Computing, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nesscomputing.cache.guava;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Collections;
import java.util.concurrent.Callable;

import com.google.common.base.Charsets;
import com.google.common.base.Functions;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import com.nesscomputing.cache.CacheModule;
import com.nesscomputing.config.Config;
import com.nesscomputing.config.ConfigModule;
import com.nesscomputing.lifecycle.junit.LifecycleRule;
import com.nesscomputing.lifecycle.junit.LifecycleRunner;
import com.nesscomputing.lifecycle.junit.LifecycleStatement;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.kitei.testing.lessio.AllowDNSResolution;

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
