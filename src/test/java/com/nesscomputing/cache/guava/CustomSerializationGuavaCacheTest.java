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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.concurrent.Callable;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.cache.Cache;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.nesscomputing.cache.CacheModule;
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
                new CacheModule("test"),
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
