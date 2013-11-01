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
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;


import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.cache.Cache;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import com.nesscomputing.cache.CacheModule;
import com.nesscomputing.cache.guava.NessGuavaCaches;
import com.nesscomputing.config.Config;
import com.nesscomputing.config.ConfigModule;
import com.nesscomputing.jackson.JacksonSerializerBinder;
import com.nesscomputing.jackson.JsonDeserializerFunction;
import com.nesscomputing.jackson.JsonSerializerFunction;
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
                new CacheModule("test"),
                new NessJacksonModule(),
                NessGuavaCaches.newModuleBuilder("test", "test-ns", BigDecimal.class, DateTime.class)
                    .withKeySerializer(JsonSerializerFunction.class)
                    .withValueSerializer(JsonSerializerFunction.class, JsonDeserializerFunction.class)
                    .build(),
                new AbstractModule() {
                    @Override
                    protected void configure() {
                        JacksonSerializerBinder.builderOf(binder(), BigDecimal.class).bind();
                        JacksonSerializerBinder.builderOf(binder(), DateTime.class).bind();
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
