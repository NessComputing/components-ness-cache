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
package com.nesscomputing.cache;

import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Collections;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import com.nesscomputing.config.Config;
import com.nesscomputing.lifecycle.Lifecycle;
import com.nesscomputing.lifecycle.LifecycleStage;
import com.nesscomputing.lifecycle.guice.LifecycleModule;

import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class NullCacheTest {

    @Inject
    @Named("test")
    NessCache cache;

    @Inject
    Lifecycle lifecycle;

    @Before
    public final void setUpClient() {
        final Config config = Config.getFixedConfig("ness.cache", "NONE",
                                                    "ness.cache.jmx", "false");

        Guice.createInjector(new CacheModule("test"),
                             new LifecycleModule(),
                             new AbstractModule() {
            @Override
            protected void configure() {
                requestInjection (NullCacheTest.this);
                bind (Config.class).toInstance(config);
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
