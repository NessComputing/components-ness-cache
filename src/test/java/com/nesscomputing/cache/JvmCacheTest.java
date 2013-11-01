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
