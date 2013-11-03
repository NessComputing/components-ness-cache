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

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;

import com.nesscomputing.config.Config;

import org.junit.Test;

public class MultipleModuleTest {
	@Test
	public void testMultipleModules() {
	    final Config config = Config.getFixedConfig("ness.cache", "NONE",
                                                    "ness.cache.noeviction", "JVM_NO_EVICTION",
                                                    "ness.cache.jmx", "false");


		Injector injector = Guice.createInjector(new AbstractModule() {
			@Override
			protected void configure() {
				binder().requireExplicitBindings();
				bind (Config.class).toInstance(config);
			}},
			new CacheModule("test"),
			new CacheModule("noeviction"));

		NessCacheImpl cache = (NessCacheImpl) injector.getInstance(Key.get(NessCache.class, Names.named("test")));
		NessCacheImpl noEvictionCache = (NessCacheImpl) injector.getInstance(Key.get(NessCache.class, Names.named("noeviction")));
		assertTrue(cache.provider instanceof NullProvider);
		assertTrue("Expecting non evicting provider. Got: " +  noEvictionCache.provider.getClass(), noEvictionCache.provider instanceof NonEvictingJvmCacheProvider);
	}
}
