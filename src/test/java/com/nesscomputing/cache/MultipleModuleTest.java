package com.nesscomputing.cache;

import static org.junit.Assert.assertTrue;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;

import org.junit.Test;

import com.nesscomputing.cache.CacheModule;
import com.nesscomputing.cache.NessCache;
import com.nesscomputing.cache.NessCacheImpl;
import com.nesscomputing.cache.NonEvictingJvmCacheProvider;
import com.nesscomputing.cache.NullProvider;
import com.nesscomputing.config.Config;

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
