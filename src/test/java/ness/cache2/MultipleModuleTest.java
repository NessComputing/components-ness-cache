package ness.cache2;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import com.nesscomputing.config.Config;
import com.nesscomputing.config.ConfigModule;

/**
 * @author christopher
 *
 */
public class MultipleModuleTest {
	@Test
	public void testMultipleModules() {
	    final Config config = Config.getFixedConfig(ImmutableMap.of("ness.cache", "NONE",
	                                                                            "ness.cache.noeviction", "JVM_NO_EVICTION",
	                                                                            "ness.cache.jmx", "false"));
		Injector injector = Guice.createInjector(new AbstractModule() {
			@Override
			protected void configure() {
				binder().requireExplicitBindings();
			}},
			new ConfigModule(config),
			new CacheModule(config, "test"),
			new CacheModule(config, "noeviction"));

		CacheImpl cache = (CacheImpl) injector.getInstance(Key.get(Cache.class, Names.named("test")));
		CacheImpl noEvictionCache = (CacheImpl) injector.getInstance(Key.get(Cache.class, Names.named("noeviction")));
		assertTrue(cache.provider instanceof NullProvider);
		assertTrue("Expecting non evicting provider. Got: " +  noEvictionCache.provider.getClass(), noEvictionCache.provider instanceof NonEvictingJvmCacheProvider);
	}
}
