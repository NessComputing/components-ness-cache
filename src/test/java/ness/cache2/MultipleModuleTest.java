package ness.cache2;

import static org.junit.Assert.assertTrue;
import io.trumpet.config.Config;
import io.trumpet.config.guice.TestingConfigModule;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;

/**
 * @author christopher
 *
 */
public class MultipleModuleTest {
	@Test
	public void testMultipleModules() {
	    final TestingConfigModule tcm = new TestingConfigModule(ImmutableMap.of("ness.cache", "NONE",
	                                                                            "ness.cache.noeviction", "JVM_NO_EVICTION",
	                                                                            "ness.cache.jmx", "false"));
        final Config config = tcm.getConfig();

		Injector injector = Guice.createInjector(new AbstractModule() {
			@Override
			protected void configure() {
				binder().requireExplicitBindings();
			}},
			tcm,
			new CacheModule(config, "test"),
			new CacheModule(config, "noeviction"));

		CacheImpl cache = (CacheImpl) injector.getInstance(Key.get(Cache.class, Names.named("test")));
		CacheImpl noEvictionCache = (CacheImpl) injector.getInstance(Key.get(Cache.class, Names.named("noeviction")));
		assertTrue(cache.provider instanceof NullProvider);
		assertTrue("Expecting non evicting provider. Got: " +  noEvictionCache.provider.getClass(), noEvictionCache.provider instanceof NonEvictingJvmCacheProvider);
	}
}
