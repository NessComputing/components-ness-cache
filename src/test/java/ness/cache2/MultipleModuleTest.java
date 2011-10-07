package ness.cache2;

import static org.junit.Assert.assertTrue;

import ness.cache2.Cache;
import ness.cache2.CacheConfiguration;
import ness.cache2.CacheImpl;
import ness.cache2.CacheModule;
import ness.cache2.NonEvictingJvmCacheProvider;
import ness.cache2.NullProvider;

import org.junit.Test;

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
		Injector injector = Guice.createInjector(new AbstractModule() {
			@Override
			protected void configure() {
				binder().requireExplicitBindings();
			}}, 
			new CacheModule(CacheConfiguration.NONE_NO_JMX), 
				new CacheModule(CacheConfiguration.IN_JVM_NO_EVICTION, "noeviction"));
		CacheImpl cache = (CacheImpl) injector.getInstance(Cache.class);
		CacheImpl noEvictionCache = (CacheImpl) injector.getInstance(Key.get(Cache.class, Names.named("noeviction")));
		assertTrue(cache.provider instanceof NullProvider);
		assertTrue("Expecting non evicting provider. Got: " +  noEvictionCache.provider.getClass(), noEvictionCache.provider instanceof NonEvictingJvmCacheProvider);
	}
}
