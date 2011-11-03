package ness.cache2;

import static org.junit.Assert.assertTrue;
import io.trumpet.config.Config;
import io.trumpet.config.guice.TestingConfigModule;

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
import com.google.inject.Module;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;

/**
 * @author christopher
 *
 */
public class MultipleModuleTest {
	@Test
	public void testMultipleModules() {
	    final TestingConfigModule tcm = new TestingConfigModule();
        final Config config = tcm.getConfig();

        final Module testModule1 = Modules.override(new CacheModule(config)).with(new AbstractModule() {
            @Override
            public void configure()
            {
                bind(CacheConfiguration.class).toInstance(CacheConfiguration.NONE_NO_JMX);
            }
        });

        final Module testModule2 = Modules.override(new CacheModule(config, "noeviction")).with(new AbstractModule() {
            @Override
            public void configure()
            {
                bind(CacheConfiguration.class).toInstance(CacheConfiguration.IN_JVM_NO_EVICTION);
            }
        });

		Injector injector = Guice.createInjector(new AbstractModule() {
			@Override
			protected void configure() {
				binder().requireExplicitBindings();
			}},
			tcm,
			testModule1,
			testModule2);

		CacheImpl cache = (CacheImpl) injector.getInstance(Cache.class);
		CacheImpl noEvictionCache = (CacheImpl) injector.getInstance(Key.get(Cache.class, Names.named("noeviction")));
		assertTrue(cache.provider instanceof NullProvider);
		assertTrue("Expecting non evicting provider. Got: " +  noEvictionCache.provider.getClass(), noEvictionCache.provider instanceof NonEvictingJvmCacheProvider);
	}
}
