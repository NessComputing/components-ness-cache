package ness.cache2;

import io.trumpet.config.Config;

import java.lang.annotation.Annotation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.google.inject.util.Providers;
import com.likeness.logging.Log;

public class CacheModule extends PrivateModule {
    private static final Log LOG = Log.findLog();

    private final Annotation bindingAnnotation;
    private final Config config;

    /** Expose additional bindings for integration testing */
    private final boolean exposeInternalClasses;

    /**
     * @deprecated Cache modules must be explicitly named, do not use the default cache anymore.
     */
    @Deprecated
    public CacheModule(final Config config)
    {
       this(config, null, false);
    }

    public CacheModule(Config config, String cacheName)
    {
    	this(config, Names.named(cacheName), false);
    }

    /**
     * @deprecated Use the naming constructor.
     */
    @Deprecated
    public CacheModule(Config config, Annotation bindingAnnotation) {
        this(config, bindingAnnotation, false);
    }

    @VisibleForTesting
    CacheModule(final Config config, final Annotation bindingAnnotation, final boolean exposeInternalClasses) {
        this.config = config;
        this.bindingAnnotation = bindingAnnotation;
        this.exposeInternalClasses = exposeInternalClasses;
    }

    @Override
    protected void configure() {

        final String cacheName;
        final CacheConfiguration cacheConfig;

        if (bindingAnnotation == null) {
            LOG.warn("Starting the default cache instance! This will soon no longer be possible. Update your code to provide a cache name!");

            cacheName = null;
            cacheConfig = config.getBean(CacheConfiguration.class, ImmutableMap.of("cacheName", "default"));

            bind (Cache.class).to(CacheImpl.class);
            expose (Cache.class);
        }
        else {
            cacheName = bindingAnnotation instanceof Named ? ((Named)bindingAnnotation).value() : bindingAnnotation.toString();
            cacheConfig = config.getBean(CacheConfiguration.class, ImmutableMap.of("cacheName", cacheName));

            bind (Cache.class).annotatedWith(bindingAnnotation).to(CacheImpl.class);
            expose (Cache.class).annotatedWith(bindingAnnotation);
        }

        bind(String.class).annotatedWith(Names.named("cacheName")).toProvider(Providers.of(cacheName)).in(Scopes.SINGLETON);
        LOG.info("Caching initialize... binding=%s, type=%s, cacheName=%s", Objects.firstNonNull(bindingAnnotation, "<unset>"), cacheConfig.getCacheType(), Objects.firstNonNull(cacheName, "<default>"));

        bind(CacheConfiguration.class).toInstance(cacheConfig);

        if (cacheConfig.isJmxEnabled()) {
            bind (CacheStatisticsManager.class).to(JmxCacheStatisticsManager.class);
        }

        // Internal bindings are not exposed, so do not need to be annotated.

        switch (cacheConfig.getCacheType()) {
        case NONE:
            bind (InternalCacheProvider.class).to(NullProvider.class);
            break;

        case JVM:
            bind (InternalCacheProvider.class).to(JvmCacheProvider.class);
            break;

        case JVM_NO_EVICTION:
        	bind (InternalCacheProvider.class).to(NonEvictingJvmCacheProvider.class);
        	break;

        case MEMCACHE:
            bind (NessMemcachedConnectionFactory.class);

            bind (InternalCacheProvider.class).to(MemcacheProvider.class);
            bind (MemcachedClientFactory.class);
            bind (CacheTopologyProvider.class);
            break;

        default:
            throw new IllegalStateException("Unrecognized cache type " + cacheConfig.getCacheType());
        }

        if (exposeInternalClasses) {
            expose (MemcachedClientFactory.class);
            expose (CacheTopologyProvider.class);
        }
    }
}
