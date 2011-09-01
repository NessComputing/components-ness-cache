package ness.cache;

import io.trumpet.config.Config;
import io.trumpet.log.Log;

import java.lang.annotation.Annotation;

import com.google.inject.PrivateModule;

public class CacheModule extends PrivateModule {
    private static final Log LOG = Log.findLog();

    private final Annotation bindingAnnotation;
    private final CacheConfiguration config;
    /** Expose additional bindings for integration testing */
    private final boolean exposeInternalClasses;

    public CacheModule(Config config) {
        this (config.getBean(CacheConfiguration.class));
    }

    public CacheModule(CacheConfiguration config) {
        this (config, null);
    }

    public CacheModule(CacheConfiguration config, Annotation bindingAnnotation) {
        this(config, bindingAnnotation, false);
    }

    CacheModule(CacheConfiguration config, Annotation bindingAnnotation, boolean exposeInternalClasses) {
        this.config = config;
        this.bindingAnnotation = bindingAnnotation;
        this.exposeInternalClasses = exposeInternalClasses;
    }

    @Override
    protected void configure() {
        LOG.info("Caching initialize... binding=%s, type=%s", bindingAnnotation, config.getCacheType());

        bind (NessMemcachedConnectionFactory.class);

        if (bindingAnnotation != null) {
            bind (Cache.class).annotatedWith(bindingAnnotation);
            expose (Cache.class).annotatedWith(bindingAnnotation);
        } else {
            bind (Cache.class);
            expose (Cache.class);
        }

        if (config.isJmxEnabled()) {
            bind (CacheStatisticsManager.class).to(JmxCacheStatisticsManager.class);
        } else {
            bind (CacheStatisticsManager.class).to(NullCacheStatisticsManager.class);
        }

        bind (CacheConfiguration.class).toInstance(config);

        // Internal bindings are not exposed, so do not need to be annotated.

        switch (config.getCacheType()) {
        case NONE:
            bind (InternalCacheProvider.class).to(NullProvider.class);
            break;

        case JVM:
            bind (InternalCacheProvider.class).to(JvmCacheProvider.class);
            break;

        case MEMCACHE:
            bind (InternalCacheProvider.class).to(MemcacheProvider.class);
            bind (MemcachedClientFactory.class);
            bind (CacheTopologyProvider.class);
            break;

        default:
            throw new IllegalStateException("Unrecognized cache type " + config.getCacheType());
        }

        if (exposeInternalClasses) {
            expose (MemcachedClientFactory.class);
            expose (CacheTopologyProvider.class);
        }
    }
}
