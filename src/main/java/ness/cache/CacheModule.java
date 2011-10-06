package ness.cache;

import io.trumpet.config.Config;
import io.trumpet.log.Log;

import java.lang.annotation.Annotation;

import com.google.common.base.Preconditions;
import com.google.inject.PrivateModule;
import com.google.inject.name.Names;

public class CacheModule extends PrivateModule {
    private static final Log LOG = Log.findLog();

    private final Annotation bindingAnnotation;
    private final CacheConfiguration config;
    /** Use a particular named cache of the type specified in config. Allows you to have two disjoint memcache clusters. */
    private final String cacheName;
    /** Expose additional bindings for integration testing */
    private final boolean exposeInternalClasses;

    public CacheModule(Config config) {
        this (config.getBean(CacheConfiguration.class));
    }
    
    public CacheModule(Config config, String cacheName) {
    	this(config.getBean(CacheConfiguration.class), null, cacheName, false);
    }

    public CacheModule(CacheConfiguration config) {
        this (config, null);
    }

    public CacheModule(CacheConfiguration config, Annotation bindingAnnotation) {
        this(config, bindingAnnotation, null, false);
    }

    CacheModule(CacheConfiguration config, Annotation bindingAnnotation, String cacheName, boolean exposeInternalClasses) {
    	Preconditions.checkArgument(bindingAnnotation == null || cacheName == null, "cacheName overrides bindingAnnotation, so both cannot be set");
        this.config = config;
        if (cacheName != null) {
        	this.bindingAnnotation = Names.named(cacheName);
        } else {
        	this.bindingAnnotation = bindingAnnotation;
        }
        this.exposeInternalClasses = exposeInternalClasses;
        this.cacheName = cacheName;
    }

    @Override
    protected void configure() {
        LOG.info("Caching initialize... binding=%s, type=%s cacheName=%s", bindingAnnotation, config.getCacheType(), cacheName);

        bind (NessMemcachedConnectionFactory.class);

        if (bindingAnnotation != null) {
            bind (Cache.class).annotatedWith(bindingAnnotation).to(Cache.class);
            expose (Cache.class).annotatedWith(bindingAnnotation);
        } else {
            bind (Cache.class);
            expose (Cache.class);
        }
        
        if (cacheName != null) {
        	bind (String.class).annotatedWith(Names.named("cacheName")).toInstance(cacheName);
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
