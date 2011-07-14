package ness.cache;

import java.lang.annotation.Annotation;

import com.google.inject.PrivateModule;

public class CacheModule extends PrivateModule {
    
    private final Annotation bindingAnnotation;
    private final CacheConfiguration config;

    public CacheModule(CacheConfiguration config) {
        this (config, null);
    }
    
    public CacheModule(CacheConfiguration config, Annotation bindingAnnotation) {
        this.config = config;
        this.bindingAnnotation = bindingAnnotation;
    }
    
    @Override
    protected void configure() {
        if (bindingAnnotation != null) {
            bind (Cache.class).annotatedWith(bindingAnnotation);
            expose (Cache.class).annotatedWith(bindingAnnotation);
        } else {
            bind (Cache.class);
            expose (Cache.class);
        }
        
        bind (CacheStatisticsManager.class);
        
        bind (CacheConfiguration.class).toInstance(config);
        
        // Internal bindings are not exposed, so do not need to be annotated.
        
        switch (config.getCacheType()) {
        case NONE:
        case none:
            bind (InternalCacheProvider.class).to(NullProvider.class);
            break;
            
        case JVM:
        case jvm:
            bind (InternalCacheProvider.class).to(JvmCacheProvider.class);
            break;
            
        case MEMCACHE:
        case memcache:
            bind (InternalCacheProvider.class).to(MemcacheProvider.class);
            bind (MemcachedClientFactory.class);
            bind (CacheTopologyProvider.class);
            break;
            
        default:
            throw new IllegalStateException("Unrecognized cache type " + config.getCacheType());
        }
        
    }
}
