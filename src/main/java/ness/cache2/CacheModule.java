package ness.cache2;

import java.lang.annotation.Annotation;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.name.Names;
import com.nesscomputing.config.Config;
import com.nesscomputing.config.ConfigProvider;
import com.nesscomputing.logging.Log;

@SuppressWarnings("deprecation")
public class CacheModule extends AbstractModule {
    private static final Log LOG = Log.findLog();

    private final String cacheName;
    private final Annotation bindingAnnotation;

    private volatile Injector childInjector;

    public CacheModule(String cacheName)
    {
        this(null, cacheName);
    }

    /**
     * @deprecated don't pass in a Config object.
     */
    @Deprecated
    public CacheModule(Config config, String cacheName)
    {
        Preconditions.checkArgument(!StringUtils.isBlank(cacheName), "blank cache name");
        this.cacheName = cacheName;
        bindingAnnotation = Names.named(cacheName);
    }

    @SuppressWarnings("deprecation")
    @Override
    protected void configure() {
        binder().disableCircularProxies();
        binder().requireExplicitBindings();

        bind (CacheConfiguration.class).annotatedWith(bindingAnnotation).toProvider(ConfigProvider.of(CacheConfiguration.class, ImmutableMap.of("cacheName", cacheName)));

        bind (Cache.class).annotatedWith(bindingAnnotation).toProvider(new NessCacheProvider());
        bind (NessCache.class).annotatedWith(bindingAnnotation).to(Key.get(Cache.class, bindingAnnotation));
    }

    class NessCacheProvider implements Provider<Cache>
    {

        private Injector injector;

        @Inject
        void setInjector(Injector injector)
        {
            this.injector = injector;
        }

        @Override
        public Cache get()
        {
            Preconditions.checkNotNull(injector, "no injector injected?");
            CacheConfiguration cacheConfig = injector.getInstance(Key.get(CacheConfiguration.class, bindingAnnotation));
            childInjector = injector.createChildInjector(
                    getRealCacheModule(cacheConfig),
                    getInternalCacheModule(cacheConfig));
            return childInjector.getInstance(Cache.class);
        }
    }

    Injector getChildInjector()
    {
        return Preconditions.checkNotNull(childInjector, "NessCache not injected yet");
    }

    Module getInternalCacheModule(CacheConfiguration cacheConfig)
    {
        LOG.info("Caching initialize... binding=%s, type=%s, cacheName=%s", Objects.firstNonNull(bindingAnnotation, "<unset>"), cacheConfig.getCacheType(), Objects.firstNonNull(cacheName, "<default>"));

        switch (cacheConfig.getCacheType()) {
        case NONE:
            return new NullCacheModule();

        case JVM:
            return new JvmCacheModule();

        case JVM_NO_EVICTION:
            return new NonEvictingJvmCacheModule();

        case MEMCACHE:
            return new MemcacheCacheModule();

        default:
            throw new IllegalStateException("Unrecognized cache type " + cacheConfig.getCacheType());
        }
    }

    private Module getRealCacheModule(final CacheConfiguration cacheConfig)
    {
        return new AbstractModule()
        {
            @Override
            protected void configure()
            {
                binder().requireExplicitBindings();
                binder().disableCircularProxies();

                bind (CacheConfiguration.class).toInstance(cacheConfig);
                bindConstant().annotatedWith(Names.named("cacheName")).to(cacheName);

                bind (NessCache.class).to(Cache.class);
                bind (Cache.class).to(NessCacheImpl.class);

                if (cacheConfig.isJmxEnabled())
                {
                    bind (CacheStatisticsManager.class).to(JmxCacheStatisticsManager.class);
                }
            }
        };
    }

    static class NullCacheModule extends AbstractModule
    {
        @Override
        protected void configure()
        {
            bind (InternalCacheProvider.class).to(NullProvider.class);
        }
    }

    static class JvmCacheModule extends AbstractModule
    {
        @Override
        protected void configure()
        {
            bind (InternalCacheProvider.class).to(JvmCacheProvider.class);
        }
    }

    static class NonEvictingJvmCacheModule extends AbstractModule
    {
        @Override
        protected void configure()
        {
            bind (InternalCacheProvider.class).to(NonEvictingJvmCacheProvider.class);
        }
    }

    static class MemcacheCacheModule extends AbstractModule
    {
        @Override
        protected void configure()
        {
            bind (InternalCacheProvider.class).to(MemcacheProvider.class);
            bind (NessMemcachedConnectionFactory.class);
            bind (MemcachedClientFactory.class);
            bind (CacheTopologyProvider.class);
        }
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((bindingAnnotation == null) ? 0 : bindingAnnotation.hashCode());
        result = prime * result + ((cacheName == null) ? 0 : cacheName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        CacheModule other = (CacheModule) obj;
        if (bindingAnnotation == null) {
            if (other.bindingAnnotation != null)
                return false;
        } else if (!bindingAnnotation.equals(other.bindingAnnotation))
            return false;
        if (cacheName == null) {
            if (other.cacheName != null)
                return false;
        } else if (!cacheName.equals(other.cacheName))
            return false;
        return true;
    }
}
