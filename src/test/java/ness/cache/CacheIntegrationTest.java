package ness.cache;

import java.net.URI;
import java.util.Collections;
import java.util.List;

import io.trumpet.lifecycle.Lifecycle;
import io.trumpet.lifecycle.LifecycleStage;
import io.trumpet.lifecycle.guice.LifecycleModule;

import org.junit.After;
import org.junit.Before;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;

public class CacheIntegrationTest extends BaseCacheIntegrationSetup {
    @Inject
    Lifecycle lifecycle;
    
    final CacheConfiguration configuration = new CacheConfiguration() {
        @Override
        public CacheType getCacheType() {
            return CacheType.MEMCACHE;
        }
        @Override
        public boolean isCacheSynchronous() {
            return true;
        }
        @Override
        public List<URI> getCacheUri() {
            return Collections.singletonList(URI.create("memcache://localhost:11212"));
        }
    };
    
    @Before
    public final void setUpClient() {
        Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                requestInjection (CacheIntegrationTest.this);
                
                install (new CacheModule(configuration));
                
                install (new LifecycleModule());
            }
        });
        lifecycle.executeTo(LifecycleStage.START_STAGE);
    }
    
    @After
    public final void stopLifecycle() {
        lifecycle.executeTo(LifecycleStage.STOP_STAGE);
    }
}
