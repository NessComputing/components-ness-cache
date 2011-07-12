package ness.cache;

import static org.junit.Assert.*;

import java.util.Collection;
import java.util.Collections;

import io.trumpet.lifecycle.Lifecycle;
import io.trumpet.lifecycle.LifecycleStage;
import io.trumpet.lifecycle.guice.LifecycleModule;

import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;

public class NullCacheTest {
    
    @Inject
    Cache cache;
    
    @Inject
    Lifecycle lifecycle;
    
    @Before
    public final void setUpClient() {
        Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                requestInjection (NullCacheTest.this);
                
                install (new CacheModule(new CacheConfiguration() {
                    @Override
                    public CacheType getCacheType() {
                        return CacheType.NONE;
                    }
                }));
                
                install (new LifecycleModule());
            }
        });
        
        lifecycle.executeTo(LifecycleStage.START_STAGE);
    }
    
    @After
    public final void stopLifecycle() {
        lifecycle.executeTo(LifecycleStage.STOP_STAGE);
    }
    
    @Test
    public void testNoCache() {
        String ns = "a";
        Collection<String> b = Collections.singleton("b");
        assertTrue(cache.get(ns, b).isEmpty());
        cache.set(ns, Collections.singletonMap("b", new CacheStore(new byte[1], new DateTime().plusMinutes(1))));
        assertTrue(cache.get(ns, b).isEmpty());
        cache.clear(ns, b);
        assertTrue(cache.get(ns, b).isEmpty());
    }
}
