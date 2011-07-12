package ness.cache;

import io.trumpet.lifecycle.Lifecycle;
import org.easymock.EasyMock;
import org.junit.Before;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;

public class JvmCacheTest extends BaseCachingTests {
    @Inject
    Lifecycle lifecycle;
    
    @Before
    public final void setUpClient() {
        Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                requestInjection (JvmCacheTest.this);
                
                install (new CacheModule(new CacheConfiguration() {
                    @Override
                    public CacheType getCacheType() {
                        return CacheType.JVM;
                    }
                    @Override
                    public boolean isJmxEnabled() {
                        return false;
                    }
                }));
                
                bind (Lifecycle.class).toInstance(EasyMock.createMock(Lifecycle.class));
            }
        });
    }
}
