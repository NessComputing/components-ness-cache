package ness.cache2;

import com.likeness.logging.Log;

import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.annotation.Nullable;

import org.weakref.jmx.JmxException;
import org.weakref.jmx.MBeanExporter;

import net.spy.memcached.KetamaConnectionFactory;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.OperationFactory;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.protocol.binary.BinaryMemcachedNodeImpl;
import net.spy.memcached.protocol.binary.BinaryOperationFactory;
import net.spy.memcached.transcoders.Transcoder;

import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * A ConnectionFactory which is Ketama and Binary capable, and uses the custom Ness transcoder.
 */
public class NessMemcachedConnectionFactory extends KetamaConnectionFactory {

    private static final Log LOG = Log.findLog();

    private final CacheConfiguration configuration;
    private final String cacheName;
    private MBeanExporter exporter = null;

    @Inject
    NessMemcachedConnectionFactory(final CacheConfiguration configuration,
                                   @Nullable @Named("cacheName") final String cacheName) {
        this.configuration = configuration;
        this.cacheName = cacheName == null ? "[default]" : cacheName;
    }

    @Inject(optional=true)
    void injectMBeanExporter(final MBeanExporter exporter)
    {
        this.exporter = exporter;
    }

    // Use our custom transcoder

    @SuppressWarnings(value = {"unchecked", "rawtypes"} )
    @Override
    public Transcoder<Object> getDefaultTranscoder() {
        return (Transcoder) new MemcacheByteArrayTranscoder();
    }

    // C+P from BinaryConnectionFactory below

    @Override
    public MemcachedNode createMemcachedNode(SocketAddress sa,
            SocketChannel c, int bufSize) {
        boolean doAuth = false;
        final MemcachedNode node = new BinaryMemcachedNodeImpl(sa, c, bufSize,
            new LinkedBlockingQueue<Operation>(configuration.getReadQueueSize()),
            new LinkedBlockingQueue<Operation>(configuration.getWriteQueueSize()),
            new ArrayBlockingQueue<Operation>(configuration.getIncomingQueueSize()),
            configuration.getOperationQueueBlockTime().getMillis(),
            doAuth,
            getOperationTimeout());

        if (exporter != null) {
            final String name = String.format("ness.memcached:cache=%s,node=%s", cacheName, sa.toString());
            try {
                exporter.export(name, node);
            }
            catch (JmxException je) {
                LOG.warnDebug(je, "Could not export new memcached node for %s", sa);
            }
        }

        return node;
    }

    @Override
    public OperationFactory getOperationFactory() {
        return new BinaryOperationFactory();
    }

    @Override
    public boolean isDaemon()
    {
        return configuration.isDaemonThreads();
    }
}
