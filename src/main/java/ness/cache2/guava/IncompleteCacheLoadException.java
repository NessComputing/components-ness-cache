package ness.cache2.guava;

/**
 * Thrown if a CacheLoader does not provide all requested keys in response to a getAll invocation.
 */
public class IncompleteCacheLoadException extends RuntimeException
{
    private static final long serialVersionUID = 1L;

    public IncompleteCacheLoadException(String message)
    {
        super(message);
    }

    public IncompleteCacheLoadException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
