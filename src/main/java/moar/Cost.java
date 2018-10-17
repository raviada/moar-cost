package moar;

import static java.lang.Math.min;
import static java.lang.System.currentTimeMillis;
import static moar.Exceptional.require;
import static moar.JsonUtil.debug;
import static moar.JsonUtil.toJson;
import static moar.JsonUtil.trace;
import static moar.JsonUtil.warn;
import static org.slf4j.LoggerFactory.getLogger;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.URI;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * In software time is money!
 * <p>
 * The unit of cost is a millisecond. Methods here help track cost with the ability to roll up activities that involve
 * multiple threads.
 */
public class Cost {
  private static class Activity {
    private final Map<String, CostTracker> costMap = new ConcurrentHashMap<>();
    private final long start = currentTimeMillis();
    private final AtomicLong cost = new AtomicLong();

    void accumulate(final String description, final long elapsed) {
      synchronized (costMap) {
        if (!costMap.containsKey(description)) {
          costMap.put(description, new CostTracker(description));
        }
        final CostTracker mapEntry = costMap.get(description);
        mapEntry.add(elapsed);
        cost.addAndGet(elapsed);
      }
    }

    CostReport describe() {
      final long cost = currentTimeMillis() - start;
      final List<CostTracker> sortedCosts = new ArrayList<>();
      for (final String desc : costMap.keySet()) {
        final CostTracker entry = costMap.get(desc);
        sortedCosts.add(entry);
      }
      Collections.sort(sortedCosts, (o1, o2) -> {
        final Long o1Cost = o1.getMax();
        final Long o2Cost = o2.getMax();
        if (o1Cost == o2Cost) {
          return 1;
        }
        if (o1Cost > o2Cost) {
          return 1;
        }
        return -1;
      });
      return new CostReport(cost, sortedCosts);
    }
  }

  public interface AsyncProvider {
    <T> Future<T> submit(final Callable<T> c);
  }

  private static final Logger LOG = getLogger(Cost.class.getName());
  private static final PropertyAccessor prop = new PropertyAccessor(Cost.class.getName());
  private final static boolean asyncEnabled = prop.getBoolean("async", true);
  private static final long TRACE_COST_LIMIT = prop.getLong("traceCostLimit", 10 * 1000L);
  private static final ThreadLocal<Activity> threadActivity = new ThreadLocal<>();
  private static final ThreadLocal<Boolean> threadIsAsync = new ThreadLocal<>();
  private static final ListeningExecutorService directExecutorService = MoreExecutors.newDirectExecutorService();
  private static boolean trackCosts = prop.getBoolean("trackCosts", true);
  private static boolean trackDetailCosts = prop.getBoolean("trackDetailCosts", true);
  private static AsyncProvider directAsyncProvider = new Cost.AsyncProvider() {
    @Override
    public <T> Future<T> submit(final Callable<T> c) {
      return directExecutorService.submit(c);
    }
  };

  public static Cost $() {
    return new Cost();
  }

  /**
   * Submit a callable
   */
  public static <T> Future<T> $(final AsyncProvider provider, final Callable<T> callable) {
    final Activity parentActivity = threadActivity.get();
    return resolve(provider).submit(() -> {
      threadIsAsync.set(true);
      final Activity priorActivity = threadActivity.get();
      try {
        threadActivity.set(parentActivity);
        return callable.call();
      } finally {
        threadActivity.set(priorActivity);
        threadIsAsync.set(false);
      }
    });
  }

  /**
   * submit a runnable
   */
  public static <T> Future<T> $(final AsyncProvider provider, final Runnable runnable) {
    return $(provider, () -> {
      runnable.run();
      return null;
    });
  }

  public static <T> T $(final Callable<T> call) throws Exception {
    return $($(1), call);
  }

  /**
   * Get a single futures result
   */
  public static <T> T $(final Future<T> future) {
    return (T) require(() -> {
      return future.get();
    });
  }

  public static final String $(final int offset) {
    return Exceptional.$(offset);
  }

  /**
   * Complete a batch of futures
   * <p>
   * Wait for all the futures to return. If any futures have exceptions a single {@link FutureListException} is thrown
   * with the results of the batch.
   */
  public static <T> List<T> $(final List<Future<T>> futures) throws Exception {
    return $("futures " + $(1), () -> {
      final List<T> resultList = new ArrayList<>();
      final List<Two<Object, Exception>> resultWithExceptions = new ArrayList<>();
      Exception exception = null;
      for (final Future<T> future : futures) {
        T result;
        try {
          result = future.get();
          exception = null;
        } catch (final Exception e) {
          exception = e;
          result = null;
        }
        resultList.add(result);
        resultWithExceptions.add(new Two<>(result, exception));
      }
      if (exception != null) {
        throw new FutureListException(resultWithExceptions);
      }
      return resultList;
    });
  }

  public static void $(final Runnable r) {
    $($(1), r);
  }

  /**
   * Add a descriptive about the current cost.
   */
  public static void $(final String description) {
    debug(LOG, $(1), description);
  }

  /**
   * Call something
   */
  public static <T> T $(final String description, final Callable<T> callable) throws Exception {
    if (!trackDetailCosts) {
      return callable.call();
    }
    if (threadActivity.get() == null) {
      return callable.call();
    }
    final long clock = currentTimeMillis();
    try {
      return callable.call();
    } finally {
      final long cost = currentTimeMillis() - clock;
      accumulate(description, cost);
      if (cost < TRACE_COST_LIMIT) {
        trace(LOG, cost, description);
      } else {
        debug(LOG, cost, description);
      }
    }
  }

  /**
   * Create a proxy to track the cost of the methods
   */
  public static <T> T $(final String generalDescription, final Class<?> clz, final T r) {
    if (!trackDetailCosts) {
      return r;
    }
    final String simpleName = clz.getSimpleName();
    if (!clz.isInterface()) {
      warn(LOG, clz.getSimpleName(), "Unable to track cost because it is not an interface");
      return r;
    }
    final ClassLoader c = Cost.class.getClassLoader();
    final Class<?>[] cc = { clz };
    return (T) Proxy.newProxyInstance(c, cc, (proxy, method, args) -> {
      String desc;
      if (isRestExchange(r, method, args)) {
        final URI uri = (URI) args[0];
        desc = uri.toString();
      } else if (clz == Statement.class && method.getName().equals("execute") && args.length == 1
          && args[0] instanceof String) {
        final String sql = (String) args[0];
        desc = sql.substring(0, min(sql.length(), 40));
      } else {
        final List<String> pTypes = new ArrayList<>();
        for (final Class<?> p : method.getParameterTypes()) {
          pTypes.add(p.getSimpleName());
        }
        desc = toJson(simpleName, method.getName(), pTypes);
      }
      try {
        return $(generalDescription, () -> $(desc, () -> method.invoke(r, args)));
      } catch (final UndeclaredThrowableException e1) {
        throw e1.getCause();
      } catch (final InvocationTargetException e2) {
        throw e2.getCause();
      } catch (final Exception e3) {
        throw e3;
      }
    });
  }

  /**
   * Run something
   */
  public static void $(final String description, final Runnable runnable) {
    if (!trackDetailCosts) {
      runnable.run();
      return;
    }
    require(() -> {
      $(description, () -> {
        runnable.run();
        return null;
      });
    });
  }

  /**
   * Track the cost of an activity and with a scope that follows work across threads.
   */
  public static CostReport $$(final Runnable r) throws Exception {
    if (!trackCosts) {
      // Skip the cost/complexity if we are not tracking detail level costs
      r.run();
      return new CostReport(0, Collections.EMPTY_LIST);
    }
    final Activity priorActity = threadActivity.get();
    final Activity activity = new Activity();
    try {
      threadActivity.set(activity);
      r.run();
    } finally {
      threadActivity.set(priorActity);
    }
    return activity.describe();
  }

  /**
   * Accumulate costs based on description
   */
  private static void accumulate(final String description, final long elapsed) {
    threadActivity.get().accumulate(description, elapsed);
  }

  /**
   * Detect rest exchange so we can provide better descriptions.
   */
  private static <T> boolean isRestExchange(final T r, final Method method, final Object[] args) {
    if (method.getName().equals("exchange") && args != null) {
      if (args.length == 4 && args[0] instanceof URI) {
        return true;
      }
    }
    return false;
  }

  private static AsyncProvider resolve(final AsyncProvider async) {
    if (!asyncEnabled) {
      return directAsyncProvider;
    }
    return async;
  }

  public static void setTrackCosts(final boolean trackCosts) {
    Cost.trackCosts = trackCosts;
  }

  public static void setTrackDetailCosts(final boolean trackDetailCosts) {
    Cost.trackDetailCosts = trackDetailCosts;
  }

}