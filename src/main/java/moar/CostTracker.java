package moar;

import static java.util.Collections.unmodifiableMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class CostTracker {
  private static final PropertyAccessor props = new PropertyAccessor(CostTracker.class);
  private static final long bucketSize = props.getLong("bucketSize", 1000L);
  private final String description;
  private final AtomicLong max, count, total;
  private final Map<Long, AtomicLong> buckets = new ConcurrentHashMap<>();

  public CostTracker(final String description) {
    count = new AtomicLong();
    max = new AtomicLong();
    this.description = description;
    total = new AtomicLong();
  }

  public void add(final Long elapsed) {
    updateMax(elapsed);
    count.incrementAndGet();
    total.addAndGet(elapsed);
    final Long key = elapsed / bucketSize;
    getBucket(key).incrementAndGet();
  }

  private AtomicLong getBucket(final Long key) {
    final AtomicLong bucket = getBuckets().get(key);
    if (bucket == null) {
      final AtomicLong newBucket = new AtomicLong();
      buckets.put(key, newBucket);
      return newBucket;
    }
    return bucket;
  }

  public Map<Long, AtomicLong> getBuckets() {
    return unmodifiableMap(buckets);
  }

  public Long getCount() {
    return count.get();
  }

  public String getDescription() {
    return description;
  }

  public Long getMax() {
    return max.get();
  }

  public Long getTotal() {
    return total.get();
  }

  private void updateMax(final long elapsed) {
    final long currentMax = max.get();
    while (!max.compareAndSet(currentMax, Math.max(currentMax, elapsed))) {
      // noop, the only way we loop is if another thread is hitting
      // this same cost tracker.
    }
    ;
  }

}
