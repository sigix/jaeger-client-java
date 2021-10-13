package io.jaegertracing.internal.reporters;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import io.jaegertracing.internal.JaegerSpan;
import io.jaegertracing.internal.JaegerSpanContext;
import io.jaegertracing.spi.Reporter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * DeferringReporter buffers spans in memory and sends them to a delegate only if the top level span exceeds
 * a specified threshold.
 */
@ToString
@Slf4j
public class DeferringReporter implements Reporter {
  private static final long DEFAULT_MINIMUM_DURATION = TimeUnit.SECONDS.toMicros(5);

  private final Reporter delegate;
  private final long minimumDuration;
  private final Map<Long,List<JaegerSpan>> pending = new ConcurrentHashMap<>();

  public DeferringReporter(Reporter delegate) {
    this.delegate = delegate;
    this.minimumDuration = DEFAULT_MINIMUM_DURATION;
  }

  public DeferringReporter(Reporter delegate, final long minimumDurationMicros) {
    this.delegate = delegate;
    this.minimumDuration = minimumDurationMicros;
  }

  @Override
  public void report(JaegerSpan span) {
    // LATER: metrics throughout (dropped pending, reported pending batch size, etc)

    /**
     * Random Notes:
     *
     * Maintain a mapping of parent->span (collection? lift up? support multi-layer?)... sentinel collection
     * that marks "send it". If a span is reported and it is complete, if it is over the threshold send it and
     * any currently accumulated spans; add marker that additional children should be sent immediately.
     *
     * Is there any ordering guarantee on when spans are reported? Are the ever reported before all children
     * are reported? Ever reported when still open? Is that even allowed?
     *
     *
     * Only finished spans are reported. So on report: if exceeds threshold, report all accumulated child spans
     * and then this one; else accumulate. How do we key? When do we purge stale accumulated spans? I guess we
     * know when the top level span is reported; at that point we either fish or cut bait.
     *
     * In JaegerSpan internals, references can contain more than one parent; context.parentId appears to be
     * immutable though so... should be ok?
     */

    final JaegerSpanContext context = span.context();
    final List<JaegerSpan> pendingChildren = pending.remove(context.getSpanId());

    if (span.getDuration() < minimumDuration) {
      final long parentId = context.getParentId();
      if (parentId != 0) {
        // Create/add to pending
        pending.compute(parentId, (pId, spans) -> {
          if (spans == null) {
            // LATER: choose a good starting size
            spans = new ArrayList<>();
          }
          if (pendingChildren != null) {
            spans.addAll(pendingChildren);
          }
          spans.add(span);
          return spans;
        });
      }
    } else {
      // report pending if any, then this
      if (pendingChildren != null) {
        pendingChildren.stream().forEachOrdered(pendingSpan -> delegate.report(pendingSpan));
      }
      delegate.report(span);
    }
  }

  @Override
  public void close() {
    pending.clear();
    delegate.close();
  }
}
