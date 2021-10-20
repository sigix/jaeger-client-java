package io.jaegertracing.internal.reporters;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import io.jaegertracing.internal.JaegerSpan;
import io.jaegertracing.internal.JaegerSpanContext;
import io.jaegertracing.spi.Reporter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * FilteringReporter drops spans below a specific threshold and buffers the remaining spans in memory and
 * sends them to a delegate only once a parent span exceeds a specified threshold.
 */
@ToString
@Slf4j
public class FilteringReporter implements Reporter {
  public static final long DEFAULT_FILTER_SPANS_UNDER_MICROS = 0L;
  public static final long DEFAULT_DEFER_SPANS_UNDER_MICROS = 0L;

  private final Reporter delegate;
  private final long filterSpansUnderMicros;
  private final long deferSpansUnderMicros;

  private final Map<Long,List<JaegerSpan>> pending = new ConcurrentHashMap<>();
  private final Metrics metrics;

  public interface Metrics {
    default Metrics set(Gauge gauge, long amount) {
      return this;
    }

    default Metrics increment(Gauge gauge) {
      return this;
    }

    default Metrics decrement(Gauge gauge) {
      return this;
    }

    default Metrics decrement(Gauge gauge, long amount) {
      return this;
    }

    default Metrics increment(Counter counter) {
      return this;
    }

    default Metrics increment(Counter counter, long amount) {
      return this;
    }
  }

  public static final Metrics NOOP_METRICS = new Metrics() {
  };

  public enum Gauge {
    PARENTS_WITH_PENDING, // G parent spans collecting
    PENDING_SPANS, // G total pending spans
  }

  private static final LongAdder PENDING_SPANS = new LongAdder();

  public enum Counter {
    FILTERED_SPANS, // C
    DEFERRED_SPANS, // C
    DEFERRED_SPANS_DROPPED, // C - this and sent only count spans that were at one time pending
    DEFERRED_SPANS_SENT, // C   DEFERRED_SPANS_DROPPED + DEFERRED_SPANS_SENT + PENDING_SPANS = DEFERRED_SPANS
    ROOT_SPANS_DROPPED
    // median size of pending list per parent?
  }

  public FilteringReporter(
      Reporter delegate, long filterSpansUnderMicros, long deferSpansUnderMicros, Metrics filteringMetrics) {
    this.delegate = delegate;
    this.filterSpansUnderMicros = filterSpansUnderMicros;
    this.deferSpansUnderMicros = deferSpansUnderMicros;
    this.metrics = filteringMetrics;
  }

  @Override
  public void report(JaegerSpan span) {
    final JaegerSpanContext context = span.context();
    final List<JaegerSpan> pendingChildren = pending.remove(context.getSpanId());
    if (pendingChildren != null) {
      metrics.decrement(Gauge.PARENTS_WITH_PENDING);
    }

    if (span.getDuration() < filterSpansUnderMicros) {
      metrics.increment(Counter.FILTERED_SPANS); // assumes pendingChildren == null
      return;
    }

    if (span.getDuration() < deferSpansUnderMicros) {
      defer(span, context, pendingChildren);
    } else {
      // report pending if any, then this span
      if (pendingChildren != null) {
        pendingChildren.stream().forEachOrdered(pendingSpan -> delegate.report(pendingSpan));
        final int count = pendingChildren.size();
        metrics.decrement(Gauge.PENDING_SPANS, count).increment(Counter.DEFERRED_SPANS_SENT, count);
      }
      delegate.report(span);
    }
  }

  private void defer(JaegerSpan span, JaegerSpanContext context, List<JaegerSpan> pendingChildren) {
    final long parentId = context.getParentId();
    if (parentId != 0) {
      metrics.increment(Gauge.PENDING_SPANS).increment(Counter.DEFERRED_SPANS);
      pending.compute(parentId, (pId, spans) -> {
        if (spans == null) {
          metrics.increment(Gauge.PARENTS_WITH_PENDING);
          if (pendingChildren == null) {
            spans = new ArrayList<>(1);
          } else {
            spans = pendingChildren;
          }
        } else {
          if (pendingChildren != null) {
            spans.addAll(pendingChildren);
          }
        }
        spans.add(span);
        return spans;
      });
    } else {
      metrics.increment(Counter.ROOT_SPANS_DROPPED);
      if (pendingChildren != null) {
        final int count = pendingChildren.size();
        metrics.decrement(Gauge.PENDING_SPANS, count).increment(Counter.DEFERRED_SPANS_DROPPED, count);
      }
    }
  }

  @Override
  public void close() {
    pending.clear();
    delegate.close();
  }
}
