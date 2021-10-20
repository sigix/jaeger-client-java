package io.jaegertracing.internal.reporters;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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

  public FilteringReporter(Reporter delegate, long filterSpansUnderMicros, long deferSpansUnderMicros) {
    this.delegate = delegate;
    this.filterSpansUnderMicros = filterSpansUnderMicros;
    this.deferSpansUnderMicros = deferSpansUnderMicros;
  }

  @Override
  public void report(JaegerSpan span) {
    final JaegerSpanContext context = span.context();
    final List<JaegerSpan> pendingChildren = pending.remove(context.getSpanId());

    if (span.getDuration() < filterSpansUnderMicros) {
      return;
    }

    if (span.getDuration() < deferSpansUnderMicros) {
      defer(span, context, pendingChildren);
    } else {
      // report pending if any, then this span
      if (pendingChildren != null) {
        pendingChildren.stream().forEachOrdered(pendingSpan -> delegate.report(pendingSpan));
      }
      delegate.report(span);
    }
  }

  private void defer(JaegerSpan span, JaegerSpanContext context, List<JaegerSpan> pendingChildren) {
    final long parentId = context.getParentId();
    if (parentId != 0) {
      pending.compute(parentId, (pId, spans) -> {
        if (spans == null) {
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
    }
  }

  @Override
  public void close() {
    pending.clear();
    delegate.close();
  }
}
