/*
 * Copyright (c) 2021, The Jaeger Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.jaegertracing.internal.reporters;

import io.jaegertracing.internal.JaegerSpan;
import io.jaegertracing.internal.JaegerSpanContext;
import io.jaegertracing.internal.metrics.Metrics;
import io.jaegertracing.spi.Reporter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
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
  private final Metrics metrics;

  private final Map<Long, List<JaegerSpan>> pendingByParent = new ConcurrentHashMap<>();
  private final LongAdder pendingCount = new LongAdder();

  public FilteringReporter(
      Reporter delegate, long filterSpansUnderMicros, long deferSpansUnderMicros, Metrics metrics) {
    this.delegate = delegate;
    this.filterSpansUnderMicros = filterSpansUnderMicros;
    this.deferSpansUnderMicros = deferSpansUnderMicros;
    this.metrics = metrics;
  }

  @Override
  public void report(JaegerSpan span) {
    final JaegerSpanContext context = span.context();
    final List<JaegerSpan> pendingChildren = pendingByParent.remove(context.getSpanId());

    if (span.getDuration() < filterSpansUnderMicros) {
      metrics.filteredSpans.inc(1);
      return;
    }

    if (span.getDuration() < deferSpansUnderMicros) {
      defer(span, context, pendingChildren);
    } else {
      // report pending if any, then this span
      if (pendingChildren != null) {
        pendingChildren.forEach(delegate::report);
        final int count = pendingChildren.size();
        pendingCount.add(-1 * count);
        metrics.deferredSpansPending.update(pendingCount.longValue());
        metrics.deferredSpansSent.inc(count);
      }
      delegate.report(span);
    }
  }

  private void defer(final JaegerSpan span, final JaegerSpanContext context,
      final List<JaegerSpan> pendingChildren) {
    final long parentId = context.getParentId();
    if (parentId != 0) {
      pendingCount.increment();
      metrics.deferredSpansPending.update(pendingCount.longValue());
      metrics.deferredSpansStarted.inc(1);
      pendingByParent.compute(parentId, (id, spans) -> {
        if (spans == null) {
          if (pendingChildren == null) {
            spans = new ArrayList<>(1);
          } else {
            spans = pendingChildren;
          }
        } else if (pendingChildren != null) {
          spans.addAll(pendingChildren);
        }
        spans.add(span);
        return spans;
      });
    } else if (pendingChildren != null) {
      final int count = pendingChildren.size();
      pendingCount.add(-1 * count);
      metrics.deferredSpansPending.update(pendingCount.longValue());
      metrics.deferredSpansDropped.inc(count);
    }
  }

  @Override
  public void close() {
    pendingByParent.values().forEach(pendingSpans -> metrics.deferredSpansDropped.inc(pendingSpans.size()));
    pendingByParent.clear();
    delegate.close();
    pendingCount.reset();
    metrics.deferredSpansPending.update(0);
  }
}
