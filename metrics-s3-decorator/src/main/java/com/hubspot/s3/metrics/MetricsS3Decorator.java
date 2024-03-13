package com.hubspot.s3.metrics;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.hubspot.s3.S3Decorator;
import java.util.function.Supplier;

public class MetricsS3Decorator extends S3Decorator {

  private final AmazonS3 delegate;
  private final Timer requestTimer;
  private final Meter exceptionMeter;

  private MetricsS3Decorator(AmazonS3 delegate, MetricRegistry metricRegistry) {
    this.delegate = delegate;
    this.requestTimer = metricRegistry.timer(metricName("requests"));
    this.exceptionMeter = metricRegistry.meter(metricName("exceptions"));
  }

  public static AmazonS3 decorate(AmazonS3 s3, MetricRegistry metricRegistry) {
    return new MetricsS3Decorator(s3, metricRegistry);
  }

  @Override
  protected AmazonS3 getDelegate() {
    return delegate;
  }

  @Override
  protected <T> T call(Supplier<T> callable) {
    try (Context context = requestTimer.time()) {
      return callable.get();
    } catch (AmazonServiceException e) {
      if (!is403or404(e)) {
        exceptionMeter.mark();
      }

      throw e;
    }
  }

  private static String metricName(String name) {
    return MetricRegistry.name(MetricsS3Decorator.class, name);
  }
}
