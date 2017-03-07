package com.hubspot.s3.metrics;

import java.util.function.Function;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.hubspot.s3.S3Decorator;

public class MetricsS3Decorator extends S3Decorator {
  private final AmazonS3 delegate;
  private final Timer readTimer;
  private final Meter readExceptions;
  private final Timer writeTimer;
  private final Meter writeExceptions;

  private MetricsS3Decorator(AmazonS3 delegate, MetricRegistry metricRegistry) {
    this.delegate = delegate;
    this.readTimer = metricRegistry.timer(metricName("reads"));
    this.readExceptions = metricRegistry.meter(metricName("readExceptions"));
    this.writeTimer = metricRegistry.timer(metricName("writes"));
    this.writeExceptions = metricRegistry.meter(metricName("writeExceptions"));
  }

  public static AmazonS3 decorate(AmazonS3 s3, MetricRegistry metricRegistry) {
    return new MetricsS3Decorator(s3, metricRegistry);
  }

  @Override
  protected <T> T read(Function<AmazonS3, T> function) {
    try (Context context = readTimer.time()) {
      return function.apply(delegate);
    } catch (AmazonServiceException e) {
      if (!is404(e)) {
        readExceptions.mark();
      }

      throw e;
    }
  }

  @Override
  protected <T> T write(Function<AmazonS3, T> function) {
    try (Context context = writeTimer.time()) {
      return function.apply(delegate);
    } catch (AmazonServiceException e) {
      if (!is404(e)) {
        writeExceptions.mark();
      }

      throw e;
    }
  }

  private static String metricName(String name) {
    return MetricRegistry.name(MetricsS3Decorator.class, name);
  }
}
