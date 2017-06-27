package com.hubspot.s3.failsafe;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.amazonaws.services.s3.AmazonS3;
import com.hubspot.s3.S3Decorator;

import net.jodah.failsafe.CircuitBreaker;
import net.jodah.failsafe.Failsafe;

public class FailsafeS3Decorator extends S3Decorator {
  private final AmazonS3 delegate;
  private final CircuitBreaker circuitBreaker;

  private FailsafeS3Decorator(AmazonS3 delegate, CircuitBreaker circuitBreaker) {
    this.delegate = checkNotNull(delegate, "delegate");
    this.circuitBreaker = checkNotNull(circuitBreaker, "circuitBreaker");
  }

  @Override
  protected AmazonS3 getDelegate() {
    return delegate;
  }

  @Override
  protected <T> T call(Supplier<T> callable) {
    return Failsafe.with(circuitBreaker).get(callable::get);
  }

  public static FailsafeS3Decorator decorate(AmazonS3 s3) {
    return decorate(s3, defaultCircuitBreaker());
  }

  public static FailsafeS3Decorator decorate(AmazonS3 s3, CircuitBreaker circuitBreaker) {
    return new FailsafeS3Decorator(s3, circuitBreaker);
  }

  private static CircuitBreaker defaultCircuitBreaker() {
    return new CircuitBreaker().withFailureThreshold(6, 10).withDelay(5, TimeUnit.SECONDS).failOn(t -> !is403or404(t));
  }

  private static <T> T checkNotNull(T value, String parameterName) {
    if (value == null) {
      throw new NullPointerException(parameterName + " must not be null");
    } else {
      return value;
    }
  }
}
