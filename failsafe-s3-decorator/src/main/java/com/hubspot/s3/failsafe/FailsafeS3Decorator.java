package com.hubspot.s3.failsafe;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.hubspot.s3.S3Decorator;

import net.jodah.failsafe.CircuitBreaker;
import net.jodah.failsafe.Failsafe;

public class FailsafeS3Decorator extends S3Decorator {
  private final FailsafeS3 primary;
  private final FailsafeS3 fallback;
  private final FallbackMode fallbackMode;

  private FailsafeS3Decorator(FailsafeS3 primary) {
    this(primary, null, FallbackMode.NONE);
  }

  private FailsafeS3Decorator(FailsafeS3 primary, FailsafeS3 fallback, FallbackMode fallbackMode) {
    this.primary = primary;
    this.fallback = fallback;
    this.fallbackMode = fallbackMode;
  }

  @Override
  protected <T> T read(Function<AmazonS3, T> function) {
    switch (fallbackMode) {
      case READ:
      case READ_WRITE:
        return call(function, primary, fallback);
      default:
        return call(function, primary, null);
    }
  }

  @Override
  protected <T> T write(Function<AmazonS3, T> function) {
    switch (fallbackMode) {
      case WRITE:
      case READ_WRITE:
        return call(function, primary, fallback);
      default:
        return call(function, primary, null);
    }
  }

  private <T> T call(Function<AmazonS3, T> function, FailsafeS3 primary, FailsafeS3 fallback) {
    try {
      return Failsafe.with(primary.getCircuitBreaker()).get(() -> function.apply(primary.getS3()));
    } catch (RuntimeException e) {
      // don't count 404 as failure
      if (e instanceof AmazonServiceException && ((AmazonServiceException) e).getStatusCode() == 404) {
        throw e;
      } else if (fallback != null) {
        return Failsafe.with(fallback.getCircuitBreaker()).get(() -> function.apply(fallback.getS3()));
      } else {
        throw e;
      }
    }
  }

  public static FailsafeS3Decorator decorate(AmazonS3 s3) {
    return decorate(s3, defaultCircuitBreaker());
  }

  public static FailsafeS3Decorator decorate(AmazonS3 s3, CircuitBreaker circuitBreaker) {
    return new FailsafeS3Decorator(new FailsafeS3(s3, circuitBreaker));
  }

  public AmazonS3 withReadFallback(AmazonS3 fallback) {
    return withReadFallback(fallback, defaultCircuitBreaker());
  }

  public AmazonS3 withReadFallback(AmazonS3 fallback, CircuitBreaker setter) {
    return withFallback(new FailsafeS3(fallback, setter), FallbackMode.READ);
  }

  public AmazonS3 withWriteFallback(AmazonS3 fallback) {
    return withWriteFallback(fallback, defaultCircuitBreaker());
  }

  public AmazonS3 withWriteFallback(AmazonS3 fallback, CircuitBreaker setter) {
    return withFallback(new FailsafeS3(fallback, setter), FallbackMode.WRITE);
  }

  public AmazonS3 withReadWriteFallback(AmazonS3 fallback) {
    return withReadWriteFallback(fallback, defaultCircuitBreaker());
  }

  public AmazonS3 withReadWriteFallback(AmazonS3 fallback, CircuitBreaker setter) {
    return withFallback(new FailsafeS3(fallback, setter), FallbackMode.READ_WRITE);
  }

  private AmazonS3 withFallback(FailsafeS3 fallback, FallbackMode fallbackMode) {
    return new FailsafeS3Decorator(primary, fallback, fallbackMode);
  }

  private static CircuitBreaker defaultCircuitBreaker() {
    return new CircuitBreaker().withFailureThreshold(6, 10).withDelay(5, TimeUnit.SECONDS).failOn(t -> {
      if (t instanceof AmazonServiceException) {
        AmazonServiceException serviceException = (AmazonServiceException) t;
        return serviceException.getStatusCode() != 404;
      } else {
        return true;
      }
    });
  }

  private enum FallbackMode {
    NONE, READ, WRITE, READ_WRITE
  }

  private static class FailsafeS3 {
    private final AmazonS3 s3;
    private final CircuitBreaker circuitBreaker;

    private FailsafeS3(AmazonS3 s3, CircuitBreaker circuitBreaker) {
      this.s3 = s3;
      this.circuitBreaker = circuitBreaker;
    }

    public AmazonS3 getS3() {
      return s3;
    }

    public CircuitBreaker getCircuitBreaker() {
      return circuitBreaker;
    }
  }
}
