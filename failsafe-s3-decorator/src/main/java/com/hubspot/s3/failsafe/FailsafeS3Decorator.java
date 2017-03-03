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

  private FailsafeS3Decorator(FailsafeS3 primary, FailsafeS3 fallback) {
    this.primary = primary;
    this.fallback = fallback;
  }

  @Override
  protected <T> T call(Function<AmazonS3, T> function) {
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
    return new FailsafeS3Decorator(new FailsafeS3(s3, circuitBreaker), null);
  }

  public FailsafeS3Decorator withFallback(AmazonS3 fallback) {
    return withFallback(fallback, defaultCircuitBreaker());
  }

  public FailsafeS3Decorator withFallback(AmazonS3 fallback, CircuitBreaker circuitBreaker) {
    return new FailsafeS3Decorator(primary, new FailsafeS3(fallback, circuitBreaker));
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
