package com.hubspot.s3.failsafe;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.hubspot.s3.S3Decorator;

import net.jodah.failsafe.CircuitBreaker;
import net.jodah.failsafe.Failsafe;

public abstract class FailsafeS3Decorator extends S3Decorator {
  protected abstract CircuitBreaker getCircuitBreaker();

  @Override
  protected <T> Callable<T> decorate(Callable<T> callable) {
    return () -> Failsafe.with(getCircuitBreaker()).get(callable);
  }

  public static FailsafeS3Decorator decorate(AmazonS3 amazonS3) {
    return decorate(amazonS3, defaultCircuitBreaker());
  }

  public static FailsafeS3Decorator decorate(AmazonS3 amazonS3, CircuitBreaker circuitBreaker) {
    return new FailsafeS3Decorator() {

      @Override
      protected AmazonS3 getDelegate() {
        return amazonS3;
      }

      @Override
      protected CircuitBreaker getCircuitBreaker() {
        return circuitBreaker;
      }
    };
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
}
