package com.hubspot.s3.hystrix;

import java.util.function.Supplier;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkBaseException;
import com.amazonaws.services.s3.AmazonS3;
import com.hubspot.s3.S3Decorator;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommand.Setter;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixCommandProperties;
import com.netflix.hystrix.HystrixThreadPoolProperties;
import com.netflix.hystrix.exception.HystrixBadRequestException;
import com.netflix.hystrix.exception.HystrixRuntimeException;

public class HystrixS3Decorator extends S3Decorator {
  private final AmazonS3 delegate;
  private final Setter setter;

  private HystrixS3Decorator(AmazonS3 delegate, Setter setter) {
    this.delegate = checkNotNull(delegate, "delegate");
    this.setter = checkNotNull(setter, "setter");
  }

  @Override
  protected AmazonS3 getDelegate() {
    return delegate;
  }

  @Override
  protected <T> T call(Supplier<T> callable) {
    try {
      return new S3Command<>(setter, callable).execute();
    } catch (HystrixRuntimeException | HystrixBadRequestException e) {
      if (e.getCause() instanceof SdkBaseException) {
        throw (SdkBaseException) e.getCause();
      } else {
        throw e;
      }
    }
  }

  public static HystrixS3Decorator decorate(AmazonS3 s3) {
    return decorate(s3, defaultSetter(s3));
  }

  public static HystrixS3Decorator decorate(AmazonS3 s3, Setter setter) {
    return new HystrixS3Decorator(s3, setter);
  }

  private static Setter defaultSetter(AmazonS3 s3) {
    return Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey("s3"))
        .andCommandKey(HystrixCommandKey.Factory.asKey(s3.getRegionName()))
        .andCommandPropertiesDefaults(
            HystrixCommandProperties.defaultSetter()
                .withCircuitBreakerRequestVolumeThreshold(5)
                .withExecutionTimeoutEnabled(false))
        .andThreadPoolPropertiesDefaults(
            HystrixThreadPoolProperties.defaultSetter()
                .withMaxQueueSize(5)
                .withQueueSizeRejectionThreshold(5));
  }

  private static <T> T checkNotNull(T value, String parameterName) {
    if (value == null) {
      throw new NullPointerException(parameterName + " must not be null");
    } else {
      return value;
    }
  }

  private static class S3Command<T> extends HystrixCommand<T> {
    private final Supplier<T> callable;

    private S3Command(Setter setter, Supplier<T> callable) {
      super(setter);
      this.callable = callable;
    }

    @Override
    protected T run() throws Exception {
      try {
        return callable.get();
      } catch (AmazonServiceException e) {
        // don't count 404 as failure
        if (is403or404(e)) {
          throw new HystrixBadRequestException(e.getMessage(), e);
        } else {
          throw e;
        }
      }
    }
  }
}
