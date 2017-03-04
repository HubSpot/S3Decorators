package com.hubspot.s3.hystrix;

import java.util.function.Function;

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
  private final HystrixS3 primary;
  private final HystrixS3 fallback;

  private HystrixS3Decorator(HystrixS3 primary, HystrixS3 fallback) {
    this.primary = primary;
    this.fallback = fallback;
  }

  @Override
  protected <T> T call(Function<AmazonS3, T> function) {
    try {
      return new S3Command<>(function, primary, fallback).execute();
    } catch (HystrixRuntimeException | HystrixBadRequestException e) {
      if (e.getCause() instanceof SdkBaseException) {
        throw (SdkBaseException) e.getCause();
      } else {
        throw e;
      }
    }
  }

  public static HystrixS3Decorator decorate(AmazonS3 s3) {
    return decorate(s3, defaultSetter("s3-primary"));
  }

  public static HystrixS3Decorator decorate(AmazonS3 s3, Setter setter) {
    return new HystrixS3Decorator(new HystrixS3(s3, setter), null);
  }

  public AmazonS3 withFallback(AmazonS3 fallback) {
    return withFallback(fallback, defaultSetter("s3-fallback"));
  }

  public AmazonS3 withFallback(AmazonS3 fallback, Setter setter) {
    return new HystrixS3Decorator(primary, new HystrixS3(fallback, setter));
  }

  private static Setter defaultSetter(String commandKey) {
    return Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey("s3"))
        .andCommandKey(HystrixCommandKey.Factory.asKey(commandKey))
        .andCommandPropertiesDefaults(
            HystrixCommandProperties.defaultSetter()
                .withCircuitBreakerRequestVolumeThreshold(5)
                .withExecutionTimeoutInMilliseconds(5000))
        .andThreadPoolPropertiesDefaults(
            HystrixThreadPoolProperties.defaultSetter()
                .withMaxQueueSize(10)
                .withQueueSizeRejectionThreshold(10));
  }

  private static class HystrixS3 {
    private final AmazonS3 s3;
    private final Setter setter;

    private HystrixS3(AmazonS3 s3, Setter setter) {
      this.s3 = s3;
      this.setter = setter;
    }

    public AmazonS3 getS3() {
      return s3;
    }

    public Setter getSetter() {
      return setter;
    }
  }

  private static class S3Command<T> extends HystrixCommand<T> {
    private final Function<AmazonS3, T> function;
    private final HystrixS3 primary;
    private final HystrixS3 fallback;

    private S3Command(Function<AmazonS3, T> function, HystrixS3 primary, HystrixS3 fallback) {
      super(primary.getSetter());
      this.function = function;
      this.primary = primary;
      this.fallback = fallback;
    }

    @Override
    protected T run() throws Exception {
      try {
        return function.apply(primary.getS3());
      } catch (AmazonServiceException e) {
        // don't count 404 as failure
        if (e.getStatusCode() == 404) {
          throw new HystrixBadRequestException(e.getMessage(), e);
        } else {
          throw e;
        }
      }
    }

    @Override
    protected T getFallback() {
      if (fallback != null) {
        return new S3Command<>(function, fallback, null).execute();
      } else {
        return super.getFallback();
      }
    }
  }
}
