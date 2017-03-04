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
  private final FallbackMode fallbackMode;

  private HystrixS3Decorator(HystrixS3 primary) {
    this(primary, null, FallbackMode.NONE);
  }

  private HystrixS3Decorator(HystrixS3 primary, HystrixS3 fallback, FallbackMode fallbackMode) {
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

  private <T> T call(Function<AmazonS3, T> function, HystrixS3 primary, HystrixS3 fallback) {
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
    return new HystrixS3Decorator(new HystrixS3(s3, setter));
  }

  public AmazonS3 withReadFallback(AmazonS3 fallback) {
    return withReadFallback(fallback, defaultSetter("s3-fallback"));
  }

  public AmazonS3 withReadFallback(AmazonS3 fallback, Setter setter) {
    return withFallback(new HystrixS3(fallback, setter), FallbackMode.READ);
  }

  public AmazonS3 withWriteFallback(AmazonS3 fallback) {
    return withWriteFallback(fallback, defaultSetter("s3-fallback"));
  }

  public AmazonS3 withWriteFallback(AmazonS3 fallback, Setter setter) {
    return withFallback(new HystrixS3(fallback, setter), FallbackMode.WRITE);
  }

  public AmazonS3 withReadWriteFallback(AmazonS3 fallback) {
    return withReadWriteFallback(fallback, defaultSetter("s3-fallback"));
  }

  public AmazonS3 withReadWriteFallback(AmazonS3 fallback, Setter setter) {
    return withFallback(new HystrixS3(fallback, setter), FallbackMode.READ_WRITE);
  }

  private AmazonS3 withFallback(HystrixS3 fallback, FallbackMode fallbackMode) {
    return new HystrixS3Decorator(primary, fallback, fallbackMode);
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

  private enum FallbackMode {
    NONE, READ, WRITE, READ_WRITE
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
