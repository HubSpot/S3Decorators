package com.hubspot.s3.hystrix;

import java.util.concurrent.Callable;

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

public abstract class HystrixS3Decorator extends S3Decorator {
  protected abstract Setter getSetter();

  @Override
  protected <T> Callable<T> decorate(Callable<T> callable) {
    return () -> {
      try {
        return new S3Command<>(getSetter(), callable).execute();
      } catch (HystrixRuntimeException | HystrixBadRequestException e) {
        if (e.getCause() instanceof SdkBaseException) {
          throw (SdkBaseException) e.getCause();
        } else {
          throw e;
        }
      }
    };
  }

  public static HystrixS3Decorator decorate(AmazonS3 amazonS3) {
    return decorate(amazonS3, defaultSetter());
  }

  public static HystrixS3Decorator decorate(AmazonS3 amazonS3, Setter setter) {
    return new HystrixS3Decorator() {

      @Override
      protected AmazonS3 getDelegate() {
        return amazonS3;
      }

      @Override
      protected Setter getSetter() {
        return setter;
      }
    };
  }

  private static Setter defaultSetter() {
    return Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey("s3"))
        .andCommandKey(HystrixCommandKey.Factory.asKey("s3"))
        .andCommandPropertiesDefaults(
            HystrixCommandProperties.defaultSetter()
                .withCircuitBreakerRequestVolumeThreshold(5)
                .withExecutionTimeoutInMilliseconds(5000))
        .andThreadPoolPropertiesDefaults(
            HystrixThreadPoolProperties.defaultSetter()
                .withMaxQueueSize(10).withQueueSizeRejectionThreshold(10));
  }

  private static class S3Command<T> extends HystrixCommand<T> {
    private final Callable<T> callable;

    private S3Command(Setter setter, Callable<T> callable) {
      super(setter);
      this.callable = callable;
    }

    @Override
    protected T run() throws Exception {
      try {
        return callable.call();
      } catch (AmazonServiceException e) {
        // don't count 404 as failure
        if (e.getStatusCode() == 404) {
          throw new HystrixBadRequestException(e.getMessage(), e);
        } else {
          throw e;
        }
      }
    }
  }
}
