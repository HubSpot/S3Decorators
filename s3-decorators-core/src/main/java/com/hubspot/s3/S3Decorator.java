package com.hubspot.s3;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.HttpMethod;
import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Region;
import com.amazonaws.services.s3.AbstractAmazonS3;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.S3ResponseMetadata;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.BucketAccelerateConfiguration;
import com.amazonaws.services.s3.model.BucketCrossOriginConfiguration;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.BucketLoggingConfiguration;
import com.amazonaws.services.s3.model.BucketNotificationConfiguration;
import com.amazonaws.services.s3.model.BucketPolicy;
import com.amazonaws.services.s3.model.BucketReplicationConfiguration;
import com.amazonaws.services.s3.model.BucketTaggingConfiguration;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.BucketWebsiteConfiguration;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.CopyPartRequest;
import com.amazonaws.services.s3.model.CopyPartResult;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.DeleteBucketAnalyticsConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteBucketAnalyticsConfigurationResult;
import com.amazonaws.services.s3.model.DeleteBucketCrossOriginConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteBucketEncryptionRequest;
import com.amazonaws.services.s3.model.DeleteBucketEncryptionResult;
import com.amazonaws.services.s3.model.DeleteBucketInventoryConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteBucketInventoryConfigurationResult;
import com.amazonaws.services.s3.model.DeleteBucketLifecycleConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteBucketMetricsConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteBucketMetricsConfigurationResult;
import com.amazonaws.services.s3.model.DeleteBucketPolicyRequest;
import com.amazonaws.services.s3.model.DeleteBucketReplicationConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteBucketRequest;
import com.amazonaws.services.s3.model.DeleteBucketTaggingConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteBucketWebsiteConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectTaggingRequest;
import com.amazonaws.services.s3.model.DeleteObjectTaggingResult;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.DeletePublicAccessBlockRequest;
import com.amazonaws.services.s3.model.DeletePublicAccessBlockResult;
import com.amazonaws.services.s3.model.DeleteVersionRequest;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.GetBucketAccelerateConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketAclRequest;
import com.amazonaws.services.s3.model.GetBucketAnalyticsConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketAnalyticsConfigurationResult;
import com.amazonaws.services.s3.model.GetBucketCrossOriginConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketEncryptionRequest;
import com.amazonaws.services.s3.model.GetBucketEncryptionResult;
import com.amazonaws.services.s3.model.GetBucketInventoryConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketInventoryConfigurationResult;
import com.amazonaws.services.s3.model.GetBucketLifecycleConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketLocationRequest;
import com.amazonaws.services.s3.model.GetBucketLoggingConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketMetricsConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketMetricsConfigurationResult;
import com.amazonaws.services.s3.model.GetBucketNotificationConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketOwnershipControlsRequest;
import com.amazonaws.services.s3.model.GetBucketOwnershipControlsResult;
import com.amazonaws.services.s3.model.GetBucketPolicyRequest;
import com.amazonaws.services.s3.model.GetBucketPolicyStatusRequest;
import com.amazonaws.services.s3.model.GetBucketPolicyStatusResult;
import com.amazonaws.services.s3.model.GetBucketReplicationConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketTaggingConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketVersioningConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketWebsiteConfigurationRequest;
import com.amazonaws.services.s3.model.GetObjectAclRequest;
import com.amazonaws.services.s3.model.GetObjectLegalHoldRequest;
import com.amazonaws.services.s3.model.GetObjectLegalHoldResult;
import com.amazonaws.services.s3.model.GetObjectLockConfigurationRequest;
import com.amazonaws.services.s3.model.GetObjectLockConfigurationResult;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRetentionRequest;
import com.amazonaws.services.s3.model.GetObjectRetentionResult;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
import com.amazonaws.services.s3.model.GetPublicAccessBlockRequest;
import com.amazonaws.services.s3.model.GetPublicAccessBlockResult;
import com.amazonaws.services.s3.model.GetS3AccountOwnerRequest;
import com.amazonaws.services.s3.model.HeadBucketRequest;
import com.amazonaws.services.s3.model.HeadBucketResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListBucketAnalyticsConfigurationsRequest;
import com.amazonaws.services.s3.model.ListBucketAnalyticsConfigurationsResult;
import com.amazonaws.services.s3.model.ListBucketInventoryConfigurationsRequest;
import com.amazonaws.services.s3.model.ListBucketInventoryConfigurationsResult;
import com.amazonaws.services.s3.model.ListBucketMetricsConfigurationsRequest;
import com.amazonaws.services.s3.model.ListBucketMetricsConfigurationsResult;
import com.amazonaws.services.s3.model.ListBucketsRequest;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListNextBatchOfObjectsRequest;
import com.amazonaws.services.s3.model.ListNextBatchOfVersionsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ListPartsRequest;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.PresignedUrlDownloadRequest;
import com.amazonaws.services.s3.model.PresignedUrlDownloadResult;
import com.amazonaws.services.s3.model.PresignedUrlUploadRequest;
import com.amazonaws.services.s3.model.PresignedUrlUploadResult;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.RestoreObjectRequest;
import com.amazonaws.services.s3.model.RestoreObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.SelectObjectContentRequest;
import com.amazonaws.services.s3.model.SelectObjectContentResult;
import com.amazonaws.services.s3.model.SetBucketAccelerateConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketAclRequest;
import com.amazonaws.services.s3.model.SetBucketAnalyticsConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketAnalyticsConfigurationResult;
import com.amazonaws.services.s3.model.SetBucketCrossOriginConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketEncryptionRequest;
import com.amazonaws.services.s3.model.SetBucketEncryptionResult;
import com.amazonaws.services.s3.model.SetBucketInventoryConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketInventoryConfigurationResult;
import com.amazonaws.services.s3.model.SetBucketLifecycleConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketLoggingConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketMetricsConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketMetricsConfigurationResult;
import com.amazonaws.services.s3.model.SetBucketNotificationConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketOwnershipControlsRequest;
import com.amazonaws.services.s3.model.SetBucketOwnershipControlsResult;
import com.amazonaws.services.s3.model.SetBucketPolicyRequest;
import com.amazonaws.services.s3.model.SetBucketReplicationConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketTaggingConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketVersioningConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketWebsiteConfigurationRequest;
import com.amazonaws.services.s3.model.SetObjectAclRequest;
import com.amazonaws.services.s3.model.SetObjectLegalHoldRequest;
import com.amazonaws.services.s3.model.SetObjectLegalHoldResult;
import com.amazonaws.services.s3.model.SetObjectLockConfigurationRequest;
import com.amazonaws.services.s3.model.SetObjectLockConfigurationResult;
import com.amazonaws.services.s3.model.SetObjectRetentionRequest;
import com.amazonaws.services.s3.model.SetObjectRetentionResult;
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;
import com.amazonaws.services.s3.model.SetObjectTaggingResult;
import com.amazonaws.services.s3.model.SetPublicAccessBlockRequest;
import com.amazonaws.services.s3.model.SetPublicAccessBlockResult;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.amazonaws.services.s3.model.VersionListing;
import com.amazonaws.services.s3.model.analytics.AnalyticsConfiguration;
import com.amazonaws.services.s3.model.inventory.InventoryConfiguration;
import com.amazonaws.services.s3.model.metrics.MetricsConfiguration;
import com.amazonaws.services.s3.waiters.AmazonS3Waiters;
import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.Date;
import java.util.List;
import java.util.function.Supplier;

public abstract class S3Decorator extends AbstractAmazonS3 {

  protected abstract AmazonS3 getDelegate();

  protected abstract <T> T call(Supplier<T> callable);

  protected void run(Runnable runnable) {
    call(() -> {
      runnable.run();
      return null;
    });
  }

  protected static boolean is403or404(Throwable t) {
    boolean isServiceException = t instanceof AmazonServiceException;

    if (!isServiceException) {
      return false;
    }

    int statusCode = ((AmazonServiceException) t).getStatusCode();
    return statusCode == 403 || statusCode == 404;
  }

  @Override
  public void setEndpoint(String endpoint) {
    getDelegate().setEndpoint(endpoint);
  }

  @Override
  public void setRegion(Region region) throws IllegalArgumentException {
    getDelegate().setRegion(region);
  }

  @Override
  public void setS3ClientOptions(S3ClientOptions clientOptions) {
    getDelegate().setS3ClientOptions(clientOptions);
  }

  @Override
  public void changeObjectStorageClass(
    String bucketName,
    String key,
    StorageClass newStorageClass
  ) throws SdkClientException, AmazonServiceException {
    run(() -> getDelegate().changeObjectStorageClass(bucketName, key, newStorageClass));
  }

  @Override
  public void setObjectRedirectLocation(
    String bucketName,
    String key,
    String newRedirectLocation
  ) throws SdkClientException, AmazonServiceException {
    run(() ->
      getDelegate().setObjectRedirectLocation(bucketName, key, newRedirectLocation)
    );
  }

  @Override
  public ObjectListing listObjects(String bucketName)
    throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().listObjects(bucketName));
  }

  @Override
  public ObjectListing listObjects(String bucketName, String prefix)
    throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().listObjects(bucketName, prefix));
  }

  @Override
  public ObjectListing listObjects(ListObjectsRequest listObjectsRequest)
    throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().listObjects(listObjectsRequest));
  }

  @Override
  public ListObjectsV2Result listObjectsV2(String bucketName)
    throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().listObjectsV2(bucketName));
  }

  @Override
  public ListObjectsV2Result listObjectsV2(String bucketName, String prefix)
    throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().listObjectsV2(bucketName, prefix));
  }

  @Override
  public ListObjectsV2Result listObjectsV2(ListObjectsV2Request listObjectsV2Request)
    throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().listObjectsV2(listObjectsV2Request));
  }

  @Override
  public ObjectListing listNextBatchOfObjects(ObjectListing previousObjectListing)
    throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().listNextBatchOfObjects(previousObjectListing));
  }

  @Override
  public ObjectListing listNextBatchOfObjects(
    ListNextBatchOfObjectsRequest listNextBatchOfObjectsRequest
  ) throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().listNextBatchOfObjects(listNextBatchOfObjectsRequest)
    );
  }

  @Override
  public VersionListing listVersions(String bucketName, String prefix)
    throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().listVersions(bucketName, prefix));
  }

  @Override
  public VersionListing listVersions(
    String bucketName,
    String prefix,
    String keyMarker,
    String versionIdMarker,
    String delimiter,
    Integer maxResults
  ) throws SdkClientException, AmazonServiceException {
    return call(() ->
      getDelegate()
        .listVersions(
          bucketName,
          prefix,
          keyMarker,
          versionIdMarker,
          delimiter,
          maxResults
        )
    );
  }

  @Override
  public VersionListing listVersions(ListVersionsRequest listVersionsRequest)
    throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().listVersions(listVersionsRequest));
  }

  @Override
  public VersionListing listNextBatchOfVersions(VersionListing previousVersionListing)
    throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().listNextBatchOfVersions(previousVersionListing));
  }

  @Override
  public VersionListing listNextBatchOfVersions(
    ListNextBatchOfVersionsRequest listNextBatchOfVersionsRequest
  ) throws SdkClientException, AmazonServiceException {
    return call(() ->
      getDelegate().listNextBatchOfVersions(listNextBatchOfVersionsRequest)
    );
  }

  @Override
  public Owner getS3AccountOwner() throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().getS3AccountOwner());
  }

  @Override
  public Owner getS3AccountOwner(GetS3AccountOwnerRequest getS3AccountOwnerRequest)
    throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().getS3AccountOwner(getS3AccountOwnerRequest));
  }

  @Override
  public boolean doesBucketExist(String bucketName)
    throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().doesBucketExist(bucketName));
  }

  @Override
  public HeadBucketResult headBucket(HeadBucketRequest headBucketRequest)
    throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().headBucket(headBucketRequest));
  }

  @Override
  public List<Bucket> listBuckets() throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().listBuckets());
  }

  @Override
  public List<Bucket> listBuckets(ListBucketsRequest listBucketsRequest)
    throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().listBuckets(listBucketsRequest));
  }

  @Override
  public String getBucketLocation(String bucketName)
    throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().getBucketLocation(bucketName));
  }

  @Override
  public String getBucketLocation(GetBucketLocationRequest getBucketLocationRequest)
    throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().getBucketLocation(getBucketLocationRequest));
  }

  @Override
  public Bucket createBucket(CreateBucketRequest createBucketRequest)
    throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().createBucket(createBucketRequest));
  }

  @Override
  public Bucket createBucket(String bucketName)
    throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().createBucket(bucketName));
  }

  @Override
  public Bucket createBucket(
    String bucketName,
    com.amazonaws.services.s3.model.Region region
  ) throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().createBucket(bucketName, region));
  }

  @Override
  public Bucket createBucket(String bucketName, String region)
    throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().createBucket(bucketName, region));
  }

  @Override
  public AccessControlList getObjectAcl(String bucketName, String key)
    throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().getObjectAcl(bucketName, key));
  }

  @Override
  public AccessControlList getObjectAcl(String bucketName, String key, String versionId)
    throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().getObjectAcl(bucketName, key, versionId));
  }

  @Override
  public AccessControlList getObjectAcl(GetObjectAclRequest getObjectAclRequest)
    throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().getObjectAcl(getObjectAclRequest));
  }

  @Override
  public void setObjectAcl(String bucketName, String key, AccessControlList acl)
    throws SdkClientException, AmazonServiceException {
    run(() -> getDelegate().setObjectAcl(bucketName, key, acl));
  }

  @Override
  public void setObjectAcl(String bucketName, String key, CannedAccessControlList acl)
    throws SdkClientException, AmazonServiceException {
    run(() -> getDelegate().setObjectAcl(bucketName, key, acl));
  }

  @Override
  public void setObjectAcl(
    String bucketName,
    String key,
    String versionId,
    AccessControlList acl
  ) throws SdkClientException, AmazonServiceException {
    run(() -> getDelegate().setObjectAcl(bucketName, key, versionId, acl));
  }

  @Override
  public void setObjectAcl(
    String bucketName,
    String key,
    String versionId,
    CannedAccessControlList acl
  ) throws SdkClientException, AmazonServiceException {
    run(() -> getDelegate().setObjectAcl(bucketName, key, versionId, acl));
  }

  @Override
  public void setObjectAcl(SetObjectAclRequest setObjectAclRequest)
    throws SdkClientException, AmazonServiceException {
    run(() -> getDelegate().setObjectAcl(setObjectAclRequest));
  }

  @Override
  public AccessControlList getBucketAcl(String bucketName)
    throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().getBucketAcl(bucketName));
  }

  @Override
  public AccessControlList getBucketAcl(GetBucketAclRequest getBucketAclRequest)
    throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().getBucketAcl(getBucketAclRequest));
  }

  @Override
  public void setBucketAcl(String bucketName, AccessControlList acl)
    throws SdkClientException, AmazonServiceException {
    run(() -> getDelegate().setBucketAcl(bucketName, acl));
  }

  @Override
  public void setBucketAcl(String bucketName, CannedAccessControlList cannedAcl)
    throws SdkClientException, AmazonServiceException {
    run(() -> getDelegate().setBucketAcl(bucketName, cannedAcl));
  }

  @Override
  public void setBucketAcl(SetBucketAclRequest setBucketAclRequest)
    throws SdkClientException, AmazonServiceException {
    run(() -> getDelegate().setBucketAcl(setBucketAclRequest));
  }

  @Override
  public ObjectMetadata getObjectMetadata(String bucketName, String key)
    throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().getObjectMetadata(bucketName, key));
  }

  @Override
  public ObjectMetadata getObjectMetadata(
    GetObjectMetadataRequest getObjectMetadataRequest
  ) throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().getObjectMetadata(getObjectMetadataRequest));
  }

  @Override
  public S3Object getObject(String bucketName, String key)
    throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().getObject(bucketName, key));
  }

  @Override
  public S3Object getObject(GetObjectRequest getObjectRequest) {
    return call(() -> getDelegate().getObject(getObjectRequest));
  }

  @Override
  public ObjectMetadata getObject(
    GetObjectRequest getObjectRequest,
    File destinationFile
  ) {
    return call(() -> getDelegate().getObject(getObjectRequest, destinationFile));
  }

  @Override
  public String getObjectAsString(String bucketName, String key)
    throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().getObjectAsString(bucketName, key));
  }

  @Override
  public GetObjectTaggingResult getObjectTagging(
    GetObjectTaggingRequest objectTaggingRequest
  ) {
    return call(() -> getDelegate().getObjectTagging(objectTaggingRequest));
  }

  @Override
  public SetObjectTaggingResult setObjectTagging(
    SetObjectTaggingRequest setObjectTaggingRequest
  ) {
    return call(() -> getDelegate().setObjectTagging(setObjectTaggingRequest));
  }

  @Override
  public DeleteObjectTaggingResult deleteObjectTagging(
    DeleteObjectTaggingRequest deleteObjectTaggingRequest
  ) {
    return call(() -> getDelegate().deleteObjectTagging(deleteObjectTaggingRequest));
  }

  @Override
  public void deleteBucket(String bucketName)
    throws SdkClientException, AmazonServiceException {
    run(() -> getDelegate().deleteBucket(bucketName));
  }

  @Override
  public void deleteBucket(DeleteBucketRequest deleteBucketRequest)
    throws SdkClientException, AmazonServiceException {
    run(() -> getDelegate().deleteBucket(deleteBucketRequest));
  }

  @Override
  public PutObjectResult putObject(String bucketName, String key, File file)
    throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().putObject(bucketName, key, file));
  }

  @Override
  public PutObjectResult putObject(
    String bucketName,
    String key,
    InputStream input,
    ObjectMetadata metadata
  ) throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().putObject(bucketName, key, input, metadata));
  }

  @Override
  public PutObjectResult putObject(PutObjectRequest putObjectRequest) {
    return call(() -> getDelegate().putObject(putObjectRequest));
  }

  @Override
  public PutObjectResult putObject(String bucketName, String key, String content)
    throws AmazonServiceException, AmazonServiceException {
    return call(() -> getDelegate().putObject(bucketName, key, content));
  }

  @Override
  public CopyObjectResult copyObject(
    String sourceBucketName,
    String sourceKey,
    String destinationBucketName,
    String destinationKey
  ) throws SdkClientException, AmazonServiceException {
    return call(() ->
      getDelegate()
        .copyObject(sourceBucketName, sourceKey, destinationBucketName, destinationKey)
    );
  }

  @Override
  public CopyObjectResult copyObject(CopyObjectRequest copyObjectRequest)
    throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().copyObject(copyObjectRequest));
  }

  @Override
  public CopyPartResult copyPart(CopyPartRequest copyPartRequest) {
    return call(() -> getDelegate().copyPart(copyPartRequest));
  }

  @Override
  public void deleteObject(String bucketName, String key)
    throws SdkClientException, AmazonServiceException {
    run(() -> getDelegate().deleteObject(bucketName, key));
  }

  @Override
  public void deleteObject(DeleteObjectRequest deleteObjectRequest)
    throws SdkClientException, AmazonServiceException {
    run(() -> getDelegate().deleteObject(deleteObjectRequest));
  }

  @Override
  public DeleteObjectsResult deleteObjects(DeleteObjectsRequest deleteObjectsRequest)
    throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().deleteObjects(deleteObjectsRequest));
  }

  @Override
  public void deleteVersion(String bucketName, String key, String versionId)
    throws SdkClientException, AmazonServiceException {
    run(() -> getDelegate().deleteVersion(bucketName, key, versionId));
  }

  @Override
  public void deleteVersion(DeleteVersionRequest deleteVersionRequest)
    throws SdkClientException, AmazonServiceException {
    run(() -> getDelegate().deleteVersion(deleteVersionRequest));
  }

  @Override
  public BucketLoggingConfiguration getBucketLoggingConfiguration(String bucketName)
    throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().getBucketLoggingConfiguration(bucketName));
  }

  @Override
  public BucketLoggingConfiguration getBucketLoggingConfiguration(
    GetBucketLoggingConfigurationRequest getBucketLoggingConfigurationRequest
  ) throws SdkClientException, AmazonServiceException {
    return call(() ->
      getDelegate().getBucketLoggingConfiguration(getBucketLoggingConfigurationRequest)
    );
  }

  @Override
  public void setBucketLoggingConfiguration(
    SetBucketLoggingConfigurationRequest setBucketLoggingConfigurationRequest
  ) throws SdkClientException, AmazonServiceException {
    run(() ->
      getDelegate().setBucketLoggingConfiguration(setBucketLoggingConfigurationRequest)
    );
  }

  @Override
  public BucketVersioningConfiguration getBucketVersioningConfiguration(
    String bucketName
  ) throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().getBucketVersioningConfiguration(bucketName));
  }

  @Override
  public BucketVersioningConfiguration getBucketVersioningConfiguration(
    GetBucketVersioningConfigurationRequest getBucketVersioningConfigurationRequest
  ) throws SdkClientException, AmazonServiceException {
    return call(() ->
      getDelegate()
        .getBucketVersioningConfiguration(getBucketVersioningConfigurationRequest)
    );
  }

  @Override
  public void setBucketVersioningConfiguration(
    SetBucketVersioningConfigurationRequest setBucketVersioningConfigurationRequest
  ) throws SdkClientException, AmazonServiceException {
    run(() ->
      getDelegate()
        .setBucketVersioningConfiguration(setBucketVersioningConfigurationRequest)
    );
  }

  @Override
  public BucketLifecycleConfiguration getBucketLifecycleConfiguration(String bucketName) {
    return call(() -> getDelegate().getBucketLifecycleConfiguration(bucketName));
  }

  @Override
  public BucketLifecycleConfiguration getBucketLifecycleConfiguration(
    GetBucketLifecycleConfigurationRequest getBucketLifecycleConfigurationRequest
  ) {
    return call(() ->
      getDelegate()
        .getBucketLifecycleConfiguration(getBucketLifecycleConfigurationRequest)
    );
  }

  @Override
  public void setBucketLifecycleConfiguration(
    String bucketName,
    BucketLifecycleConfiguration bucketLifecycleConfiguration
  ) {
    run(() ->
      getDelegate()
        .setBucketLifecycleConfiguration(bucketName, bucketLifecycleConfiguration)
    );
  }

  @Override
  public void setBucketLifecycleConfiguration(
    SetBucketLifecycleConfigurationRequest setBucketLifecycleConfigurationRequest
  ) {
    run(() ->
      getDelegate()
        .setBucketLifecycleConfiguration(setBucketLifecycleConfigurationRequest)
    );
  }

  @Override
  public void deleteBucketLifecycleConfiguration(String bucketName) {
    run(() -> getDelegate().deleteBucketLifecycleConfiguration(bucketName));
  }

  @Override
  public void deleteBucketLifecycleConfiguration(
    DeleteBucketLifecycleConfigurationRequest deleteBucketLifecycleConfigurationRequest
  ) {
    run(() ->
      getDelegate()
        .deleteBucketLifecycleConfiguration(deleteBucketLifecycleConfigurationRequest)
    );
  }

  @Override
  public BucketCrossOriginConfiguration getBucketCrossOriginConfiguration(
    String bucketName
  ) {
    return call(() -> getDelegate().getBucketCrossOriginConfiguration(bucketName));
  }

  @Override
  public BucketCrossOriginConfiguration getBucketCrossOriginConfiguration(
    GetBucketCrossOriginConfigurationRequest getBucketCrossOriginConfigurationRequest
  ) {
    return call(() ->
      getDelegate()
        .getBucketCrossOriginConfiguration(getBucketCrossOriginConfigurationRequest)
    );
  }

  @Override
  public void setBucketCrossOriginConfiguration(
    String bucketName,
    BucketCrossOriginConfiguration bucketCrossOriginConfiguration
  ) {
    run(() ->
      getDelegate()
        .setBucketCrossOriginConfiguration(bucketName, bucketCrossOriginConfiguration)
    );
  }

  @Override
  public void setBucketCrossOriginConfiguration(
    SetBucketCrossOriginConfigurationRequest setBucketCrossOriginConfigurationRequest
  ) {
    run(() ->
      getDelegate()
        .setBucketCrossOriginConfiguration(setBucketCrossOriginConfigurationRequest)
    );
  }

  @Override
  public void deleteBucketCrossOriginConfiguration(String bucketName) {
    run(() -> getDelegate().deleteBucketCrossOriginConfiguration(bucketName));
  }

  @Override
  public void deleteBucketCrossOriginConfiguration(
    DeleteBucketCrossOriginConfigurationRequest deleteBucketCrossOriginConfigurationRequest
  ) {
    run(() ->
      getDelegate()
        .deleteBucketCrossOriginConfiguration(deleteBucketCrossOriginConfigurationRequest)
    );
  }

  @Override
  public BucketTaggingConfiguration getBucketTaggingConfiguration(String bucketName) {
    return call(() -> getDelegate().getBucketTaggingConfiguration(bucketName));
  }

  @Override
  public BucketTaggingConfiguration getBucketTaggingConfiguration(
    GetBucketTaggingConfigurationRequest getBucketTaggingConfigurationRequest
  ) {
    return call(() ->
      getDelegate().getBucketTaggingConfiguration(getBucketTaggingConfigurationRequest)
    );
  }

  @Override
  public void setBucketTaggingConfiguration(
    String bucketName,
    BucketTaggingConfiguration bucketTaggingConfiguration
  ) {
    run(() ->
      getDelegate().setBucketTaggingConfiguration(bucketName, bucketTaggingConfiguration)
    );
  }

  @Override
  public void setBucketTaggingConfiguration(
    SetBucketTaggingConfigurationRequest setBucketTaggingConfigurationRequest
  ) {
    run(() ->
      getDelegate().setBucketTaggingConfiguration(setBucketTaggingConfigurationRequest)
    );
  }

  @Override
  public void deleteBucketTaggingConfiguration(String bucketName) {
    run(() -> getDelegate().deleteBucketTaggingConfiguration(bucketName));
  }

  @Override
  public void deleteBucketTaggingConfiguration(
    DeleteBucketTaggingConfigurationRequest deleteBucketTaggingConfigurationRequest
  ) {
    run(() ->
      getDelegate()
        .deleteBucketTaggingConfiguration(deleteBucketTaggingConfigurationRequest)
    );
  }

  @Override
  public BucketNotificationConfiguration getBucketNotificationConfiguration(
    String bucketName
  ) throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().getBucketNotificationConfiguration(bucketName));
  }

  @Override
  public BucketNotificationConfiguration getBucketNotificationConfiguration(
    GetBucketNotificationConfigurationRequest getBucketNotificationConfigurationRequest
  ) throws SdkClientException, AmazonServiceException {
    return call(() ->
      getDelegate()
        .getBucketNotificationConfiguration(getBucketNotificationConfigurationRequest)
    );
  }

  @Override
  public void setBucketNotificationConfiguration(
    String bucketName,
    BucketNotificationConfiguration bucketNotificationConfiguration
  ) throws SdkClientException, AmazonServiceException {
    run(() ->
      getDelegate()
        .setBucketNotificationConfiguration(bucketName, bucketNotificationConfiguration)
    );
  }

  @Override
  public void setBucketNotificationConfiguration(
    SetBucketNotificationConfigurationRequest setBucketNotificationConfigurationRequest
  ) throws SdkClientException, AmazonServiceException {
    run(() ->
      getDelegate()
        .setBucketNotificationConfiguration(setBucketNotificationConfigurationRequest)
    );
  }

  @Override
  public BucketWebsiteConfiguration getBucketWebsiteConfiguration(String bucketName)
    throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().getBucketWebsiteConfiguration(bucketName));
  }

  @Override
  public BucketWebsiteConfiguration getBucketWebsiteConfiguration(
    GetBucketWebsiteConfigurationRequest getBucketWebsiteConfigurationRequest
  ) throws SdkClientException, AmazonServiceException {
    return call(() ->
      getDelegate().getBucketWebsiteConfiguration(getBucketWebsiteConfigurationRequest)
    );
  }

  @Override
  public void setBucketWebsiteConfiguration(
    String bucketName,
    BucketWebsiteConfiguration configuration
  ) throws SdkClientException, AmazonServiceException {
    run(() -> getDelegate().setBucketWebsiteConfiguration(bucketName, configuration));
  }

  @Override
  public void setBucketWebsiteConfiguration(
    SetBucketWebsiteConfigurationRequest setBucketWebsiteConfigurationRequest
  ) throws SdkClientException, AmazonServiceException {
    run(() ->
      getDelegate().setBucketWebsiteConfiguration(setBucketWebsiteConfigurationRequest)
    );
  }

  @Override
  public void deleteBucketWebsiteConfiguration(String bucketName)
    throws SdkClientException, AmazonServiceException {
    run(() -> getDelegate().deleteBucketWebsiteConfiguration(bucketName));
  }

  @Override
  public void deleteBucketWebsiteConfiguration(
    DeleteBucketWebsiteConfigurationRequest deleteBucketWebsiteConfigurationRequest
  ) throws SdkClientException, AmazonServiceException {
    run(() ->
      getDelegate()
        .deleteBucketWebsiteConfiguration(deleteBucketWebsiteConfigurationRequest)
    );
  }

  @Override
  public BucketPolicy getBucketPolicy(String bucketName)
    throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().getBucketPolicy(bucketName));
  }

  @Override
  public BucketPolicy getBucketPolicy(GetBucketPolicyRequest getBucketPolicyRequest)
    throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().getBucketPolicy(getBucketPolicyRequest));
  }

  @Override
  public void setBucketPolicy(String bucketName, String policyText)
    throws SdkClientException, AmazonServiceException {
    run(() -> getDelegate().setBucketPolicy(bucketName, policyText));
  }

  @Override
  public void setBucketPolicy(SetBucketPolicyRequest setBucketPolicyRequest)
    throws SdkClientException, AmazonServiceException {
    run(() -> getDelegate().setBucketPolicy(setBucketPolicyRequest));
  }

  @Override
  public void deleteBucketPolicy(String bucketName)
    throws SdkClientException, AmazonServiceException {
    run(() -> getDelegate().deleteBucketPolicy(bucketName));
  }

  @Override
  public void deleteBucketPolicy(DeleteBucketPolicyRequest deleteBucketPolicyRequest)
    throws SdkClientException, AmazonServiceException {
    run(() -> getDelegate().deleteBucketPolicy(deleteBucketPolicyRequest));
  }

  @Override
  public URL generatePresignedUrl(String bucketName, String key, Date expiration)
    throws SdkClientException {
    return call(() -> getDelegate().generatePresignedUrl(bucketName, key, expiration));
  }

  @Override
  public URL generatePresignedUrl(
    String bucketName,
    String key,
    Date expiration,
    HttpMethod method
  ) throws SdkClientException {
    return call(() ->
      getDelegate().generatePresignedUrl(bucketName, key, expiration, method)
    );
  }

  @Override
  public URL generatePresignedUrl(
    GeneratePresignedUrlRequest generatePresignedUrlRequest
  ) throws SdkClientException {
    return call(() -> getDelegate().generatePresignedUrl(generatePresignedUrlRequest));
  }

  @Override
  public InitiateMultipartUploadResult initiateMultipartUpload(
    InitiateMultipartUploadRequest request
  ) {
    return call(() -> getDelegate().initiateMultipartUpload(request));
  }

  @Override
  public UploadPartResult uploadPart(UploadPartRequest request) {
    return call(() -> getDelegate().uploadPart(request));
  }

  @Override
  public PartListing listParts(ListPartsRequest request)
    throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().listParts(request));
  }

  @Override
  public void abortMultipartUpload(AbortMultipartUploadRequest request) {
    run(() -> getDelegate().abortMultipartUpload(request));
  }

  @Override
  public CompleteMultipartUploadResult completeMultipartUpload(
    CompleteMultipartUploadRequest request
  ) {
    return call(() -> getDelegate().completeMultipartUpload(request));
  }

  @Override
  public MultipartUploadListing listMultipartUploads(ListMultipartUploadsRequest request)
    throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().listMultipartUploads(request));
  }

  @Override
  public S3ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request) {
    return getDelegate().getCachedResponseMetadata(request);
  }

  @Override
  public void restoreObject(String bucketName, String key, int expirationInDays)
    throws AmazonServiceException {
    run(() -> getDelegate().restoreObject(bucketName, key, expirationInDays));
  }

  @Override
  public void restoreObject(RestoreObjectRequest request) throws AmazonServiceException {
    run(() -> getDelegate().restoreObject(request));
  }

  @Override
  public void enableRequesterPays(String bucketName)
    throws AmazonServiceException, AmazonServiceException {
    run(() -> getDelegate().enableRequesterPays(bucketName));
  }

  @Override
  public void disableRequesterPays(String bucketName)
    throws AmazonServiceException, AmazonServiceException {
    run(() -> getDelegate().disableRequesterPays(bucketName));
  }

  @Override
  public boolean isRequesterPaysEnabled(String bucketName)
    throws AmazonServiceException, AmazonServiceException {
    return call(() -> getDelegate().isRequesterPaysEnabled(bucketName));
  }

  @Override
  public void setBucketReplicationConfiguration(
    String bucketName,
    BucketReplicationConfiguration configuration
  ) throws AmazonServiceException, AmazonServiceException {
    run(() -> getDelegate().setBucketReplicationConfiguration(bucketName, configuration));
  }

  @Override
  public void setBucketReplicationConfiguration(
    SetBucketReplicationConfigurationRequest setBucketReplicationConfigurationRequest
  ) throws AmazonServiceException, AmazonServiceException {
    run(() ->
      getDelegate()
        .setBucketReplicationConfiguration(setBucketReplicationConfigurationRequest)
    );
  }

  @Override
  public BucketReplicationConfiguration getBucketReplicationConfiguration(
    String bucketName
  ) throws AmazonServiceException, AmazonServiceException {
    return call(() -> getDelegate().getBucketReplicationConfiguration(bucketName));
  }

  @Override
  public BucketReplicationConfiguration getBucketReplicationConfiguration(
    GetBucketReplicationConfigurationRequest getBucketReplicationConfigurationRequest
  ) throws AmazonServiceException, AmazonServiceException {
    return call(() ->
      getDelegate()
        .getBucketReplicationConfiguration(getBucketReplicationConfigurationRequest)
    );
  }

  @Override
  public void deleteBucketReplicationConfiguration(String bucketName)
    throws AmazonServiceException, AmazonServiceException {
    run(() -> getDelegate().deleteBucketReplicationConfiguration(bucketName));
  }

  @Override
  public void deleteBucketReplicationConfiguration(
    DeleteBucketReplicationConfigurationRequest request
  ) throws AmazonServiceException, AmazonServiceException {
    run(() -> getDelegate().deleteBucketReplicationConfiguration(request));
  }

  @Override
  public boolean doesObjectExist(String bucketName, String objectName)
    throws AmazonServiceException, AmazonServiceException {
    return call(() -> getDelegate().doesObjectExist(bucketName, objectName));
  }

  @Override
  public BucketAccelerateConfiguration getBucketAccelerateConfiguration(
    String bucketName
  ) throws AmazonServiceException, AmazonServiceException {
    return call(() -> getDelegate().getBucketAccelerateConfiguration(bucketName));
  }

  @Override
  public BucketAccelerateConfiguration getBucketAccelerateConfiguration(
    GetBucketAccelerateConfigurationRequest getBucketAccelerateConfigurationRequest
  ) throws AmazonServiceException, AmazonServiceException {
    return call(() ->
      getDelegate()
        .getBucketAccelerateConfiguration(getBucketAccelerateConfigurationRequest)
    );
  }

  @Override
  public void setBucketAccelerateConfiguration(
    String bucketName,
    BucketAccelerateConfiguration accelerateConfiguration
  ) throws AmazonServiceException, AmazonServiceException {
    run(() ->
      getDelegate().setBucketAccelerateConfiguration(bucketName, accelerateConfiguration)
    );
  }

  @Override
  public void setBucketAccelerateConfiguration(
    SetBucketAccelerateConfigurationRequest setBucketAccelerateConfigurationRequest
  ) throws AmazonServiceException, AmazonServiceException {
    run(() ->
      getDelegate()
        .setBucketAccelerateConfiguration(setBucketAccelerateConfigurationRequest)
    );
  }

  @Override
  public DeleteBucketMetricsConfigurationResult deleteBucketMetricsConfiguration(
    String bucketName,
    String id
  ) throws AmazonServiceException, AmazonServiceException {
    return call(() -> getDelegate().deleteBucketMetricsConfiguration(bucketName, id));
  }

  @Override
  public DeleteBucketMetricsConfigurationResult deleteBucketMetricsConfiguration(
    DeleteBucketMetricsConfigurationRequest deleteBucketMetricsConfigurationRequest
  ) throws AmazonServiceException, AmazonServiceException {
    return call(() ->
      getDelegate()
        .deleteBucketMetricsConfiguration(deleteBucketMetricsConfigurationRequest)
    );
  }

  @Override
  public GetBucketMetricsConfigurationResult getBucketMetricsConfiguration(
    String bucketName,
    String id
  ) throws AmazonServiceException, AmazonServiceException {
    return call(() -> getDelegate().getBucketMetricsConfiguration(bucketName, id));
  }

  @Override
  public GetBucketMetricsConfigurationResult getBucketMetricsConfiguration(
    GetBucketMetricsConfigurationRequest getBucketMetricsConfigurationRequest
  ) throws AmazonServiceException, AmazonServiceException {
    return call(() ->
      getDelegate().getBucketMetricsConfiguration(getBucketMetricsConfigurationRequest)
    );
  }

  @Override
  public SetBucketMetricsConfigurationResult setBucketMetricsConfiguration(
    String bucketName,
    MetricsConfiguration metricsConfiguration
  ) throws AmazonServiceException, AmazonServiceException {
    return call(() ->
      getDelegate().setBucketMetricsConfiguration(bucketName, metricsConfiguration)
    );
  }

  @Override
  public SetBucketMetricsConfigurationResult setBucketMetricsConfiguration(
    SetBucketMetricsConfigurationRequest setBucketMetricsConfigurationRequest
  ) throws AmazonServiceException, AmazonServiceException {
    return call(() ->
      getDelegate().setBucketMetricsConfiguration(setBucketMetricsConfigurationRequest)
    );
  }

  @Override
  public ListBucketMetricsConfigurationsResult listBucketMetricsConfigurations(
    ListBucketMetricsConfigurationsRequest listBucketMetricsConfigurationsRequest
  ) throws AmazonServiceException, AmazonServiceException {
    return call(() ->
      getDelegate()
        .listBucketMetricsConfigurations(listBucketMetricsConfigurationsRequest)
    );
  }

  @Override
  public DeleteBucketAnalyticsConfigurationResult deleteBucketAnalyticsConfiguration(
    String bucketName,
    String id
  ) throws AmazonServiceException, AmazonServiceException {
    return call(() -> getDelegate().deleteBucketAnalyticsConfiguration(bucketName, id));
  }

  @Override
  public DeleteBucketAnalyticsConfigurationResult deleteBucketAnalyticsConfiguration(
    DeleteBucketAnalyticsConfigurationRequest deleteBucketAnalyticsConfigurationRequest
  ) throws AmazonServiceException, AmazonServiceException {
    return call(() ->
      getDelegate()
        .deleteBucketAnalyticsConfiguration(deleteBucketAnalyticsConfigurationRequest)
    );
  }

  @Override
  public GetBucketAnalyticsConfigurationResult getBucketAnalyticsConfiguration(
    String bucketName,
    String id
  ) throws AmazonServiceException, AmazonServiceException {
    return call(() -> getDelegate().getBucketAnalyticsConfiguration(bucketName, id));
  }

  @Override
  public GetBucketAnalyticsConfigurationResult getBucketAnalyticsConfiguration(
    GetBucketAnalyticsConfigurationRequest getBucketAnalyticsConfigurationRequest
  ) throws AmazonServiceException, AmazonServiceException {
    return call(() ->
      getDelegate()
        .getBucketAnalyticsConfiguration(getBucketAnalyticsConfigurationRequest)
    );
  }

  @Override
  public SetBucketAnalyticsConfigurationResult setBucketAnalyticsConfiguration(
    String bucketName,
    AnalyticsConfiguration analyticsConfiguration
  ) throws AmazonServiceException, AmazonServiceException {
    return call(() ->
      getDelegate().setBucketAnalyticsConfiguration(bucketName, analyticsConfiguration)
    );
  }

  @Override
  public SetBucketAnalyticsConfigurationResult setBucketAnalyticsConfiguration(
    SetBucketAnalyticsConfigurationRequest setBucketAnalyticsConfigurationRequest
  ) throws AmazonServiceException, AmazonServiceException {
    return call(() ->
      getDelegate()
        .setBucketAnalyticsConfiguration(setBucketAnalyticsConfigurationRequest)
    );
  }

  @Override
  public ListBucketAnalyticsConfigurationsResult listBucketAnalyticsConfigurations(
    ListBucketAnalyticsConfigurationsRequest listBucketAnalyticsConfigurationsRequest
  ) throws AmazonServiceException, AmazonServiceException {
    return call(() ->
      getDelegate()
        .listBucketAnalyticsConfigurations(listBucketAnalyticsConfigurationsRequest)
    );
  }

  @Override
  public DeleteBucketInventoryConfigurationResult deleteBucketInventoryConfiguration(
    String bucketName,
    String id
  ) throws AmazonServiceException, AmazonServiceException {
    return call(() -> getDelegate().deleteBucketInventoryConfiguration(bucketName, id));
  }

  @Override
  public DeleteBucketInventoryConfigurationResult deleteBucketInventoryConfiguration(
    DeleteBucketInventoryConfigurationRequest deleteBucketInventoryConfigurationRequest
  ) throws AmazonServiceException, AmazonServiceException {
    return call(() ->
      getDelegate()
        .deleteBucketInventoryConfiguration(deleteBucketInventoryConfigurationRequest)
    );
  }

  @Override
  public GetBucketInventoryConfigurationResult getBucketInventoryConfiguration(
    String bucketName,
    String id
  ) throws AmazonServiceException, AmazonServiceException {
    return call(() -> getDelegate().getBucketInventoryConfiguration(bucketName, id));
  }

  @Override
  public GetBucketInventoryConfigurationResult getBucketInventoryConfiguration(
    GetBucketInventoryConfigurationRequest getBucketInventoryConfigurationRequest
  ) throws AmazonServiceException, AmazonServiceException {
    return call(() ->
      getDelegate()
        .getBucketInventoryConfiguration(getBucketInventoryConfigurationRequest)
    );
  }

  @Override
  public SetBucketInventoryConfigurationResult setBucketInventoryConfiguration(
    String bucketName,
    InventoryConfiguration inventoryConfiguration
  ) throws AmazonServiceException, AmazonServiceException {
    return call(() ->
      getDelegate().setBucketInventoryConfiguration(bucketName, inventoryConfiguration)
    );
  }

  @Override
  public SetBucketInventoryConfigurationResult setBucketInventoryConfiguration(
    SetBucketInventoryConfigurationRequest setBucketInventoryConfigurationRequest
  ) throws AmazonServiceException, AmazonServiceException {
    return call(() ->
      getDelegate()
        .setBucketInventoryConfiguration(setBucketInventoryConfigurationRequest)
    );
  }

  @Override
  public ListBucketInventoryConfigurationsResult listBucketInventoryConfigurations(
    ListBucketInventoryConfigurationsRequest listBucketInventoryConfigurationsRequest
  ) throws AmazonServiceException, AmazonServiceException {
    return call(() ->
      getDelegate()
        .listBucketInventoryConfigurations(listBucketInventoryConfigurationsRequest)
    );
  }

  @Override
  public com.amazonaws.services.s3.model.Region getRegion() {
    return getDelegate().getRegion();
  }

  @Override
  public String getRegionName() {
    return getDelegate().getRegionName();
  }

  @Override
  public URL getUrl(String bucketName, String key) {
    return getDelegate().getUrl(bucketName, key);
  }

  @Override
  public AmazonS3Waiters waiters() {
    return getDelegate().waiters();
  }

  @Override
  public DeleteBucketEncryptionResult deleteBucketEncryption(String bucketName)
    throws SdkClientException {
    return call(() -> getDelegate().deleteBucketEncryption(bucketName));
  }

  @Override
  public DeleteBucketEncryptionResult deleteBucketEncryption(
    DeleteBucketEncryptionRequest request
  ) throws SdkClientException {
    return call(() -> getDelegate().deleteBucketEncryption(request));
  }

  @Override
  public GetBucketEncryptionResult getBucketEncryption(String bucketName)
    throws SdkClientException {
    return call(() -> getDelegate().getBucketEncryption(bucketName));
  }

  @Override
  public GetBucketEncryptionResult getBucketEncryption(
    GetBucketEncryptionRequest request
  ) throws SdkClientException {
    return call(() -> getDelegate().getBucketEncryption(request));
  }

  @Override
  public SetBucketEncryptionResult setBucketEncryption(
    SetBucketEncryptionRequest request
  ) throws SdkClientException {
    return call(() -> getDelegate().setBucketEncryption(request));
  }

  @Override
  public boolean doesBucketExistV2(String bucketName)
    throws SdkClientException, AmazonServiceException {
    return call(() -> getDelegate().doesBucketExistV2(bucketName));
  }

  @Override
  public RestoreObjectResult restoreObjectV2(RestoreObjectRequest request)
    throws AmazonServiceException {
    return call(() -> getDelegate().restoreObjectV2(request));
  }

  @Override
  public SetPublicAccessBlockResult setPublicAccessBlock(
    SetPublicAccessBlockRequest request
  ) {
    return call(() -> getDelegate().setPublicAccessBlock(request));
  }

  @Override
  public GetPublicAccessBlockResult getPublicAccessBlock(
    GetPublicAccessBlockRequest request
  ) {
    return call(() -> getDelegate().getPublicAccessBlock(request));
  }

  @Override
  public DeletePublicAccessBlockResult deletePublicAccessBlock(
    DeletePublicAccessBlockRequest request
  ) {
    return call(() -> getDelegate().deletePublicAccessBlock(request));
  }

  @Override
  public GetBucketPolicyStatusResult getBucketPolicyStatus(
    GetBucketPolicyStatusRequest request
  ) {
    return call(() -> getDelegate().getBucketPolicyStatus(request));
  }

  @Override
  public SelectObjectContentResult selectObjectContent(
    SelectObjectContentRequest selectRequest
  ) throws AmazonServiceException, SdkClientException {
    return call(() -> getDelegate().selectObjectContent(selectRequest));
  }

  @Override
  public SetObjectLegalHoldResult setObjectLegalHold(
    SetObjectLegalHoldRequest setObjectLegalHoldRequest
  ) {
    return call(() -> getDelegate().setObjectLegalHold(setObjectLegalHoldRequest));
  }

  @Override
  public GetObjectLegalHoldResult getObjectLegalHold(
    GetObjectLegalHoldRequest getObjectLegalHoldRequest
  ) {
    return call(() -> getDelegate().getObjectLegalHold(getObjectLegalHoldRequest));
  }

  @Override
  public SetObjectLockConfigurationResult setObjectLockConfiguration(
    SetObjectLockConfigurationRequest setObjectLockConfigurationRequest
  ) {
    return call(() ->
      getDelegate().setObjectLockConfiguration(setObjectLockConfigurationRequest)
    );
  }

  @Override
  public GetObjectLockConfigurationResult getObjectLockConfiguration(
    GetObjectLockConfigurationRequest getObjectLockConfigurationRequest
  ) {
    return call(() ->
      getDelegate().getObjectLockConfiguration(getObjectLockConfigurationRequest)
    );
  }

  @Override
  public SetObjectRetentionResult setObjectRetention(
    SetObjectRetentionRequest setObjectRetentionRequest
  ) {
    return call(() -> getDelegate().setObjectRetention(setObjectRetentionRequest));
  }

  @Override
  public GetObjectRetentionResult getObjectRetention(
    GetObjectRetentionRequest getObjectRetentionRequest
  ) {
    return call(() -> getDelegate().getObjectRetention(getObjectRetentionRequest));
  }

  @Override
  public PresignedUrlDownloadResult download(
    PresignedUrlDownloadRequest presignedUrlDownloadRequest
  ) {
    return call(() -> getDelegate().download(presignedUrlDownloadRequest));
  }

  @Override
  public void download(
    PresignedUrlDownloadRequest presignedUrlDownloadRequest,
    File destinationFile
  ) {
    run(() -> getDelegate().download(presignedUrlDownloadRequest, destinationFile));
  }

  @Override
  public PresignedUrlUploadResult upload(
    PresignedUrlUploadRequest presignedUrlUploadRequest
  ) {
    return call(() -> getDelegate().upload(presignedUrlUploadRequest));
  }

  @Override
  public GetBucketOwnershipControlsResult getBucketOwnershipControls(
          GetBucketOwnershipControlsRequest ownershipControlsRequest
  ) {
    return call(() -> getDelegate().getBucketOwnershipControls(ownershipControlsRequest));
  }

  @Override
  public SetBucketOwnershipControlsResult setBucketOwnershipControls(
          SetBucketOwnershipControlsRequest ownershipControlsRequest
  ) {
    return call(() -> getDelegate().setBucketOwnershipControls(ownershipControlsRequest));
  }

  @Override
  public void shutdown() {
    getDelegate().shutdown();
  }
}
