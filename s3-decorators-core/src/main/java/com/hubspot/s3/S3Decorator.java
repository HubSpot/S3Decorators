package com.hubspot.s3;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;

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
import com.amazonaws.services.s3.model.DeleteVersionRequest;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.GetBucketAccelerateConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketAclRequest;
import com.amazonaws.services.s3.model.GetBucketAnalyticsConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketAnalyticsConfigurationResult;
import com.amazonaws.services.s3.model.GetBucketCrossOriginConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketInventoryConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketInventoryConfigurationResult;
import com.amazonaws.services.s3.model.GetBucketLifecycleConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketLocationRequest;
import com.amazonaws.services.s3.model.GetBucketLoggingConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketMetricsConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketMetricsConfigurationResult;
import com.amazonaws.services.s3.model.GetBucketNotificationConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketPolicyRequest;
import com.amazonaws.services.s3.model.GetBucketReplicationConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketTaggingConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketVersioningConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketWebsiteConfigurationRequest;
import com.amazonaws.services.s3.model.GetObjectAclRequest;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
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
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.RestoreObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.SetBucketAccelerateConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketAclRequest;
import com.amazonaws.services.s3.model.SetBucketAnalyticsConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketAnalyticsConfigurationResult;
import com.amazonaws.services.s3.model.SetBucketCrossOriginConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketInventoryConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketInventoryConfigurationResult;
import com.amazonaws.services.s3.model.SetBucketLifecycleConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketLoggingConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketMetricsConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketMetricsConfigurationResult;
import com.amazonaws.services.s3.model.SetBucketNotificationConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketPolicyRequest;
import com.amazonaws.services.s3.model.SetBucketReplicationConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketTaggingConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketVersioningConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketWebsiteConfigurationRequest;
import com.amazonaws.services.s3.model.SetObjectAclRequest;
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;
import com.amazonaws.services.s3.model.SetObjectTaggingResult;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.amazonaws.services.s3.model.VersionListing;
import com.amazonaws.services.s3.model.analytics.AnalyticsConfiguration;
import com.amazonaws.services.s3.model.inventory.InventoryConfiguration;
import com.amazonaws.services.s3.model.metrics.MetricsConfiguration;
import com.amazonaws.services.s3.waiters.AmazonS3Waiters;

public abstract class S3Decorator extends AbstractAmazonS3 {
  protected abstract <T> T call(Function<AmazonS3, T> function);

  protected void run(Consumer<AmazonS3> consumer) {
    call(s3 -> {
      consumer.accept(s3);
      return null;
    });
  }

  @Override
  public void setEndpoint(String endpoint) {
    run(s3 -> s3.setEndpoint(endpoint));
  }

  @Override
  public void setRegion(Region region) throws IllegalArgumentException {
    run(s3 -> s3.setRegion(region));
  }

  @Override
  public void setS3ClientOptions(S3ClientOptions clientOptions) {
    run(s3 -> s3.setS3ClientOptions(clientOptions));
  }

  @Override
  public void changeObjectStorageClass(String bucketName, String key, StorageClass newStorageClass) throws SdkClientException, AmazonServiceException {
    run(s3 -> s3.changeObjectStorageClass(bucketName, key, newStorageClass));
  }

  @Override
  public void setObjectRedirectLocation(String bucketName, String key, String newRedirectLocation) throws SdkClientException, AmazonServiceException {
    run(s3 -> s3.setObjectRedirectLocation(bucketName, key, newRedirectLocation));
  }

  @Override
  public ObjectListing listObjects(String bucketName) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.listObjects(bucketName));
  }

  @Override
  public ObjectListing listObjects(String bucketName, String prefix) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.listObjects(bucketName, prefix));
  }

  @Override
  public ObjectListing listObjects(ListObjectsRequest listObjectsRequest) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.listObjects(listObjectsRequest));
  }

  @Override
  public ListObjectsV2Result listObjectsV2(String bucketName) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.listObjectsV2(bucketName));
  }

  @Override
  public ListObjectsV2Result listObjectsV2(String bucketName, String prefix) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.listObjectsV2(bucketName, prefix));
  }

  @Override
  public ListObjectsV2Result listObjectsV2(ListObjectsV2Request listObjectsV2Request) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.listObjectsV2(listObjectsV2Request));
  }

  @Override
  public ObjectListing listNextBatchOfObjects(ObjectListing previousObjectListing) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.listNextBatchOfObjects(previousObjectListing));
  }

  @Override
  public ObjectListing listNextBatchOfObjects(ListNextBatchOfObjectsRequest listNextBatchOfObjectsRequest) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.listNextBatchOfObjects(listNextBatchOfObjectsRequest));
  }

  @Override
  public VersionListing listVersions(String bucketName, String prefix) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.listVersions(bucketName, prefix));
  }

  @Override
  public VersionListing listVersions(String bucketName, String prefix, String keyMarker, String versionIdMarker, String delimiter, Integer maxResults) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.listVersions(bucketName, prefix, keyMarker, versionIdMarker, delimiter, maxResults));
  }

  @Override
  public VersionListing listVersions(ListVersionsRequest listVersionsRequest) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.listVersions(listVersionsRequest));
  }

  @Override
  public VersionListing listNextBatchOfVersions(VersionListing previousVersionListing) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.listNextBatchOfVersions(previousVersionListing));
  }

  @Override
  public VersionListing listNextBatchOfVersions(ListNextBatchOfVersionsRequest listNextBatchOfVersionsRequest) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.listNextBatchOfVersions(listNextBatchOfVersionsRequest));
  }

  @Override
  public Owner getS3AccountOwner() throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.getS3AccountOwner());
  }

  @Override
  public Owner getS3AccountOwner(GetS3AccountOwnerRequest getS3AccountOwnerRequest) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.getS3AccountOwner(getS3AccountOwnerRequest));
  }

  @Override
  public boolean doesBucketExist(String bucketName) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.doesBucketExist(bucketName));
  }

  @Override
  public HeadBucketResult headBucket(HeadBucketRequest headBucketRequest) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.headBucket(headBucketRequest));
  }

  @Override
  public List<Bucket> listBuckets() throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.listBuckets());
  }

  @Override
  public List<Bucket> listBuckets(ListBucketsRequest listBucketsRequest) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.listBuckets(listBucketsRequest));
  }

  @Override
  public String getBucketLocation(String bucketName) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.getBucketLocation(bucketName));
  }

  @Override
  public String getBucketLocation(GetBucketLocationRequest getBucketLocationRequest) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.getBucketLocation(getBucketLocationRequest));
  }

  @Override
  public Bucket createBucket(CreateBucketRequest createBucketRequest) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.createBucket(createBucketRequest));
  }

  @Override
  public Bucket createBucket(String bucketName) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.createBucket(bucketName));
  }

  @Override
  public Bucket createBucket(String bucketName, com.amazonaws.services.s3.model.Region region) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.createBucket(bucketName, region));
  }

  @Override
  public Bucket createBucket(String bucketName, String region) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.createBucket(bucketName, region));
  }

  @Override
  public AccessControlList getObjectAcl(String bucketName, String key) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.getObjectAcl(bucketName, key));
  }

  @Override
  public AccessControlList getObjectAcl(String bucketName, String key, String versionId) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.getObjectAcl(bucketName, key, versionId));
  }

  @Override
  public AccessControlList getObjectAcl(GetObjectAclRequest getObjectAclRequest) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.getObjectAcl(getObjectAclRequest));
  }

  @Override
  public void setObjectAcl(String bucketName, String key, AccessControlList acl) throws SdkClientException, AmazonServiceException {
    run(s3 -> s3.setObjectAcl(bucketName, key, acl));
  }

  @Override
  public void setObjectAcl(String bucketName, String key, CannedAccessControlList acl) throws SdkClientException, AmazonServiceException {
    run(s3 -> s3.setObjectAcl(bucketName, key, acl));
  }

  @Override
  public void setObjectAcl(String bucketName, String key, String versionId, AccessControlList acl) throws SdkClientException, AmazonServiceException {
    run(s3 -> s3.setObjectAcl(bucketName, key, versionId, acl));
  }

  @Override
  public void setObjectAcl(String bucketName, String key, String versionId, CannedAccessControlList acl) throws SdkClientException, AmazonServiceException {
    run(s3 -> s3.setObjectAcl(bucketName, key, versionId, acl));
  }

  @Override
  public void setObjectAcl(SetObjectAclRequest setObjectAclRequest) throws SdkClientException, AmazonServiceException {
    run(s3 -> s3.setObjectAcl(setObjectAclRequest));
  }

  @Override
  public AccessControlList getBucketAcl(String bucketName) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.getBucketAcl(bucketName));
  }

  @Override
  public AccessControlList getBucketAcl(GetBucketAclRequest getBucketAclRequest) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.getBucketAcl(getBucketAclRequest));
  }

  @Override
  public void setBucketAcl(String bucketName, AccessControlList acl) throws SdkClientException, AmazonServiceException {
    run(s3 -> s3.setBucketAcl(bucketName, acl));
  }

  @Override
  public void setBucketAcl(String bucketName, CannedAccessControlList cannedAcl) throws SdkClientException, AmazonServiceException {
    run(s3 -> s3.setBucketAcl(bucketName, cannedAcl));
  }

  @Override
  public void setBucketAcl(SetBucketAclRequest setBucketAclRequest) throws SdkClientException, AmazonServiceException {
    run(s3 -> s3.setBucketAcl(setBucketAclRequest));
  }

  @Override
  public ObjectMetadata getObjectMetadata(String bucketName, String key) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.getObjectMetadata(bucketName, key));
  }

  @Override
  public ObjectMetadata getObjectMetadata(GetObjectMetadataRequest getObjectMetadataRequest) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.getObjectMetadata(getObjectMetadataRequest));
  }

  @Override
  public S3Object getObject(String bucketName, String key) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.getObject(bucketName, key));
  }

  @Override
  public S3Object getObject(GetObjectRequest getObjectRequest) {
    return call(s3 -> s3.getObject(getObjectRequest));
  }

  @Override
  public ObjectMetadata getObject(GetObjectRequest getObjectRequest, File destinationFile) {
    return call(s3 -> s3.getObject(getObjectRequest, destinationFile));
  }

  @Override
  public String getObjectAsString(String bucketName, String key) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.getObjectAsString(bucketName, key));
  }

  @Override
  public GetObjectTaggingResult getObjectTagging(GetObjectTaggingRequest objectTaggingRequest) {
    return call(s3 -> s3.getObjectTagging(objectTaggingRequest));
  }

  @Override
  public SetObjectTaggingResult setObjectTagging(SetObjectTaggingRequest setObjectTaggingRequest) {
    return call(s3 -> s3.setObjectTagging(setObjectTaggingRequest));
  }

  @Override
  public DeleteObjectTaggingResult deleteObjectTagging(DeleteObjectTaggingRequest deleteObjectTaggingRequest) {
    return call(s3 -> s3.deleteObjectTagging(deleteObjectTaggingRequest));
  }

  @Override
  public void deleteBucket(String bucketName) throws SdkClientException, AmazonServiceException {
    run(s3 -> s3.deleteBucket(bucketName));
  }

  @Override
  public void deleteBucket(DeleteBucketRequest deleteBucketRequest) throws SdkClientException, AmazonServiceException {
    run(s3 -> s3.deleteBucket(deleteBucketRequest));
  }

  @Override
  public PutObjectResult putObject(String bucketName, String key, File file) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.putObject(bucketName, key, file));
  }

  @Override
  public PutObjectResult putObject(String bucketName, String key, InputStream input, ObjectMetadata metadata) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.putObject(bucketName, key, input, metadata));
  }

  @Override
  public PutObjectResult putObject(PutObjectRequest putObjectRequest) {
    return call(s3 -> s3.putObject(putObjectRequest));
  }

  @Override
  public PutObjectResult putObject(String bucketName, String key, String content) throws AmazonServiceException, AmazonServiceException {
    return call(s3 -> s3.putObject(bucketName, key, content));
  }

  @Override
  public CopyObjectResult copyObject(String sourceBucketName, String sourceKey, String destinationBucketName, String destinationKey) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.copyObject(sourceBucketName, sourceKey, destinationBucketName, destinationKey));
  }

  @Override
  public CopyObjectResult copyObject(CopyObjectRequest copyObjectRequest) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.copyObject(copyObjectRequest));
  }

  @Override
  public CopyPartResult copyPart(CopyPartRequest copyPartRequest) {
    return call(s3 -> s3.copyPart(copyPartRequest));
  }

  @Override
  public void deleteObject(String bucketName, String key) throws SdkClientException, AmazonServiceException {
    run(s3 -> s3.deleteObject(bucketName, key));
  }

  @Override
  public void deleteObject(DeleteObjectRequest deleteObjectRequest) throws SdkClientException, AmazonServiceException {
    run(s3 -> s3.deleteObject(deleteObjectRequest));
  }

  @Override
  public DeleteObjectsResult deleteObjects(DeleteObjectsRequest deleteObjectsRequest) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.deleteObjects(deleteObjectsRequest));
  }

  @Override
  public void deleteVersion(String bucketName, String key, String versionId) throws SdkClientException, AmazonServiceException {
    run(s3 -> s3.deleteVersion(bucketName, key, versionId));
  }

  @Override
  public void deleteVersion(DeleteVersionRequest deleteVersionRequest) throws SdkClientException, AmazonServiceException {
    run(s3 -> s3.deleteVersion(deleteVersionRequest));
  }

  @Override
  public BucketLoggingConfiguration getBucketLoggingConfiguration(String bucketName) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.getBucketLoggingConfiguration(bucketName));
  }

  @Override
  public BucketLoggingConfiguration getBucketLoggingConfiguration(GetBucketLoggingConfigurationRequest getBucketLoggingConfigurationRequest) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.getBucketLoggingConfiguration(getBucketLoggingConfigurationRequest));
  }

  @Override
  public void setBucketLoggingConfiguration(SetBucketLoggingConfigurationRequest setBucketLoggingConfigurationRequest) throws SdkClientException, AmazonServiceException {
    run(s3 -> s3.setBucketLoggingConfiguration(setBucketLoggingConfigurationRequest));
  }

  @Override
  public BucketVersioningConfiguration getBucketVersioningConfiguration(String bucketName) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.getBucketVersioningConfiguration(bucketName));
  }

  @Override
  public BucketVersioningConfiguration getBucketVersioningConfiguration(GetBucketVersioningConfigurationRequest getBucketVersioningConfigurationRequest) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.getBucketVersioningConfiguration(getBucketVersioningConfigurationRequest));
  }

  @Override
  public void setBucketVersioningConfiguration(SetBucketVersioningConfigurationRequest setBucketVersioningConfigurationRequest) throws SdkClientException, AmazonServiceException {
    run(s3 -> s3.setBucketVersioningConfiguration(setBucketVersioningConfigurationRequest));
  }

  @Override
  public BucketLifecycleConfiguration getBucketLifecycleConfiguration(String bucketName) {
    return call(s3 -> s3.getBucketLifecycleConfiguration(bucketName));
  }

  @Override
  public BucketLifecycleConfiguration getBucketLifecycleConfiguration(GetBucketLifecycleConfigurationRequest getBucketLifecycleConfigurationRequest) {
    return call(s3 -> s3.getBucketLifecycleConfiguration(getBucketLifecycleConfigurationRequest));
  }

  @Override
  public void setBucketLifecycleConfiguration(String bucketName, BucketLifecycleConfiguration bucketLifecycleConfiguration) {
    run(s3 -> s3.setBucketLifecycleConfiguration(bucketName, bucketLifecycleConfiguration));
  }

  @Override
  public void setBucketLifecycleConfiguration(SetBucketLifecycleConfigurationRequest setBucketLifecycleConfigurationRequest) {
    run(s3 -> s3.setBucketLifecycleConfiguration(setBucketLifecycleConfigurationRequest));
  }

  @Override
  public void deleteBucketLifecycleConfiguration(String bucketName) {
    run(s3 -> s3.deleteBucketLifecycleConfiguration(bucketName));
  }

  @Override
  public void deleteBucketLifecycleConfiguration(DeleteBucketLifecycleConfigurationRequest deleteBucketLifecycleConfigurationRequest) {
    run(s3 -> s3.deleteBucketLifecycleConfiguration(deleteBucketLifecycleConfigurationRequest));
  }

  @Override
  public BucketCrossOriginConfiguration getBucketCrossOriginConfiguration(String bucketName) {
    return call(s3 -> s3.getBucketCrossOriginConfiguration(bucketName));
  }

  @Override
  public BucketCrossOriginConfiguration getBucketCrossOriginConfiguration(GetBucketCrossOriginConfigurationRequest getBucketCrossOriginConfigurationRequest) {
    return call(s3 -> s3.getBucketCrossOriginConfiguration(getBucketCrossOriginConfigurationRequest));
  }

  @Override
  public void setBucketCrossOriginConfiguration(String bucketName, BucketCrossOriginConfiguration bucketCrossOriginConfiguration) {
    run(s3 -> s3.setBucketCrossOriginConfiguration(bucketName, bucketCrossOriginConfiguration));
  }

  @Override
  public void setBucketCrossOriginConfiguration(SetBucketCrossOriginConfigurationRequest setBucketCrossOriginConfigurationRequest) {
    run(s3 -> s3.setBucketCrossOriginConfiguration(setBucketCrossOriginConfigurationRequest));
  }

  @Override
  public void deleteBucketCrossOriginConfiguration(String bucketName) {
    run(s3 -> s3.deleteBucketCrossOriginConfiguration(bucketName));
  }

  @Override
  public void deleteBucketCrossOriginConfiguration(DeleteBucketCrossOriginConfigurationRequest deleteBucketCrossOriginConfigurationRequest) {
    run(s3 -> s3.deleteBucketCrossOriginConfiguration(deleteBucketCrossOriginConfigurationRequest));
  }

  @Override
  public BucketTaggingConfiguration getBucketTaggingConfiguration(String bucketName) {
    return call(s3 -> s3.getBucketTaggingConfiguration(bucketName));
  }

  @Override
  public BucketTaggingConfiguration getBucketTaggingConfiguration(GetBucketTaggingConfigurationRequest getBucketTaggingConfigurationRequest) {
    return call(s3 -> s3.getBucketTaggingConfiguration(getBucketTaggingConfigurationRequest));
  }

  @Override
  public void setBucketTaggingConfiguration(String bucketName, BucketTaggingConfiguration bucketTaggingConfiguration) {
    run(s3 -> s3.setBucketTaggingConfiguration(bucketName, bucketTaggingConfiguration));
  }

  @Override
  public void setBucketTaggingConfiguration(SetBucketTaggingConfigurationRequest setBucketTaggingConfigurationRequest) {
    run(s3 -> s3.setBucketTaggingConfiguration(setBucketTaggingConfigurationRequest));
  }

  @Override
  public void deleteBucketTaggingConfiguration(String bucketName) {
    run(s3 -> s3.deleteBucketTaggingConfiguration(bucketName));
  }

  @Override
  public void deleteBucketTaggingConfiguration(DeleteBucketTaggingConfigurationRequest deleteBucketTaggingConfigurationRequest) {
    run(s3 -> s3.deleteBucketTaggingConfiguration(deleteBucketTaggingConfigurationRequest));
  }

  @Override
  public BucketNotificationConfiguration getBucketNotificationConfiguration(String bucketName) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.getBucketNotificationConfiguration(bucketName));
  }

  @Override
  public BucketNotificationConfiguration getBucketNotificationConfiguration(GetBucketNotificationConfigurationRequest getBucketNotificationConfigurationRequest) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.getBucketNotificationConfiguration(getBucketNotificationConfigurationRequest));
  }

  @Override
  public void setBucketNotificationConfiguration(String bucketName, BucketNotificationConfiguration bucketNotificationConfiguration) throws SdkClientException, AmazonServiceException {
    run(s3 -> s3.setBucketNotificationConfiguration(bucketName, bucketNotificationConfiguration));
  }

  @Override
  public void setBucketNotificationConfiguration(SetBucketNotificationConfigurationRequest setBucketNotificationConfigurationRequest) throws SdkClientException, AmazonServiceException {
    run(s3 -> s3.setBucketNotificationConfiguration(setBucketNotificationConfigurationRequest));
  }

  @Override
  public BucketWebsiteConfiguration getBucketWebsiteConfiguration(String bucketName) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.getBucketWebsiteConfiguration(bucketName));
  }

  @Override
  public BucketWebsiteConfiguration getBucketWebsiteConfiguration(GetBucketWebsiteConfigurationRequest getBucketWebsiteConfigurationRequest) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.getBucketWebsiteConfiguration(getBucketWebsiteConfigurationRequest));
  }

  @Override
  public void setBucketWebsiteConfiguration(String bucketName, BucketWebsiteConfiguration configuration) throws SdkClientException, AmazonServiceException {
    run(s3 -> s3.setBucketWebsiteConfiguration(bucketName, configuration));
  }

  @Override
  public void setBucketWebsiteConfiguration(SetBucketWebsiteConfigurationRequest setBucketWebsiteConfigurationRequest) throws SdkClientException, AmazonServiceException {
    run(s3 -> s3.setBucketWebsiteConfiguration(setBucketWebsiteConfigurationRequest));
  }

  @Override
  public void deleteBucketWebsiteConfiguration(String bucketName) throws SdkClientException, AmazonServiceException {
    run(s3 -> s3.deleteBucketWebsiteConfiguration(bucketName));
  }

  @Override
  public void deleteBucketWebsiteConfiguration(DeleteBucketWebsiteConfigurationRequest deleteBucketWebsiteConfigurationRequest) throws SdkClientException, AmazonServiceException {
    run(s3 -> s3.deleteBucketWebsiteConfiguration(deleteBucketWebsiteConfigurationRequest));
  }

  @Override
  public BucketPolicy getBucketPolicy(String bucketName) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.getBucketPolicy(bucketName));
  }

  @Override
  public BucketPolicy getBucketPolicy(GetBucketPolicyRequest getBucketPolicyRequest) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.getBucketPolicy(getBucketPolicyRequest));
  }

  @Override
  public void setBucketPolicy(String bucketName, String policyText) throws SdkClientException, AmazonServiceException {
    run(s3 -> s3.setBucketPolicy(bucketName, policyText));
  }

  @Override
  public void setBucketPolicy(SetBucketPolicyRequest setBucketPolicyRequest) throws SdkClientException, AmazonServiceException {
    run(s3 -> s3.setBucketPolicy(setBucketPolicyRequest));
  }

  @Override
  public void deleteBucketPolicy(String bucketName) throws SdkClientException, AmazonServiceException {
    run(s3 -> s3.deleteBucketPolicy(bucketName));
  }

  @Override
  public void deleteBucketPolicy(DeleteBucketPolicyRequest deleteBucketPolicyRequest) throws SdkClientException, AmazonServiceException {
    run(s3 -> s3.deleteBucketPolicy(deleteBucketPolicyRequest));
  }

  @Override
  public URL generatePresignedUrl(String bucketName, String key, Date expiration) throws SdkClientException {
    return call(s3 -> s3.generatePresignedUrl(bucketName, key, expiration));
  }

  @Override
  public URL generatePresignedUrl(String bucketName, String key, Date expiration, HttpMethod method) throws SdkClientException {
    return call(s3 -> s3.generatePresignedUrl(bucketName, key, expiration, method));
  }

  @Override
  public URL generatePresignedUrl(GeneratePresignedUrlRequest generatePresignedUrlRequest) throws SdkClientException {
    return call(s3 -> s3.generatePresignedUrl(generatePresignedUrlRequest));
  }

  @Override
  public InitiateMultipartUploadResult initiateMultipartUpload(InitiateMultipartUploadRequest request) {
    return call(s3 -> s3.initiateMultipartUpload(request));
  }

  @Override
  public UploadPartResult uploadPart(UploadPartRequest request) {
    return call(s3 -> s3.uploadPart(request));
  }

  @Override
  public PartListing listParts(ListPartsRequest request) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.listParts(request));
  }

  @Override
  public void abortMultipartUpload(AbortMultipartUploadRequest request) {
    run(s3 -> s3.abortMultipartUpload(request));
  }

  @Override
  public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest request) {
    return call(s3 -> s3.completeMultipartUpload(request));
  }

  @Override
  public MultipartUploadListing listMultipartUploads(ListMultipartUploadsRequest request) throws SdkClientException, AmazonServiceException {
    return call(s3 -> s3.listMultipartUploads(request));
  }

  @Override
  public S3ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request) {
    return call(s3 -> s3.getCachedResponseMetadata(request));
  }

  @Override
  public void restoreObject(String bucketName, String key, int expirationInDays) throws AmazonServiceException {
    run(s3 -> s3.restoreObject(bucketName, key, expirationInDays));
  }

  @Override
  public void restoreObject(RestoreObjectRequest request) throws AmazonServiceException {
    run(s3 -> s3.restoreObject(request));
  }

  @Override
  public void enableRequesterPays(String bucketName) throws AmazonServiceException, AmazonServiceException {
    run(s3 -> s3.enableRequesterPays(bucketName));
  }

  @Override
  public void disableRequesterPays(String bucketName) throws AmazonServiceException, AmazonServiceException {
    run(s3 -> s3.disableRequesterPays(bucketName));
  }

  @Override
  public boolean isRequesterPaysEnabled(String bucketName) throws AmazonServiceException, AmazonServiceException {
    return call(s3 -> s3.isRequesterPaysEnabled(bucketName));
  }

  @Override
  public void setBucketReplicationConfiguration(String bucketName, BucketReplicationConfiguration configuration) throws AmazonServiceException, AmazonServiceException {
    run(s3 -> s3.setBucketReplicationConfiguration(bucketName, configuration));
  }

  @Override
  public void setBucketReplicationConfiguration(SetBucketReplicationConfigurationRequest setBucketReplicationConfigurationRequest) throws AmazonServiceException, AmazonServiceException {
    run(s3 -> s3.setBucketReplicationConfiguration(setBucketReplicationConfigurationRequest));
  }

  @Override
  public BucketReplicationConfiguration getBucketReplicationConfiguration(String bucketName) throws AmazonServiceException, AmazonServiceException {
    return call(s3 -> s3.getBucketReplicationConfiguration(bucketName));
  }

  @Override
  public BucketReplicationConfiguration getBucketReplicationConfiguration(GetBucketReplicationConfigurationRequest getBucketReplicationConfigurationRequest) throws AmazonServiceException, AmazonServiceException {
    return call(s3 -> s3.getBucketReplicationConfiguration(getBucketReplicationConfigurationRequest));
  }

  @Override
  public void deleteBucketReplicationConfiguration(String bucketName) throws AmazonServiceException, AmazonServiceException {
    run(s3 -> s3.deleteBucketReplicationConfiguration(bucketName));
  }

  @Override
  public void deleteBucketReplicationConfiguration(DeleteBucketReplicationConfigurationRequest request) throws AmazonServiceException, AmazonServiceException {
    run(s3 -> s3.deleteBucketReplicationConfiguration(request));
  }

  @Override
  public boolean doesObjectExist(String bucketName, String objectName) throws AmazonServiceException, AmazonServiceException {
    return call(s3 -> s3.doesObjectExist(bucketName, objectName));
  }

  @Override
  public BucketAccelerateConfiguration getBucketAccelerateConfiguration(String bucketName) throws AmazonServiceException, AmazonServiceException {
    return call(s3 -> s3.getBucketAccelerateConfiguration(bucketName));
  }

  @Override
  public BucketAccelerateConfiguration getBucketAccelerateConfiguration(GetBucketAccelerateConfigurationRequest getBucketAccelerateConfigurationRequest) throws AmazonServiceException, AmazonServiceException {
    return call(s3 -> s3.getBucketAccelerateConfiguration(getBucketAccelerateConfigurationRequest));
  }

  @Override
  public void setBucketAccelerateConfiguration(String bucketName, BucketAccelerateConfiguration accelerateConfiguration) throws AmazonServiceException, AmazonServiceException {
    run(s3 -> s3.setBucketAccelerateConfiguration(bucketName, accelerateConfiguration));
  }

  @Override
  public void setBucketAccelerateConfiguration(SetBucketAccelerateConfigurationRequest setBucketAccelerateConfigurationRequest) throws AmazonServiceException, AmazonServiceException {
    run(s3 -> s3.setBucketAccelerateConfiguration(setBucketAccelerateConfigurationRequest));
  }

  @Override
  public DeleteBucketMetricsConfigurationResult deleteBucketMetricsConfiguration(String bucketName, String id) throws AmazonServiceException, AmazonServiceException {
    return call(s3 -> s3.deleteBucketMetricsConfiguration(bucketName, id));
  }

  @Override
  public DeleteBucketMetricsConfigurationResult deleteBucketMetricsConfiguration(DeleteBucketMetricsConfigurationRequest deleteBucketMetricsConfigurationRequest) throws AmazonServiceException, AmazonServiceException {
    return call(s3 -> s3.deleteBucketMetricsConfiguration(deleteBucketMetricsConfigurationRequest));
  }

  @Override
  public GetBucketMetricsConfigurationResult getBucketMetricsConfiguration(String bucketName, String id) throws AmazonServiceException, AmazonServiceException {
    return call(s3 -> s3.getBucketMetricsConfiguration(bucketName, id));
  }

  @Override
  public GetBucketMetricsConfigurationResult getBucketMetricsConfiguration(GetBucketMetricsConfigurationRequest getBucketMetricsConfigurationRequest) throws AmazonServiceException, AmazonServiceException {
    return call(s3 -> s3.getBucketMetricsConfiguration(getBucketMetricsConfigurationRequest));
  }

  @Override
  public SetBucketMetricsConfigurationResult setBucketMetricsConfiguration(String bucketName, MetricsConfiguration metricsConfiguration) throws AmazonServiceException, AmazonServiceException {
    return call(s3 -> s3.setBucketMetricsConfiguration(bucketName, metricsConfiguration));
  }

  @Override
  public SetBucketMetricsConfigurationResult setBucketMetricsConfiguration(SetBucketMetricsConfigurationRequest setBucketMetricsConfigurationRequest) throws AmazonServiceException, AmazonServiceException {
    return call(s3 -> s3.setBucketMetricsConfiguration(setBucketMetricsConfigurationRequest));
  }

  @Override
  public ListBucketMetricsConfigurationsResult listBucketMetricsConfigurations(ListBucketMetricsConfigurationsRequest listBucketMetricsConfigurationsRequest) throws AmazonServiceException, AmazonServiceException {
    return call(s3 -> s3.listBucketMetricsConfigurations(listBucketMetricsConfigurationsRequest));
  }

  @Override
  public DeleteBucketAnalyticsConfigurationResult deleteBucketAnalyticsConfiguration(String bucketName, String id) throws AmazonServiceException, AmazonServiceException {
    return call(s3 -> s3.deleteBucketAnalyticsConfiguration(bucketName, id));
  }

  @Override
  public DeleteBucketAnalyticsConfigurationResult deleteBucketAnalyticsConfiguration(DeleteBucketAnalyticsConfigurationRequest deleteBucketAnalyticsConfigurationRequest) throws AmazonServiceException, AmazonServiceException {
    return call(s3 -> s3.deleteBucketAnalyticsConfiguration(deleteBucketAnalyticsConfigurationRequest));
  }

  @Override
  public GetBucketAnalyticsConfigurationResult getBucketAnalyticsConfiguration(String bucketName, String id) throws AmazonServiceException, AmazonServiceException {
    return call(s3 -> s3.getBucketAnalyticsConfiguration(bucketName, id));
  }

  @Override
  public GetBucketAnalyticsConfigurationResult getBucketAnalyticsConfiguration(GetBucketAnalyticsConfigurationRequest getBucketAnalyticsConfigurationRequest) throws AmazonServiceException, AmazonServiceException {
    return call(s3 -> s3.getBucketAnalyticsConfiguration(getBucketAnalyticsConfigurationRequest));
  }

  @Override
  public SetBucketAnalyticsConfigurationResult setBucketAnalyticsConfiguration(String bucketName, AnalyticsConfiguration analyticsConfiguration) throws AmazonServiceException, AmazonServiceException {
    return call(s3 -> s3.setBucketAnalyticsConfiguration(bucketName, analyticsConfiguration));
  }

  @Override
  public SetBucketAnalyticsConfigurationResult setBucketAnalyticsConfiguration(SetBucketAnalyticsConfigurationRequest setBucketAnalyticsConfigurationRequest) throws AmazonServiceException, AmazonServiceException {
    return call(s3 -> s3.setBucketAnalyticsConfiguration(setBucketAnalyticsConfigurationRequest));
  }

  @Override
  public ListBucketAnalyticsConfigurationsResult listBucketAnalyticsConfigurations(ListBucketAnalyticsConfigurationsRequest listBucketAnalyticsConfigurationsRequest) throws AmazonServiceException, AmazonServiceException {
    return call(s3 -> s3.listBucketAnalyticsConfigurations(listBucketAnalyticsConfigurationsRequest));
  }

  @Override
  public DeleteBucketInventoryConfigurationResult deleteBucketInventoryConfiguration(String bucketName, String id) throws AmazonServiceException, AmazonServiceException {
    return call(s3 -> s3.deleteBucketInventoryConfiguration(bucketName, id));
  }

  @Override
  public DeleteBucketInventoryConfigurationResult deleteBucketInventoryConfiguration(DeleteBucketInventoryConfigurationRequest deleteBucketInventoryConfigurationRequest) throws AmazonServiceException, AmazonServiceException {
    return call(s3 -> s3.deleteBucketInventoryConfiguration(deleteBucketInventoryConfigurationRequest));
  }

  @Override
  public GetBucketInventoryConfigurationResult getBucketInventoryConfiguration(String bucketName, String id) throws AmazonServiceException, AmazonServiceException {
    return call(s3 -> s3.getBucketInventoryConfiguration(bucketName, id));
  }

  @Override
  public GetBucketInventoryConfigurationResult getBucketInventoryConfiguration(GetBucketInventoryConfigurationRequest getBucketInventoryConfigurationRequest) throws AmazonServiceException, AmazonServiceException {
    return call(s3 -> s3.getBucketInventoryConfiguration(getBucketInventoryConfigurationRequest));
  }

  @Override
  public SetBucketInventoryConfigurationResult setBucketInventoryConfiguration(String bucketName, InventoryConfiguration inventoryConfiguration) throws AmazonServiceException, AmazonServiceException {
    return call(s3 -> s3.setBucketInventoryConfiguration(bucketName, inventoryConfiguration));
  }

  @Override
  public SetBucketInventoryConfigurationResult setBucketInventoryConfiguration(SetBucketInventoryConfigurationRequest setBucketInventoryConfigurationRequest) throws AmazonServiceException, AmazonServiceException {
    return call(s3 -> s3.setBucketInventoryConfiguration(setBucketInventoryConfigurationRequest));
  }

  @Override
  public ListBucketInventoryConfigurationsResult listBucketInventoryConfigurations(ListBucketInventoryConfigurationsRequest listBucketInventoryConfigurationsRequest) throws AmazonServiceException, AmazonServiceException {
    return call(s3 -> s3.listBucketInventoryConfigurations(listBucketInventoryConfigurationsRequest));
  }

  @Override
  public com.amazonaws.services.s3.model.Region getRegion() {
    return call(AmazonS3::getRegion);
  }

  @Override
  public String getRegionName() {
    return call(AmazonS3::getRegionName);
  }

  @Override
  public URL getUrl(String bucketName, String key) {
    return call(s3 -> s3.getUrl(bucketName, key));
  }

  @Override
  public AmazonS3Waiters waiters() {
    return call(AmazonS3::waiters);
  }
}
