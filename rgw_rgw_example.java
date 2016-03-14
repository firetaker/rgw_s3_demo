import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.HttpMethod;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListPartsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.PartSummary;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import com.amazonaws.services.s3.transfer.TransferProgress;
import com.amazonaws.services.s3.transfer.Upload;

public class wskj_java_example {

	public static String access_key = "access_key";
	public static String secret_key = "secret_key";
	public static String endpoint = "10.10.1.64";
	public static String bucketname = "buck001";
	public static String objectname = "object name";

	static AmazonS3 client;

	public static void main(String[] args) {
		AWSCredentials credentials = new BasicAWSCredentials(
				access_key, secret_key);
		ClientConfiguration clientconfig = new ClientConfiguration();
		clientconfig.setProtocol(Protocol.HTTP);
		clientconfig.setConnectionTimeout(300);//unit: s 秒
		clientconfig.setMaxErrorRetry(3);//推荐为3次
		//RetryPolicy retryp = new RetryPolicy(retryCondition, backoffStrategy, maxErrorRetry, honorMaxErrorRetryInClientConfig);
		//clientconfig.setRetryPolicy(retryp);
		
		S3ClientOptions client_options = new S3ClientOptions();
		client_options.setPathStyleAccess(true);
		
		client = new AmazonS3Client(credentials, clientconfig);
		client.setEndpoint(endpoint);
		client.setS3ClientOptions(client_options);

		// list_buckets();
		list_objs(bucketname,"");
		// gettobj_info(bucketname,"obj");
		// putobj_withmeta(bucketname,"local path/file");
		// setobj_acl(bucketname,objectname);
		// getobj_acl(bucketname,objectname);
		// generate_url(bucketname,"xxxxx");
		// get_obj(bucketname,objectname);
		// copy_obj(bucketname,objectname,objectname);
		// list_multipart(bucketname);
		//multipart_upload(bucketname, "new", "local path/file");
		 //multi_upload(bucketname,"multi","local path/file");
	}

	public static void list_buckets() {
		try {
			List<Bucket> buckets = client.listBuckets();
			if (!buckets.isEmpty()) {
				for (Bucket bucket : buckets) {
					System.out.println("bucket_name: " + bucket.getName()
							+ " bucket_createdate: "
							+ bucket.getCreationDate());
				}
			}
		} catch (AmazonServiceException ase) {
			System.out.println("svr_error_message:" + ase.getMessage());
			System.out.println("svr_status_code:  " + ase.getStatusCode());
			System.out.println("svr_error_code:   " + ase.getErrorCode());
			System.out.println("svr_error_type:   " + ase.getErrorType());
			System.out.println("svr_request_id:   " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("clt_error_message:" + ace.getMessage());
		}
	}

	public static void list_objs(String bucketname, String prefix) {
		try {
			ListObjectsRequest req = new ListObjectsRequest()
					.withBucketName(bucketname);
			if (!prefix.isEmpty()) {
				req.setPrefix(prefix);
			}

			ObjectListing list;
			do {
				list = client.listObjects(req);
				for (S3ObjectSummary x3_objsum : list.getObjectSummaries()) {
					System.out.println(" -- " + x3_objsum.getKey() + "  "
							+ "(size = " + x3_objsum.getSize() + " ,ETag = "
							+ x3_objsum.getETag() + ")");
				}
				req.setMarker(list.getNextMarker());
			} while (list.isTruncated());

		} catch (AmazonServiceException ase) {
			System.out.println("svr_error_message:" + ase.getMessage());
			System.out.println("svr_status_code:  " + ase.getStatusCode());
			System.out.println("svr_error_code:   " + ase.getErrorCode());
			System.out.println("svr_error_type:   " + ase.getErrorType());
			System.out.println("svr_request_id:   " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("clt_error_message:" + ace.getMessage());
		}
	}

	public static void putobj_simple(String bucketname, String file_path) {
		try {
			File file = new File(file_path);
			PutObjectResult putobj_result = client
					.putObject(new PutObjectRequest(bucketname, file
							.getName(), file));
			System.out
					.println("putobj_etag:" + putobj_result.getETag());
		} catch (AmazonServiceException ase) {
			System.out.println("svr_error_message:" + ase.getMessage());
			System.out.println("svr_status_code:  " + ase.getStatusCode());
			System.out.println("svr_error_code:   " + ase.getErrorCode());
			System.out.println("svr_error_type:   " + ase.getErrorType());
			System.out.println("svr_request_id:   " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("clt_error_message:" + ace.getMessage());
		}
	}

	public static void putobj_withmeta(String bucketname,
			String file_path) {
		try {
			ObjectMetadata meta = new ObjectMetadata();
			meta.addUserMetadata("metaname", "metavalue");
			meta.setContentType("image/jpeg");
			meta.setHeader("x-amz-acl", "public-read");
			meta.setHeader("x-amz-meta-your-key-here", "your-private-value");

			File file = new File(file_path);

			PutObjectRequest x3_req = new PutObjectRequest(bucketname,
					file.getName(), file);
			x3_req.setMetadata(meta);
			x3_req.setCannedAcl(CannedAccessControlList.PublicRead);
			
			PutObjectResult putobj_result = client.putObject(x3_req);
			System.out
					.println("putobj_etag:" + putobj_result.getETag());
		} catch (AmazonServiceException ase) {
			System.out.println("svr_error_message:" + ase.getMessage());
			System.out.println("svr_status_code:  " + ase.getStatusCode());
			System.out.println("svr_error_code:   " + ase.getErrorCode());
			System.out.println("svr_error_type:   " + ase.getErrorType());
			System.out.println("svr_request_id:   " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("clt_error_message:" + ace.getMessage());
		}
	}

	public static void get_obj(String bucketname, String objname) {
		BufferedInputStream in_stream = null;
		BufferedOutputStream out_stream = null;
		try {
			S3Object object = client.getObject(new GetObjectRequest(
					bucketname, objname));
			System.out.println("content-type :"
					+ object.getObjectMetadata().getContentType());
			System.out.println("etag : "
					+ object.getObjectMetadata().getETag());
			System.out.println("content_length:"
					+ object.getObjectMetadata().getContentLength());

			in_stream = new BufferedInputStream(object.getObjectContent());
			File local_file = new File("d:/" + objname);
			if (!local_file.getParentFile().exists()) {
				local_file.getParentFile().delete();
			}

			try {
				out_stream = new BufferedOutputStream(new FileOutputStream(
						local_file, false));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}

			byte[] buffer = new byte[1024];
			int offset = 0;
			try {
				while ((offset = in_stream.read(buffer)) != -1) {
					out_stream.write(buffer, 0, offset);
					out_stream.flush();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}

		} catch (AmazonServiceException ase) {
			System.out.println("svr_error_message:" + ase.getMessage());
			System.out.println("svr_status_code:  " + ase.getStatusCode());
			System.out.println("svr_error_code:   " + ase.getErrorCode());
			System.out.println("svr_error_type:   " + ase.getErrorType());
			System.out.println("svr_request_id:   " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("clt_error_message:" + ace.getMessage());
		} finally {
			if (null != in_stream)
				try {
					in_stream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			if (null != out_stream)
				try {
					out_stream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}
	}

	public static void gettobj_meta(String bucketname,
			String objname) {

		try {
			ObjectMetadata meta = client
					.getObjectMetadata(new GetObjectMetadataRequest(
							bucketname, objname));
			System.out.println("content_length:"
					+ meta.getContentLength());
			System.out.println("xs_content_type:   "
					+ meta.getContentType());
			System.out.println("xs_etag :          " + meta.getETag());

		} catch (AmazonServiceException ase) {
			System.out.println("svr_error_message:" + ase.getMessage());
			System.out.println("svr_status_code:  " + ase.getStatusCode());
			System.out.println("svr_error_code:   " + ase.getErrorCode());
			System.out.println("svr_error_type:   " + ase.getErrorType());
			System.out.println("svr_request_id:   " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("clt_error_message:" + ace.getMessage());
		}
	}

	public static void delete_obj(String bucketname, String objname) {
		try {
			client.deleteObject(bucketname, objname);
		} catch (AmazonServiceException ase) {
			System.out.println("svr_error_message:" + ase.getMessage());
			System.out.println("svr_status_code:  " + ase.getStatusCode());
			System.out.println("svr_error_code:   " + ase.getErrorCode());
			System.out.println("svr_error_type:   " + ase.getErrorType());
			System.out.println("svr_request_id:   " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("clt_error_message:" + ace.getMessage());
		}
	}

	public static void setobj_acl(String bucketname, String objname) {
		try {
			client.setObjectAcl(bucketname, objname,
					CannedAccessControlList.PublicRead);
		} catch (AmazonServiceException ase) {
			System.out.println("svr_error_message:" + ase.getMessage());
			System.out.println("svr_status_code:  " + ase.getStatusCode());
			System.out.println("svr_error_code:   " + ase.getErrorCode());
			System.out.println("svr_error_type:   " + ase.getErrorType());
			System.out.println("svr_request_id:   " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("clt_error_message:" + ace.getMessage());
		}
	}

	public static void getobj_acl(String bucketname, String objname) {
		try {
			AccessControlList obj_acl = client.getObjectAcl(
					bucketname, objname);
			for (Grant grant : obj_acl.getGrants()) {
				System.out.println(grant.getGrantee().getIdentifier());
			}
		} catch (AmazonServiceException ase) {
			System.out.println("svr_error_message:" + ase.getMessage());
			System.out.println("svr_status_code:  " + ase.getStatusCode());
			System.out.println("svr_error_code:   " + ase.getErrorCode());
			System.out.println("svr_error_type:   " + ase.getErrorType());
			System.out.println("svr_request_id:   " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("clt_error_message:" + ace.getMessage());
		}
	}

	public static void multipart_upload(String bucketname,
			String objname, String file_path) {

		InitiateMultipartUploadRequest multi_req = new InitiateMultipartUploadRequest(
				bucketname, objname);
		InitiateMultipartUploadResult multi_res = client
				.initiateMultipartUpload(multi_req);

		String multi_uploadid = multi_res.getUploadId();

		final int part_size = 1024 * 1024 * 5;
		File local_file = new File(file_path);

		int part_count = (int) Math.ceil((double) (local_file.length())
				/ (double) part_size);

		List<PartETag> part_etags = new java.util.ArrayList<PartETag>();
		try {
			for (int part_no = 0; part_no < part_count; part_no++) {
				FileInputStream input = new FileInputStream(local_file);

				long offset_bytes = part_size * part_no;
				input.skip(offset_bytes);

				long part_size = part_size < (local_file.length() - offset_bytes) ? part_size
						: (local_file.length() - offset_bytes);

				UploadPartRequest upload_req = new UploadPartRequest();
				upload_req.setBucketName(bucketname);
				upload_req.setKey(objname);
				upload_req.setUploadId(multi_uploadid);
				upload_req.setInputStream(input);
				upload_req.setPartSize(part_size);
				upload_req.setPartNumber(part_no + 1);
				UploadPartResult upload_res = client
						.uploadPart(upload_req);

				part_etags.add(upload_res.getPartETag());
				input.close();
				System.out.println(" -- part_id" + part_no + " Etag: "
						+ upload_res.getPartETag().getETag());

			}
	
			PartListing parts = client.listParts(new ListPartsRequest(bucketname, objname, multi_uploadid));
			for (PartSummary part : parts.getParts()) {
				System.out.println("PartNumber: " + part.getPartNumber()
						+ " ETag: " + part.getETag());
			}

			CompleteMultipartUploadRequest complete_req = new CompleteMultipartUploadRequest(
					bucketname, objname, multi_res.getUploadId(),
					part_etags);

			CompleteMultipartUploadResult complete_res = client
					.completeMultipartUpload(complete_req);
			System.out.println(complete_res.getETag());
		} catch (Exception ie) {
			System.err.println(ie.getMessage());
			ie.printStackTrace();
			client.abortMultipartUpload(new AbortMultipartUploadRequest(
					bucketname, objname, multi_uploadid));

		}
	}

	public static void list_multipart(String bucketname) {
		try {
			ListMultipartUploadsRequest list_req = new ListMultipartUploadsRequest(
					bucketname);
			MultipartUploadListing list_res = client
					.listMultipartUploads(list_req);

			for (MultipartUpload multipartUpload : list_res
					.getMultipartUploads()) {
				System.out.println("Key: " + multipartUpload.getKey()
						+ " UploadId: " + multipartUpload.getUploadId());
				AbortMultipartUploadRequest abort_req = new AbortMultipartUploadRequest(
						bucketname, multipartUpload.getKey(),
						multipartUpload.getUploadId());
				client.abortMultipartUpload(abort_req);
			}
		} catch (AmazonServiceException ase) {
			System.out.println("svr_error_message:" + ase.getMessage());
			System.out.println("svr_status_code:  " + ase.getStatusCode());
			System.out.println("svr_error_code:   " + ase.getErrorCode());
			System.out.println("svr_error_type:   " + ase.getErrorType());
			System.out.println("svr_request_id:   " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("clt_error_message:" + ace.getMessage());
		}
	}

	public static void multi_upload(String bucketname,
			String objname, String file_path) {
		int threshold = 5 * 1024 * 1024;
		TransferManager tm = new TransferManager(client);
		TransferManagerConfiguration conf = tm.getConfiguration();
		conf.setMultipartUploadThreshold(threshold);
		tm.setConfiguration(conf);

		Upload upload = tm.upload(bucketname, objname, new File(
				file_path));
		try {
			// upload.waitForCompletion();
			// UploadResult res = upload.waitForUploadResult();
			TransferProgress progress = upload.getProgress();
			while (false == upload.isDone()) {
				int percent = (int) (progress.getPercentTransferred());
				System.out.print("\r" + "[ " + percent + "% ] "
						+ progress.getBytesTransferred() + " / "
						+ progress.getTotalBytesToTransfer());
				Thread.sleep(500);
			}
			System.out.println("\ndone");
			if (upload.isDone())
				return;

		} catch (AmazonServiceException ase) {
			System.out.println("svr_error_message:" + ase.getMessage());
			System.out.println("svr_status_code:  " + ase.getStatusCode());
			System.out.println("svr_error_code:   " + ase.getErrorCode());
			System.out.println("svr_error_type:   " + ase.getErrorType());
			System.out.println("svr_request_id:   " + ase.getRequestId());
		} catch (InterruptedException ie) {
			System.out.println("ie_error_message:" + ie.getMessage());
			ie.printStackTrace();
		} catch (AmazonClientException ace) {
			System.out.println("clt_error_message:" + ace.getMessage());
			ace.printStackTrace();
		}
	}

	public static void copy_obj(String bucketname, String objname,
			String new_objname) {
		try {
			CopyObjectRequest req = new CopyObjectRequest(bucketname,
					objname, bucketname, new_objname);
			ObjectMetadata meta = new ObjectMetadata();
			meta.addUserMetadata("usermetakey", "usermetavale");
			req.setNewObjectMetadata(meta);
			req.setCannedAccessControlList(CannedAccessControlList.PublicRead);

			CopyObjectResult res = client.copyObject(req);
			System.out.println(res.getETag());
		} catch (AmazonServiceException ase) {
			System.out.println("svr_error_message:" + ase.getMessage());
			System.out.println("svr_status_code:  " + ase.getStatusCode());
			System.out.println("svr_error_code:   " + ase.getErrorCode());
			System.out.println("svr_error_type:   " + ase.getErrorType());
			System.out.println("svr_request_id:   " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("clt_error_message:" + ace.getMessage());
		}
	}

	public static void generate_url(String bucketname,
			String objname) {
		try {
			java.util.Date expiration = new java.util.Date();

			long milliSeconds = expiration.getTime();
			milliSeconds += 1000 * 60 * 5; 
			expiration.setTime(milliSeconds);

			GeneratePresignedUrlRequest genurl_req = new GeneratePresignedUrlRequest(
					bucketname, objname);
			genurl_req.setMethod(HttpMethod.PUT);
			genurl_req.setExpiration(expiration);
			genurl_req.setContentType("image/jpeg");
			genurl_req.addRequestParameter("x-amz-acl", "public-read");

			URL url = client.generatePresignedUrl(genurl_req);
			System.out.println(url.toString());

			try {
				upload_by_url(url, "local path/file");
			} catch (IOException e) {
				e.printStackTrace();
			}

			System.out.println(url.toString());
		} catch (AmazonServiceException ase) {
			System.out.println("svr_error_message:" + ase.getMessage());
			System.out.println("svr_status_code:  " + ase.getStatusCode());
			System.out.println("svr_error_code:   " + ase.getErrorCode());
			System.out.println("svr_error_type:   " + ase.getErrorType());
			System.out.println("svr_request_id:   " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("clt_error_message:" + ace.getMessage());
		}
	}

	public static void upload_by_url(URL url, String file_path)
			throws IOException {

		BufferedInputStream instream = null;
		BufferedOutputStream outstream = null;
		try {
			HttpURLConnection http_conn = (HttpURLConnection) url
					.openConnection();
			http_conn.setDoOutput(true);
			http_conn.setRequestMethod("PUT");

			http_conn.setRequestProperty("Content-Type", "image/jpeg");
			http_conn.setRequestProperty("x-amz-acl", "public-read");

			outstream = new BufferedOutputStream(
					http_conn.getOutputStream());
			instream = new BufferedInputStream(new FileInputStream(
					new File(file_path)));

			byte[] buffer = new byte[1024];
			int offset = 0;
			while ((offset = instream.read(buffer)) != -1) {
				outstream.write(buffer, 0, offset);
				outstream.flush();
			}

			System.out.println("http_status : "
					+ http_conn.getResponseCode());
			System.out.println("http_headers: "
					+ http_conn.getHeaderFields());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (null != outstream)
				outstream.close();
			if (null != instream)
				instream.close();
		}
	}

}
