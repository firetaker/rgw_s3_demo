import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.HttpMethod;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;

public class genurl_and_upload {
	public static String access_key = "access_key";
	public static String secret_key = "secret_key";
	public static String endpoint = "10.10.1.64";
	public static String bucketname = "buck001";
	public static String objectname = "object name";

	static AmazonS3 client;
	
	public static void main(String[] args) throws IOException {
		
		AWSCredentials credentials = new BasicAWSCredentials(
				access_key, secret_key);
		ClientConfiguration clientconfig = new ClientConfiguration();
		clientconfig.setProtocol(Protocol.HTTP);

		S3ClientOptions client_options = new S3ClientOptions();
		client_options.setPathStyleAccess(true);
		
		client = new AmazonS3Client(credentials, clientconfig);
		client.setEndpoint(endpoint);
		client.setS3ClientOptions(client_options);
		
		URL url_str = generate_url("d.jpg");
		upload_by_url(url_str,"f:\\d.jpg");
	}
	public static URL generate_url(String objname) {
		
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
			return url;			
		} catch (AmazonServiceException ase) {
			System.out.println("svr_error_message:" + ase.getMessage());
			System.out.println("svr_status_code:  " + ase.getStatusCode());
			System.out.println("svr_error_code:   " + ase.getErrorCode());
			System.out.println("svr_error_type:   " + ase.getErrorType());
			System.out.println("svr_request_id:   " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("clt_error_message:" + ace.getMessage());
		}
		
		return null;
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
