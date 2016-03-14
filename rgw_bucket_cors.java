import java.util.Arrays;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.BucketCrossOriginConfiguration;
import com.amazonaws.services.s3.model.CORSRule;

public class sdk_bucket_cors {
	public static String access_key = "access_key";
	public static String secret_key = "secret_key";
	public static String endpoint = "10.10.1.64";
	public static String bucketname = "buck001";
	public static String objectname = "object name";

	static AmazonS3 client;
	
	public static void print_cors(
			BucketCrossOriginConfiguration configuration) {

		if (configuration == null) {
			System.out.println("\nConfiguration is null.");
			return;
		}

		System.out.format("\nConfiguration has %s rules:\n", configuration
				.getRules().size());
		for (CORSRule rule : configuration.getRules()) {
			System.out.format("Rule ID: %s\n", rule.getId());
			System.out.format("MaxAgeSeconds: %s\n", rule.getMaxAgeSeconds());
			for (com.amazonaws.services.s3.model.CORSRule.AllowedMethods item : rule
					.getAllowedMethods()) {
				System.out.format("AllowedMethod: %s\n", item.name());
			}
			System.out.format("AllowedOrigins: %s\n", rule.getAllowedOrigins());
			System.out.format("AllowedHeaders: %s\n", rule.getAllowedHeaders());
			System.out.format("ExposeHeader: %s\n", rule.getExposedHeaders());
		}
	}
	
	public static void main(String[] args) {
		AWSCredentials credentials = new BasicAWSCredentials(
				access_key, secret_key);
		ClientConfiguration clientconfig = new ClientConfiguration();
		clientconfig.setProtocol(Protocol.HTTP);

		S3ClientOptions client_options = new S3ClientOptions();
		client_options.setPathStyleAccess(true);
		
		client = new AmazonS3Client(credentials, clientconfig);
		client.setEndpoint(endpoint);
		client.setS3ClientOptions(client_options);
		
		try {
			BucketCrossOriginConfiguration cors_config = new BucketCrossOriginConfiguration();
			
			CORSRule rule1 = new CORSRule()
			.withId("CORSRule1")
			.withExposedHeaders(Arrays.asList("*"))
			.withAllowedHeaders(Arrays.asList("*"))
			.withAllowedMethods(
					Arrays.asList(new CORSRule.AllowedMethods[] {
							CORSRule.AllowedMethods.GET,
							CORSRule.AllowedMethods.DELETE,
							CORSRule.AllowedMethods.HEAD,
							CORSRule.AllowedMethods.POST,
							CORSRule.AllowedMethods.PUT}))
			.withAllowedOrigins(
					Arrays.asList(new String[] { "http://test.com:8080" }));
			
			CORSRule rule2 = new CORSRule()
			.withId("CORSRule2")
			.withExposedHeaders(Arrays.asList("*"))
			.withAllowedHeaders(Arrays.asList("*"))
			.withAllowedMethods(
					Arrays.asList(new CORSRule.AllowedMethods[] {
							CORSRule.AllowedMethods.GET,
							CORSRule.AllowedMethods.DELETE,
							CORSRule.AllowedMethods.HEAD,
							CORSRule.AllowedMethods.POST,
							CORSRule.AllowedMethods.PUT}))
			.withAllowedOrigins(
					Arrays.asList(new String[] { "http://abc.com" }));
			
			cors_config.setRules(Arrays.asList(new CORSRule[] { rule1, rule2}));
			
			client.setBucketCrossOriginConfiguration(bucketname, cors_config);
			BucketCrossOriginConfiguration configuration = client.getBucketCrossOriginConfiguration(bucketname);
			print_cors(configuration);
			
			// client.deleteBucketCrossOriginConfiguration(bucketname);
		}  catch (AmazonServiceException ase) {
			System.out.println("svr_error_message:" + ase.getMessage());
			System.out.println("svr_status_code:  " + ase.getStatusCode());
			System.out.println("svr_error_code:   " + ase.getErrorCode());
			System.out.println("svr_error_type:   " + ase.getErrorType());
			System.out.println("svr_request_id:   " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("clt_error_message:" + ace.getMessage());
		}
	}
}
