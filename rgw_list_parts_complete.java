import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.http.conn.params.ConnConnectionParamBean;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListPartsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.PartSummary;

public class sdk_list_parts_complete {
	public static String access_key = "access_key";
	public static String secret_key = "secret_key";
	public static String endpoint = "10.10.1.64";
	public static String bucketname = "buck001";
	public static String objectname = "object name";

	static AmazonS3 client;
	
	public static void main(String[] args) {
	}

	public static void listPartsXml(String obj_name, String uploadId) {
		List<PartETag> parts = new ArrayList<PartETag>();
		BufferedInputStream instream = null;
		BufferedOutputStream outstream = null;
		try {
			URL url = new URL("http://" + endpoint + "/" + bucketname
					+ "/" + obj_name + "?uploadId=" + uploadId);
			HttpURLConnection http_conn = (HttpURLConnection) url
					.openConnection();
			http_conn.setDoOutput(true);
			http_conn.setRequestMethod("GET");

			System.out.println("http_status : "
					+ http_conn.getResponseCode());

			System.out.println("http_headers: "
					+ http_conn.getHeaderFields());

			
//			 System.out.println("---- body start ----");
//			 BufferedReader reader = new BufferedReader(new InputStreamReader(http_conn.getInputStream()));
//			 String readLine;
//			 while ((readLine = reader.read()) != null) {
//			 System.out.println(readLine);
//			 }
//			 reader.close();
//			 System.out.println("---- body end ----");

			DocumentBuilderFactory factory = DocumentBuilderFactory
					.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			Document document = builder.parse(http_conn.getInputStream());
			NodeList list = null;
			list = document
					.getElementsByTagName("ListMultipartUploadResult");
			if (null == list) {
				System.out.println("ERR no root element!");
				return;
			} else {
				NodeList partList = document.getElementsByTagName("Part");
				if (null == partList) {
					System.out.println("ERR no initList element!");
					return;
				} else {
					for (int i = 0; i < partList.getLength(); i++) {
						Node item = partList.item(i);
//						System.out.println("Name : " + item.getNodeName()
//								+ " Value: " + item.getNodeValue());

						NodeList chiList = item.getChildNodes();
						if (null != chiList) {
							for (int j = 0; j < chiList.getLength(); j++) {
								Node childitem = chiList.item(j);
								if (childitem.getNodeType() == Node.ELEMENT_NODE) {
//									System.out.println("   Name : "
//											+ childitem.getNodeName()
//											+ " Value: "
//											+ childitem.getFirstChild()
//													.getNodeValue());	
									
									if("PartNumber".equals(childitem.getNodeName()))
									{
										System.out.println(childitem.getFirstChild().getNodeValue());
									}
									
									if("ETag".equals(childitem.getNodeName()))
									{
										System.out.println(childitem.getFirstChild().getNodeValue());
									}
								}
							}
						}
					}
				}
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void tee(String[] args) {
		AWSCredentials credentials = new BasicAWSCredentials(
				access_key, secret_key);
		ClientConfiguration clientconfig = new ClientConfiguration();
		clientconfig.setProtocol(Protocol.HTTP);

		S3ClientOptions client_options = new S3ClientOptions();
		client_options.setPathStyleAccess(true);

		client = new AmazonS3Client(credentials, clientconfig);
		client.setEndpoint(endpoint);
		client.setS3ClientOptions(client_options);
        
		String  objname = "test.tar.gz";
		String uploadId = "2/ygyX6OqdFb9JRhhZa5X5sjHiWZQS4O5";

		try {
			
			
			ListMultipartUploadsRequest upload_req = new ListMultipartUploadsRequest(bucketname);
			MultipartUploadListing list_upload = client.listMultipartUploads(upload_req);
			
			for(MultipartUpload itemp : list_upload.getMultipartUploads())
			{
				System.out.println(itemp.getKey()+ " " + itemp.getUploadId());
				ListPartsRequest list_req = new ListPartsRequest(bucketname,
						itemp.getKey(), itemp.getUploadId());
				PartListing list_result = client.listParts(list_req);
				System.out.println(list_result.getBucketName());
				System.out.println(list_result.getParts());
				List<PartETag> part_etags = new ArrayList<PartETag>();
				for (PartSummary item : list_result.getParts()) {
					System.out.println(item.getPartNumber() + " -> "
							+ item.getETag());
					PartETag e_item = new PartETag(item.getPartNumber(), item.getETag());
					part_etags.add(e_item);
				}
				
			}
			/*
			// com.amazonaws.services.s3.model.ListPartsRequest.ListPartsRequest(String
			// bucketName, String key, String uploadId)
//			ListPartsRequest list_req = new ListPartsRequest(bucketname,
//					objname, uploadId);
//			PartListing list_result = client.listParts(list_req);
//			System.out.println(list_result.getBucketName());
//			System.out.println(list_result.getParts());
//			List<PartETag> part_etags = new ArrayList<PartETag>();
//			for (PartSummary item : list_result.getParts()) {
//				System.out.println(item.getPartNumber() + " -> "
//						+ item.getETag());
//				PartETag e_item = new PartETag(item.getPartNumber(), item.getETag());
//				part_etags.add(e_item);
//			}

			// com.amazonaws.services.s3.model.CompleteMultipartUploadRequest.CompleteMultipartUploadRequest(String
			// bucketName, String key, String uploadId, List<PartETag>
			// partETags)
			CompleteMultipartUploadRequest comp_req = new CompleteMultipartUploadRequest(
					bucketname, objname, uploadId, null);
//			CompleteMultipartUploadResult comp_result = client
//					.completeMultipartUpload(comp_req);
			//System.out.println(comp_result.getETag());
			//System.out.println(comp_result.getKey());
			 * 
			 */
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
}
