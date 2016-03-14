import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.httpclient.util.DateUtil;
import org.apache.commons.lang.StringUtils;


public class Rest_multi_java {
	
	static String accessKey = "accessKey";
	static String secretKey = "secretKey";
	static String bucket = "testbuck";

	public static void main(String[] args) throws Exception {
//		File localFile = new File("D:/test.tar.gz");
//		initmultiupload(localFile);
		listMultiUploads();
	}
    

	public static void initmultiupload(File localFile) throws Exception {
		HttpURLConnection conn = null;
		BufferedInputStream in = null;
		String uploadId = null;
		try {			
			// 1. init upload 
			URL url = new URL("http://your-end.com/" + bucket + "/"
					+ localFile.getName() + "?uploads");
			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("POST");
			conn.setDoOutput(true);
			conn.setDoInput(true);

			String contentType = "application/octet-stream";
			Date date = new Date();
			String dateString = DateUtil.formatDate(date,
					DateUtil.PATTERN_RFC1036);
			String sign = sign("POST", "", contentType, dateString, "/"
					+ bucket + "/" + localFile.getName() + "?uploads", null);

			conn.setRequestProperty("Date", dateString);
			conn.setRequestProperty("Authorization", sign);
			conn.setRequestProperty("Content-Type", contentType);

			System.out.println("http status: " + conn.getResponseCode());
			System.out.println("after:\n" + conn.getHeaderFields());

			DocumentBuilderFactory factory = DocumentBuilderFactory
					.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			Document document = builder.parse(conn.getInputStream());
            
			NodeList list = document
					.getElementsByTagName("InitiateMultipartUploadResult");
			if (null == list) {
				System.out.println("ERR no root element!");
				return;
			} else {
				for (int i = 0; i < list.getLength(); i++) {
					Node item = list.item(i);
					System.out.println("Name : " + item.getNodeName()
							+ " Value: " + item.getNodeValue());

					NodeList chiList = item.getChildNodes();
					if (null != chiList) {
						for (int j = 0; j < chiList.getLength(); j++) {
							Node childitem = chiList.item(j);
							if (childitem.getNodeType() == Node.ELEMENT_NODE) {
								System.out.println("   Name : "
										+ childitem.getNodeName()
										+ " Value: "
										+ childitem.getFirstChild()
												.getNodeValue());
								if ("Bucket".equals(childitem.getNodeName())) {
									if (!bucket.equals(childitem.getFirstChild()
											.getNodeValue())) {
                                       System.out.println("Bucket name err!");
                                       return;
									}
								}
								if ("Key".equals(childitem.getNodeName())) {
									if (!localFile.getName().equals(childitem.getFirstChild()
											.getNodeValue())) {
										System.out.println("Key name err!");
	                                       return;
									}
								}
								if ("UploadId".equals(childitem.getNodeName())) {
									uploadId = childitem.getFirstChild()
											.getNodeValue();
								}
							}

						}
					}
				}
			}
			
			// 2. upload parts 
			Map<Integer,String> etagMap = new HashMap<Integer,String>();
			if(0 != uploadId.length())
			{
			   long filetotallen = localFile.length();
			   int off = 0;
			   int len = 0;
			   int chunkSize = 5242880;
			   int chunk_amount = (int) Math.ceil((float)localFile.length()/chunkSize);
			   int remain = 0;
			   
			   System.out.println("FileSize " + filetotallen + " chunks "+ chunk_amount);
			   for(int partnum = 1; partnum <= chunk_amount; partnum++)
			   {
				   off = (partnum - 1) * chunkSize;
				   remain = (int) (filetotallen - off);
				   len = Math.min(chunkSize, remain);
				   System.out.println(" ---------- Part " + partnum + " off " + off + " len " + len);
				   String Etag = uploadpart(localFile,uploadId,partnum,off,len);
				   if(null != Etag){
					   etagMap.put(partnum, Etag);
				   }
			   }
			}
			
			// 3. list parts and check results is correct
			System.out.println("///////////////////////////////////////////");
			listParts(localFile.getName(),uploadId);
			System.out.println("\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\");
			
			// 4. complete upload 
			completeUpload(localFile.getName(),uploadId,etagMap);
			
		} catch (Exception e) {
			
			if(null != uploadId)
			{
				abortupload(localFile.getName(),uploadId);
			}
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			close(in);
			conn.disconnect();
		}
	}

	public static void listParts(String objName, String uploadId) throws Exception {
		HttpURLConnection conn = null;
		BufferedInputStream in = null;
		BufferedOutputStream out = null;
		try {
			URL url = new URL("http://your-end.com/" + bucket + "/" + objName
					+ "?uploadId=" + uploadId);
			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setDoOutput(true);
			conn.setDoInput(true);

			String dateString = DateUtil.formatDate(new Date(),
					DateUtil.PATTERN_RFC1036);
			String sign = sign("GET", "", "", dateString, "/" + bucket + "/"
					+ objName + "?uploadId=" + uploadId, null);

			conn.setRequestProperty("Date", dateString);
			conn.setRequestProperty("Authorization", sign);

			System.out.println("http status: " + conn.getResponseCode());
			System.out.println("Headers:\n" + conn.getHeaderFields());


			DocumentBuilderFactory factory = DocumentBuilderFactory
					.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			Document document = builder.parse(conn.getInputStream());
            
			NodeList list = null;
			list = document
					.getElementsByTagName("ListMultipartUploadResult");
			if (null == list) {
				System.out.println("ERR no root element!");
				return;
			} else {
				for (int i = 0; i < list.getLength(); i++) {
					Node item = list.item(i);
					System.out.println(" - Name : " + item.getNodeName()
							+ " Value: " + item.getNodeValue() 
							+ " NodeType: " + item.getNodeType());

					NodeList chiList = item.getChildNodes();
					if (null != chiList) {
						for (int j = 0; j < chiList.getLength(); j++) {
							Node childitem = chiList.item(j);
							
							if (childitem.getNodeType() == Node.ELEMENT_NODE) {
								System.out.println("  -- Name : "
										+ childitem.getNodeName()
										+ " Value: "
										+ childitem.getFirstChild()
												.getNodeValue());						
								NodeList thrChildList = childitem.getChildNodes();
				                if (null != thrChildList) {
				                for (int t = 0; t < thrChildList.getLength(); t++) {
				                    Node thrChilditem = thrChildList.item(t);
				                    if (thrChilditem.getNodeType() == Node.ELEMENT_NODE)
				                    System.out.println("      ---  Name : "
				                        + thrChilditem.getNodeName()
				                        + " Value: "
				                        + thrChilditem.getFirstChild()
				                            .getNodeValue());
				                }
				                }
							}
						}
					}
				}
			}
			
			NodeList initList = document.getElementsByTagName("Owner");
			if (null == initList) {
				System.out.println("ERR no initList element!");
				return;
			} else {
				for (int i = 0; i < initList.getLength(); i++) {
					Node item = initList.item(i);
					System.out.println("Name : " + item.getNodeName()
							+ " Value: " + item.getNodeValue());

					NodeList chiList = item.getChildNodes();
					if (null != chiList) {
						for (int j = 0; j < chiList.getLength(); j++) {
							Node childitem = chiList.item(j);
							if (childitem.getNodeType() == Node.ELEMENT_NODE) {
								System.out.println("   Name : "
										+ childitem.getNodeName()
										+ " Value: "
										+ childitem.getFirstChild()
												.getNodeValue());						
							}
						}
					}
				}
			}
			
			NodeList partList = document.getElementsByTagName("Part");
			if (null == partList) {
				System.out.println("ERR no initList element!");
				return;
			} else {
				for (int i = 0; i < partList.getLength(); i++) {
					Node item = partList.item(i);
					System.out.println("Name : " + item.getNodeName()
							+ " Value: " + item.getNodeValue());

					NodeList chiList = item.getChildNodes();
					if (null != chiList) {
						for (int j = 0; j < chiList.getLength(); j++) {
							Node childitem = chiList.item(j);
							if (childitem.getNodeType() == Node.ELEMENT_NODE) {
								System.out.println("   Name : "
										+ childitem.getNodeName()
										+ " Value: "
										+ childitem.getFirstChild()
												.getNodeValue());						
							}
						}
					}
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			close(in);
			close(out);
			conn.disconnect();
		}
	}
	

	public static void completeUpload(String objName, String uploadId, Map<Integer,String> parts) throws Exception {
		HttpURLConnection conn = null;
		try {
			URL url = new URL("http://your-end.com/" + bucket + "/" + objName
					+ "?uploadId=" + uploadId);
			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("POST");
			conn.setDoOutput(true);
			conn.setDoInput(true);

			String dateString = DateUtil.formatDate(new Date(),
					DateUtil.PATTERN_RFC1036);
			String sign = sign("POST", "", "text/xml", dateString, "/" + bucket + "/"
					+ objName + "?uploadId=" + uploadId, null);

			conn.setRequestProperty("Date", dateString);
			conn.setRequestProperty("Authorization", sign);
			conn.setRequestProperty("Content-Type", "text/xml");  
			
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = null;
			try {
				builder = factory.newDocumentBuilder();
			} catch (ParserConfigurationException e) {
				e.printStackTrace();
			}

			Document document = builder.newDocument();
			Element root = document.createElement("CompleteMultipartUpload");
	        document.appendChild(root);	        
	        for (Map.Entry<Integer, String> item : parts.entrySet()) {
				System.out.println("Key " + item.getKey() + " value "
						+ item.getValue());
				Element partEle = document.createElement("Part");
				Element partNumEle = document.createElement("PartNumber");
				Element etagEle = document.createElement("ETag");
				partNumEle.appendChild(document.createTextNode(String.valueOf(item
						.getKey())));
				etagEle.appendChild(document.createTextNode(item.getValue()));

				partEle.appendChild(partNumEle);
				partEle.appendChild(etagEle);
				root.appendChild(partEle);
			}
            
            DOMSource source = new DOMSource(document);  
            
            TransformerFactory transformerFactory = TransformerFactory.newInstance();  
            Transformer transformer = transformerFactory.newTransformer();  
            transformer.setOutputProperty(OutputKeys.ENCODING, "utf-8");  
            transformer.setOutputProperty(OutputKeys.INDENT, "yes"); 
            
            StreamResult result = new StreamResult(conn.getOutputStream());  
            transformer.transform(source, result);

            System.out.println("http status: " + conn.getResponseCode());
			System.out.println("Headers:\n" + conn.getHeaderFields());

			System.out.println("---- body start ----");
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					conn.getInputStream()));
			String readLine;
			while ((readLine = reader.readLine()) != null) {
				System.out.println(readLine);
			}
			reader.close();
			System.out.println("---- body end ----");
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			conn.disconnect();
		}
	}

	public static String uploadpart(File localFile, String uploadId, int partNum,
			int offset, int len) throws Exception {
		HttpURLConnection conn = null;
		BufferedInputStream in = null;
		BufferedOutputStream out = null;
		String Etag = null;
		try {
			URL url = new URL("http://your-end.com/" + bucket + "/"
					+ localFile.getName() + "?partNumber=" + partNum
					+ "&uploadId=" + uploadId);
			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("PUT");
			conn.setDoOutput(true);
			conn.setDoInput(true);

			String contentType = "application/octet-stream";
			String dateString = DateUtil.formatDate(new Date(),
					DateUtil.PATTERN_RFC1036);
			String sign = sign("PUT", "", contentType, dateString, "/" + bucket
					+ "/" + localFile.getName() + "?partNumber=" + partNum
					+ "&uploadId=" + uploadId, null);
			conn.setRequestProperty("Date", dateString);
			conn.setRequestProperty("Authorization", sign);
			conn.setRequestProperty("Content-Type", contentType);
			conn.setRequestProperty("Content-Length", String.valueOf(len));

			out = new BufferedOutputStream(conn.getOutputStream());
			in = new BufferedInputStream(new FileInputStream(localFile));

			byte[] buffer = new byte[1024];
			int p = 0;
			int size = 1024;
			int remain = len;
			in.skip(offset);
			while ((remain > 0) && (p = in.read(buffer)) != -1) {
				out.write(buffer, 0, p);
				out.flush();
				offset += size;
				remain -= size;
				size = Math.min(remain,1024);
			}

			System.out.println("http status: " + conn.getResponseCode());
			System.out.println("after:\n" + conn.getHeaderFields());
			
			if(200 == conn.getResponseCode())
			{
				Etag = conn.getHeaderField("Etag");
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			close(in);
			close(out);
			conn.disconnect();
		}
		return Etag;
	}

	public static void abortupload(String objName, String uploadId) throws Exception {
		HttpURLConnection conn = null;
		try {
			URL url = new URL("http://your-end.com/" + bucket + "/" + objName
					+ "?uploadId=" + uploadId);
			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("DELETE");
			conn.setDoOutput(true);
			conn.setDoInput(true);

			String dateString = DateUtil.formatDate(new Date(),
					DateUtil.PATTERN_RFC1036);
			String sign = sign("DELETE", "", "", dateString, "/" + bucket + "/"
					+ objName + "?uploadId=" + uploadId, null);

			conn.setRequestProperty("Date", dateString);
			conn.setRequestProperty("Authorization", sign);

			System.out.println("http status: " + conn.getResponseCode());
			System.out.println("Headers:\n" + conn.getHeaderFields());

			System.out.println("---- body start ----");
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					conn.getInputStream()));
			String readLine;
			while ((readLine = reader.readLine()) != null) {
				System.out.println(readLine);
			}
			reader.close();
			System.out.println("---- body end ----");

		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			conn.disconnect();
		}
	}

	public static void listMultiUploads() throws Exception {
		HttpURLConnection conn = null;
		try {
			URL url = new URL("http://your-end.com/" + bucket + "/?uploads ");
			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setDoOutput(true);
			conn.setDoInput(true);

			Date date = new Date();
			String dateString = DateUtil.formatDate(date,
					DateUtil.PATTERN_RFC1036);
			String sign = sign("GET", "", "", dateString, "/" + bucket
					+ "/?uploads", null);

			System.out.println(sign);
			conn.setRequestProperty("Date", dateString);
			conn.setRequestProperty("Authorization", sign);

			System.out.println("http status: " + conn.getResponseCode());
			System.out.println("Headers:\n" + conn.getHeaderFields());
			
			DocumentBuilderFactory factory = DocumentBuilderFactory
					.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			Document document = builder.parse(conn.getInputStream());
            
			ArrayList<Map<String, String>> list = new ArrayList<Map<String, String>>();
			
			
			
			NodeList uploadList = document.getElementsByTagName("Upload");
				if (null == uploadList) {
					System.out.println("ERR no initList element!");
					return;
				} else {
					for (int i = 0; i < uploadList.getLength(); i++) {
						Node item = uploadList.item(i);
						System.out.println("Name : " + item.getNodeName()
								+ " Value: " + item.getNodeValue());

						NodeList chiList = item.getChildNodes();
						if (null != chiList) {
							String keyStr = null;
							String uploadId = null;
							for (int j = 0; j < chiList.getLength(); j++) {
								Node childitem = chiList.item(j);
								if (childitem.getNodeType() == Node.ELEMENT_NODE) {
								
									if ("Key".equals(childitem.getNodeName())) {
										keyStr = childitem.getFirstChild()
												.getNodeValue();
									}
									if ("UploadId".equals(childitem.getNodeName())) {
										uploadId = childitem.getFirstChild()
												.getNodeValue();
									}
									
									System.out.println(" --  Name : "
											+ childitem.getNodeName()
											+ " Value: "
											+ childitem.getFirstChild()
													.getNodeValue());
									
									NodeList thrList = childitem.getChildNodes();
									if (null != thrList) {
										for (int t = 0; t < thrList.getLength(); t++) {
											Node thritem = thrList.item(t);
											if (thritem.getNodeType() == Node.ELEMENT_NODE) {
												System.out.println("    ---  Name : "
														+ thritem.getNodeName()
														+ " Value: "
														+ thritem.getFirstChild()
																.getNodeValue());
											}
										}
									}
									
									
								}
							}
							if(null != keyStr && null != uploadId)
							{
								Map<String,String> uploads2abort = new HashMap<String, String>();
								uploads2abort.put(keyStr, uploadId);
								list.add(uploads2abort);
							}
							
						}
					}
				}
				
				for (Map<String, String> map : list) {
					for(Map.Entry<String, String> item : map.entrySet())
					{
						System.out.println("----------  Key " + item.getKey() + " UploadId " + item.getValue());
					    abortupload(item.getKey(), item.getValue());
					}
				}
			
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			conn.disconnect();
		}
	}

	private static void close(Closeable c) {
		try {
			if (c != null) {
				c.close();
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static String md5file(File file) throws Exception {
		MessageDigest messageDigest = MessageDigest.getInstance("MD5");
		BufferedInputStream in = new BufferedInputStream(new FileInputStream(
				file));
		byte[] buf = new byte[1024 * 100];
		int p = 0;
		while ((p = in.read(buf)) != -1) {
			messageDigest.update(buf, 0, p);
		}
		in.close();
		byte[] digest = messageDigest.digest();
		return null;
	}

	public static String sign(String httpVerb, String contentMD5,
			String contentType, String date, String resource,
			Map<String, String> metas) {

		String stringToSign = httpVerb + "\n"
				+ StringUtils.trimToEmpty(contentMD5) + "\n"
				+ StringUtils.trimToEmpty(contentType) + "\n" + date + "\n";
		if (metas != null) {
			for (Map.Entry<String, String> entity : metas.entrySet()) {
				stringToSign += StringUtils.trimToEmpty(entity.getKey()) + ":"
						+ StringUtils.trimToEmpty(entity.getValue()) + "\n";
			}
		}
		stringToSign += resource;
		try {
			Mac mac = Mac.getInstance("HmacSHA1");
			byte[] keyBytes = secretKey.getBytes("UTF8");
			SecretKeySpec signingKey = new SecretKeySpec(keyBytes, "HmacSHA1");
			mac.init(signingKey);
			byte[] signBytes = mac.doFinal(stringToSign.getBytes("UTF8"));
			String signature = encodeBase64(signBytes);
			return "AWS" + " " + accessKey + ":" + signature;
		} catch (Exception e) {
			throw new RuntimeException("MAC CALC FAILED.");
		}

	}

	private static String encodeBase64(byte[] data) {
		String base64 = new String(Base64.encodeBase64(data));
		if (base64.endsWith("\r\n"))
			base64 = base64.substring(0, base64.length() - 2);
		if (base64.endsWith("\n"))
			base64 = base64.substring(0, base64.length() - 1);

		return base64;
	}

}