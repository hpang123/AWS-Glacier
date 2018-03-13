package com.amazonaws.samples;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.TreeHashGenerator;
import com.amazonaws.services.glacier.model.UploadArchiveRequest;
import com.amazonaws.services.glacier.transfer.ArchiveTransferManager;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sqs.AmazonSQSClient;

/**
 * This sample demonstrates how to make basic requests to Amazon Glacier using
 * the AWS SDK for Java.
 * 
 * Fill in your AWS access credentials in the provided credentials file
 * template, and be sure to move the file to the default location
 * (C:\\Users\\Ray\\.aws\\credentials) where the sample code will load the
 * credentials from.
 * <p>
 * <b>WARNING:</b> To avoid accidental leakage of your credentials, DO NOT keep
 * the credentials file in your source directory.
 *
 * http://aws.amazon.com/security-credentials
 * 
 */
/*
 * See latest java sample
 * https://docs.aws.amazon.com/amazonglacier/latest/dev/uploading-an-archive-
 * single-op-using-java.html#uploading-an-archive-single-op-high-level-using-
 * java
 * 
 */
public class GlacierSample {

	/*
	 * vault name can be found in
	 * https://us-east-2.console.aws.amazon.com/glacier/home?region=us-east-2#/
	 * vaults
	 */
	public static String vaultName = "my-first-vault";
	public static String archiveId = "TPrtI_jRrjV05BzD8yVvdIZgHFH2EZh3HhdJF7_gyKldN8jH72wHftCJQ6tgcCrNgCdTXS5zRoeLhlQEytJwbGSx2nSEc7tafquyTgQhrPg7xswvfzp7ZSm1-zVsW-ZmkfNslwDFwQ";
	/*
	 * archiveIds:
	 * eFOwN19lsfUjhza9CTOkm3JTTuTIgqTXWQvIIlN1YAESNVgdD51swRFWLR2_fyW0gohXwfS0Oq9Jf3bWNY8P-fCqGTyakyTvQ6M-o6DE_jKR8DireiTq8wvuAbUaV3D3MH1MOjN8PA
	 * 
	 * wfpQR4QnWvdJiXKF4UjC9GpPc-LT6EKkmTK7lk0ylJSWeMKA9PgZol13H_uuKkOMQL3qQJ40zWtNnt9UNbu1A9Oe2EsyRJJW7ho1eI3kyDZceBpblWmxK4jHa7hGhRGgcoacnh9qqQ
	 */
	public static String archiveToUpload = "c:/temp/foo.txt";
	public static String downloadFilePath = "c:/users/Ray/xxx/";
	public static AmazonGlacierClient glacierClient;
	public static AmazonSQSClient sqsClient;
	public static AmazonSNSClient snsClient;

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {

		/*
		 * The ProfileCredentialsProvider will return your [default] credential profile
		 * by reading from the credentials file located at
		 * (C:\\Users\\Ray\\.aws\\credentials).
		 */
		AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider("default").getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
					+ "Please make sure that your credentials file is at the correct "
					+ "location (C:\\Users\\Ray\\.aws\\credentials), and is in valid format.", e);
		}

		/* Using High level API */
		glacierClient = new AmazonGlacierClient(credentials);
		glacierClient.setEndpoint("https://glacier.us-east-2.amazonaws.com/");

		try {
			
			//ArchiveTransferManager atm = new ArchiveTransferManager(glacierClient, credentials);

			//UploadResult result = atm.upload(vaultName, "my test archive " + (new Date()), new File(archiveToUpload));
			//archiveId = result.getArchiveId();
			
			System.out.println("Archive ID: " + archiveId);

		} catch (Exception e) {
			System.err.println(e);
		}

		/*
		 * Using Low level API
		 */

		try {
			// First open file and read.
			File file = new File(archiveToUpload);
			InputStream is = new FileInputStream(file);
			byte[] body = new byte[(int) file.length()];
			is.read(body);

			// Send request.
			UploadArchiveRequest request = new UploadArchiveRequest().withVaultName(vaultName)
					.withChecksum(TreeHashGenerator.calculateTreeHash(new File(archiveToUpload)))
					.withBody(new ByteArrayInputStream(body)).withContentLength((long) body.length);

			//UploadArchiveResult uploadArchiveResult = glacierClient.uploadArchive(request);

			//archiveId = uploadArchiveResult.getArchiveId();
			System.out.println("Archive ID: " + archiveId);

		} catch (Exception e) {
			System.err.println("Archive not uploaded.");
			System.err.println(e);
		}

		/* 
		 * https://docs.aws.amazon.com/amazonglacier/latest/dev/downloading-an-archive-using-java.html
		 * 
		 * Download Archive Using High Level API
		 */
		glacierClient = new AmazonGlacierClient(credentials);
		sqsClient = new AmazonSQSClient(credentials);
		snsClient = new AmazonSNSClient(credentials);

		glacierClient.setEndpoint("glacier.us-east-2.amazonaws.com");
		sqsClient.setEndpoint("sqs.us-east-2.amazonaws.com");
		snsClient.setEndpoint("sns.us-east-2.amazonaws.com");

		try {
			ArchiveTransferManager atm = new ArchiveTransferManager(glacierClient, sqsClient, snsClient);

			atm.download(vaultName, archiveId, new File(downloadFilePath));
			System.out.println("Downloaded file to " + downloadFilePath);

		} catch (Exception e) {
			System.err.println(e);
		}

	}
}
