package cb.loadDoc;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.error.DocumentAlreadyExistsException;
import com.couchbase.client.java.error.RequestTooBigException;
//test cases for addig new doc
public class AddDocumentTest {

	AddDocument addDoc;
	Bucket bucket;

	String doc_id_remove;
	long userSize = 100;

	static Logger logger = Logger.getLogger(AddDocumentTest.class.getName());

	@BeforeClass
	public void setUp() {
		addDoc = new AddDocument(bucket);
		logger.log(Level.INFO, "executed the BeforeClass for 'AddDocumentTest'");

	}

	@Test(dataProvider = "syncNewDocList")
	public void testSyncAddNewDoc(String content, String doc_id) {
		doc_id_remove = doc_id;
		addDoc.upsertDocument(content, doc_id);
		logger.log(Level.INFO, "Tested the addition of new document for syncronous");

	}

	@Test(dataProvider = "syncExistingDocList", expectedExceptions = DocumentAlreadyExistsException.class)
	public void testSyncAddExistingDoc(String content, String doc_id) {
		addDoc.insertDocument(content, doc_id);
		logger.log(Level.INFO, "Tested the addition of existing doc. This method will fail if the bucket is empty");

	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void testSyncEmptyDoc() {
		logger.log(Level.INFO, "Testing empty document");
		addDoc.insertDocument("", "");
		logger.log(Level.INFO, "Tested the empty document in Sync mode");

	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void testSyncNonJsonDoc() {
		logger.log(Level.INFO, "Adding non JSON document");
		addDoc.insertDocument("test", "doc_4");
		logger.log(Level.INFO, "Testing non Json insertion throws exception");

	}

	@Test(dataProvider = "newDocWithExp")
	public void testAddWithExpiry(String content, String doc_id, int exp) throws InterruptedException {
		addDoc.insertDocWithExpiry(content, doc_id, exp);
		Thread.sleep(5000);

		logger.log(Level.INFO, "Added doc with expiry");
	}

	@Test(expectedExceptions = NullPointerException.class)
	public void testExpiredDoc() {
		Assert.assertNull(addDoc.getDocument("doc_exp3"));
		logger.log(Level.INFO, "Checking the expired document throws exception");

	}

	@Test(dependsOnMethods = "testSyncAddNewDoc", expectedExceptions = NullPointerException.class)
	public void removeDocument() {
		addDoc.removeDoc(doc_id_remove);
		Assert.assertNull(addDoc.getDocument(doc_id_remove));
		logger.log(Level.INFO, "Deleted a document and checking that its retrieval throws exception");

	}

	@Test(dataProvider = "bulkInsert")
	public void bulkInsert(String str1, String id, int count) {
		addDoc.bulkInsert(str1, id, count);
		logger.log(Level.INFO, "Inserted bulk documents through batching method");

	}

	@Test
	public void insertJsonFromFile() throws Exception {
		String[] files = addDoc.jsonFiles();

		for (int i = 0; i < files.length; i++) {
			String str = addDoc.extractData_JSON(files[i]).toString();
			addDoc.upsertDocument(str, "InsertJsonFile_" + i);
		}
		logger.log(Level.INFO, "Tested document insert from json file");

	}

	@Test(expectedExceptions = RequestTooBigException.class)
	public void testFileSize() {
		String[] files = addDoc.jsonFiles();
		long fileSize = 0;
		for (int i = 0; i < files.length; i++) {
			fileSize = addDoc.getDocSize(files[i]);
			logger.log(Level.INFO, "File size is: " + fileSize);

			if (fileSize > userSize)
				throw new RequestTooBigException();
		}
	}

	@Test(dataProvider = "concurrentInsert", threadPoolSize = 2, invocationCount = 2)
	public void concurrentTest(String content, String id) {
		addDoc.upsertDocument(content, id);
		logger.log(Level.INFO, "Thread-Id is : " + Thread.currentThread().getId());
		logger.log(Level.INFO, "inserted doc concurrently with id: " + id);

	}

	@Test(dataProvider = "asyncNewDocList")
	public void testAsyncAddNewDoc(String content, String doc_id) {
		addDoc.asyncUpsert(content, doc_id);
		logger.log(Level.INFO, "Inserted doc in async mode");

	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void testAsyncEmptyDoc() {
		logger.log(Level.INFO, "Testing empty document");
		addDoc.asyncInsert(null, "");
		logger.log(Level.INFO, "Tested async doc empty");

	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void testAsyncNonJsonDoc() {
		logger.log(Level.INFO, "Testing empty document");
		addDoc.asyncInsert("test", "invalid_json_1");
	}

	@Test
	public void testMaxCount() {
		if (addDoc.getCount() > 150) {
			logger.log(Level.SEVERE,
					"User trying to insert more than the allocated limit. count is: " + addDoc.getCount());
			Assert.fail();
		}

	}

	@DataProvider
	public Object[][] syncNewDocList() {
		return new Object[][] {
				{ "{\n" + "	\"employee\": {\n" + "		\"firstName\": \"John\",\n" + "		\"lastName\": \"Smith\",\n"
						+ "		\"website\": \"john.com\"\n" + "	}\n" + "}", "doc_5" },
				{ "{\n" + "    \"City\": \"Santa Clara\",\n" + "    \"Temperature\": \"31 Degree celsius\",\n"
						+ "    \"Humidity\": \"58 Percent\",\n" + "    \"WeatherDescription\": \"broken clouds\",\n"
						+ "    \"WindSpeed\": \"3.6 Km per hour\",\n" + "    \"WindDirectionDegree\": \"90 Degree\"\n"
						+ "}", "doc_new_1" } };
	}

	@DataProvider
	public Object[][] concurrentInsert() {
		return new Object[][] {
				{ "{\n" + "	\"City\": \"Santa Clara\",\n" + "	\"Temperature\": \"70 F\"\n" + "}",
						"doc_concurrent_1" },
				{ "{\n" + "	\"City\": \"Santa Ana\",\n" + "	\"Temperature\": \"72 F\"\n" + "}", "doc_concurrent_2" },
				{ "{\n" + "	\"City\": \"Santa Cruz\",\n" + "	\"Temperature\": \"71 F\"\n" + "}",
						"doc_concurrent_3" },
				{ "{\n" + "	\"City\": \"Santa Barbara\",\n" + "	\"Temperature\": \"76 F\"\n" + "}",
						"doc_concurrent_4" },
				{ "{\n" + "	\"City\": \"Santa Monica\",\n" + "	\"Temperature\": \"74 F\"\n" + "}",
						"doc_concurrent_5" },
				{ "{\n" + "	\"City\": \"Santa Fe\",\n" + "	\"Temperature\": \"78 F\"\n" + "}", "doc_concurrent_6" },
				{ "{\n" + "	\"City\": \"Santa Rosa\",\n" + "	\"Temperature\": \"77 F\"\n" + "}",
						"doc_concurrent_7" },
				{ "{\n" + "	\"City\": \"Santa Maria\",\n" + "	\"Temperature\": \"79 F\"\n" + "}",
						"doc_concurrent_8" },
				// { "{\n" + " \"City\": \"Santa Peso\",\n" + " \"Temperature\": \"80 F\"\n" +\
				// "}", "9" }

		};

	}

	@DataProvider
	public Object[][] syncExistingDocList() {
		return new Object[][] {
				{ "{\n" + "	\"employee\": {\n" + "		\"firstName\": \"John\",\n" + "		\"lastName\": \"Smith\",\n"
						+ "		\"website\": \"john.com\"\n" + "	}\n" + "}", "doc_5" },
				{ "{\n" + "    \"City\": \"Santa Clara\",\n" + "    \"Temperature\": \"31 Degree celsius\",\n"
						+ "    \"Humidity\": \"58 Percent\",\n" + "    \"WeatherDescription\": \"broken clouds\",\n"
						+ "    \"WindSpeed\": \"3.6 Km per hour\",\n" + "    \"WindDirectionDegree\": \"90 Degree\"\n"
						+ "}", "doc_existing_1" }

		};
	}

	@DataProvider
	public Object[][] newDocWithExp() {
		return new Object[][] { { "{ \n" + "  \"first_name\"  :  \"Sammy\", \n" + "  \"last_name\"   :  \"Dill\", \n"
				+ "  \"age\"      :  25 \n" + "}", "doc_exp1", 1 } };
	}

	@DataProvider
	public Object[][] asyncNewDocList() {
		return new Object[][] {
				{ "{\n" + "	\"employee\": {\n" + "		\"firstName\": \"Tim\",\n" + "		\"lastName\": \"Smith\",\n"
						+ "		\"website\": \"john.com\"\n" + "	}\n" + "}", "doc_async_1" }

		};
	}

	@DataProvider
	public Object[][] asyncExistingDocList() {
		return new Object[][] {
				{ "{\n" + "	\"employee\": {\n" + "		\"firstName\": \"Tim\",\n" + "		\"lastName\": \"Smith\",\n"
						+ "		\"website\": \"john.com\"\n" + "	}\n" + "}", "doc_async_existing_1" }

		};
	}

	@DataProvider
	public Object[][] bulkInsert() {
		return new Object[][] { {
				"{\n" + "    \"fruit\": \"Apple\",\n" + "    \"size\": \"Large\",\n" + "    \"color\": \"Red\"\n" + "}",
				"bulk_id_", 3 }

		};
	}
	
}
