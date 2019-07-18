package cb.loadDoc;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import com.couchbase.client.java.Bucket;
//test connection
public class ClusterConnectionTest {

	ClusterConnection conn;
	Bucket bucket;

	static Logger logger = Logger.getLogger(ClusterConnectionTest.class.getName());

	@BeforeSuite
	public void setUp() {
		logger.log(Level.INFO, "Connecting with the cluster with user credentials");
		conn = new ClusterConnection(bucket);
		conn.openConnection("127.0.0.1", "admin", "test123", "default");
		bucket = conn.getBucket();
		logger.log(Level.INFO, "Established connection with the bucket");

	}

	@Test
	public void setUpConnectionTest() {
		
		Assert.assertTrue(bucket.name().contentEquals("default"));
		logger.log(Level.INFO, "Verified the bucket name as default");

	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void invalidConnection() throws Exception {
		conn.openConnection("", "", "", "");
		logger.log(Level.INFO, "verified invalid connection details throwing error");

	}

	@AfterSuite
	public void tearDown() {
		conn.closeConnection();
		logger.log(Level.INFO, "Closed the bucket and cluster connection. Finished testing of the test suite");

	}

}
