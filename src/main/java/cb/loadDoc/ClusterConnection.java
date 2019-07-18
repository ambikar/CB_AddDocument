package cb.loadDoc;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.couchbase.client.core.BucketClosedException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.error.BucketDoesNotExistException;

// new connection
public class ClusterConnection {

	Cluster cluster;
	Bucket bucket;

	static Logger logger = Logger.getLogger(ClusterConnection.class.getName());

	ClusterConnection(Bucket bucket){
		this.bucket = bucket;
	}
	
	public ClusterConnection() {
		// TODO Auto-generated constructor stub
	}

	public void setCluster(String ip, String userName, String pwd) {
		cluster = CouchbaseCluster.create(ip);
		cluster.authenticate(userName, pwd);
	}

	public void setBucket(String name) {
		bucket = cluster.openBucket(name);
	}

	public Bucket getBucket() {
		return bucket;
	}
	
	public void openConnection(String ip, String userName, String pwd, String bucketName) throws BucketDoesNotExistException{
		this.setCluster(ip, userName, pwd);
		this.setBucket(bucketName);
		logger.log(Level.INFO, "connected to bucket with name: " + bucket.name());
	}

	public String getBucketName() {
		return bucket.name();
	}

	public void closeConnection() throws BucketClosedException {
		bucket.close();
		cluster.disconnect();
		logger.log(Level.INFO, "Connection to bucket and cluster closed");

	}
}
