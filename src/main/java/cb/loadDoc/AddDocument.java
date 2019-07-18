package cb.loadDoc;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.DocumentAlreadyExistsException;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.error.RequestTooBigException;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;

//code for adding new doc to CB server

import rx.Observable;
import rx.functions.Func1;

public class AddDocument extends ClusterConnection {

	Bucket bucket;
	ClusterConnection conn;

	AddDocument(Bucket buck) {
		super(buck);
		conn = new ClusterConnection(bucket);
		conn.openConnection("127.0.0.1", "admin", "test123", "default");
		bucket = conn.getBucket();
	}

	void insertDocument(String content, String doc_id) throws DocumentAlreadyExistsException {
		JsonObject.create();
		JsonObject jsonObj = JsonObject.fromJson(content);
		bucket.insert(JsonDocument.create(doc_id, jsonObj));
		logger.log(Level.WARNING, "Document with " + doc_id + " already exists");
	}

	void upsertDocument(String content, String doc_id) {
		JsonObject.create();
		JsonObject jsonObj = JsonObject.fromJson(content);
		bucket.upsert(JsonDocument.create(doc_id, jsonObj));

		logger.log(Level.CONFIG, "Inserted the document with id: " + doc_id);
	}

	void insertDocWithExpiry(String content, String doc_id, int exp) {
		JsonObject.create();
		JsonObject jsonObj = JsonObject.fromJson(content);
		bucket.insert(JsonDocument.create(doc_id, exp, jsonObj));
	}

	void asyncInsert(String content, String doc_id) {
		JsonObject.create();
		JsonObject jsonObj = JsonObject.fromJson(content);
		JsonDocument doc = JsonDocument.create(doc_id, jsonObj);
		Observable<JsonDocument> insert = bucket.async().insert(doc);

		insert.subscribe();

	}

	void asyncUpsert(String content, String doc_id) {
		JsonObject.create();
		JsonObject jsonObj = JsonObject.fromJson(content);
		JsonDocument doc = JsonDocument.create(doc_id, jsonObj);
		Observable<JsonDocument> upsert = bucket.async().upsert(doc);

		upsert.subscribe();
	}

	void bulkInsert(String content, String id, int count) {

		List<JsonDocument> documents = new ArrayList<JsonDocument>();
		JsonObject.create();
		JsonObject jsobObj = JsonObject.fromJson(content);

		for (int i = 0; i < count; i++) {
			documents.add(JsonDocument.create(id + i, jsobObj));
		}

		Observable.from(documents).flatMap(new Func1<JsonDocument, Observable<JsonDocument>>() {
			public Observable<JsonDocument> call(final JsonDocument content) {
				return bucket.async().upsert(content);
			}
		}).subscribe();

	}

	JsonObject getDocument(String doc_id) throws NullPointerException {
		JsonDocument doc = bucket.get(doc_id);
		return doc.content();
	}

	long getCount() {
		N1qlQuery countQuery = N1qlQuery.simple("SELECT COUNT(*) AS rowCount FROM `default`");
		N1qlQueryResult result = bucket.query(countQuery);
		long bucketSize = 0;
		if (result.finalSuccess()) {
			List<N1qlQueryRow> rows = result.allRows();
			bucketSize = rows.get(0).value().getLong("rowCount");
		}
		return bucketSize;
	}

	void removeDoc(String id) throws DocumentDoesNotExistException {
		bucket.remove(id);
	}

	String[] jsonFiles() {
		String[] files = new String[2];
		files[0] = "././jsonData/address.json";
		files[1] = "././jsonData/name.json";

		return files;
	}

	long getDocSize(String fileName){
		File file = new File(fileName);
		return file.length();

	}
	
	public Object extractData_JSON(String name) throws Exception {
		FileReader reader = new FileReader(name);
		JSONParser parser = new JSONParser();

		JSONObject jsonObject = (JSONObject) parser.parse(reader);
		Object obj = (Object) jsonObject;

		return obj;
	}


}
