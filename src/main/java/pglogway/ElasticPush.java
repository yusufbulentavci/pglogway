package pglogway;

import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.time.Year;
import java.util.Calendar;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

// https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-low-usage-requests.html
/*
 * output.elasticsearch:
  # Array of hosts to connect to.

  # Protocol - either `http` (default) or `https`.
  #protocol: "https"

  # Authentication credentials - either API key or username/password.
  #api_key: "id:api_key"


 */
public class ElasticPush {
	static final Logger logger = LogManager.getLogger(ElasticPush.class);

	long ref;
	String indexName;
	private ConfDir confDir;

	private String indexNameWoDate;

//	private RestClient restClient;
	private BulkRequest bulkRequest;

	private RestHighLevelClient highClient;

	private boolean checkExpiredIndexes;

	private int pushed = 0;

	public ElasticPush(ConfDir confDir, String year, String month, String day, int hour) {
		this.confDir = confDir;
		this.indexNameWoDate = "pg" + confDir.getCluster() + "_" + ConfDir.getHostName() + "_"
				+ (confDir.getPort().equals("5432") ? "" : confDir.getPort()) + "_";
		this.indexName = indexName(year, month, day);
		ref = hour * 50000000;
	}

	private String indexName(Object year, Object month, Object day) {
		return indexNameWoDate + year.toString() + "_" + month.toString() + "_" + day.toString();
	}

	public void connect() {
		logger.info("Connecting to elastic search");
		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY,
				new UsernamePasswordCredentials(confDir.getEcon().getUser(), confDir.getEcon().getPwd()));

		RestClientBuilder builder = RestClient
				.builder(new HttpHost(confDir.getEcon().getHost(), confDir.getEcon().getPortInt(), "http"));

		builder.setFailureListener(new RestClient.FailureListener() {
			@Override
			public void onFailure(Node node) {
				logger.error("RestClient-faillistener. Failed:" + node.toString());
			}
		});
		builder.setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS);

		builder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
			@Override
			public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
				return requestConfigBuilder.setSocketTimeout(10000);
			}
		});
		builder.setHttpClientConfigCallback(new HttpClientConfigCallback() {
			@Override
			public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
				return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
			}
		});
//		this.restClient = builder.build();

		highClient = new RestHighLevelClient(builder);

		ensureIndex();

	}

	private void ensureIndex() {
		logger.info("Check index is ready:" + indexName);

		try {
			Response response;
			Request r = new Request("HEAD", indexName);
			response = highClient.getLowLevelClient().performRequest(r);
			if (response.getStatusLine().getStatusCode() != 200) {
				createIndex();
				return;
			}
			logger.info("We have index");

		} catch (IOException e) {
			createIndex();
		}
	}

	private void deleteIndex(String toDel) {
		if (logger.isDebugEnabled())
			logger.debug("Check index to delete:" + toDel);

		try {
			Response response;
			Request r = new Request("DELETE", toDel);
			response = highClient.getLowLevelClient().performRequest(r);
			if (response.getStatusLine().getStatusCode() != 200) {
				logger.info("Index deleted:" + toDel);
				return;
			}
		} catch (IOException e) {
		}
	}

	public void expireIndexes() {
		if (logger.isDebugEnabled()) {
			logger.debug("Check expire indexes for index:" + this.indexName);
		}
		this.checkExpiredIndexes = false;
		int expireDays = confDir.getElasticExpireDays();
		if (expireDays <= 0)
			return;
		Calendar oldday = Calendar.getInstance(); // today
		oldday.add(Calendar.DAY_OF_YEAR, -1 * expireDays);

		for (int i = 0; i < 30; i++) {
			oldday.add(Calendar.DAY_OF_YEAR, -1);
			Integer year = oldday.get(Calendar.YEAR);
			Integer month = oldday.get(Calendar.MONTH);
			Integer day = oldday.get(Calendar.DAY_OF_MONTH);
			String toDel = indexName(year, month, day);
			deleteIndex(toDel);
		}

	}

	private static InputStream getRes(String rn) {

		if (!rn.startsWith("/")) {
			// rn = rn.substring(1);
			rn = "/" + rn;
		}

		// InputStream u = ClassLoader.getSystemClassLoader().getResourceAsStream(rn);
		InputStream u = ElasticPush.class.getResourceAsStream(rn);
		if (u != null)
			return u;
		// logger.debug("Failed for:" + rn);

		rn = rn.substring(1);

		// logger.debug("Tried for:" + rn);
		// u = ClassLoader.getSystemClassLoader().getResourceAsStream(rn);
		u = ElasticPush.class.getResourceAsStream(rn);
		return u;
	}

	private static String loadString(String rn) throws IOException {
		InputStream f = getRes(rn);
		if (f == null) {
			throw new NullPointerException("Resource file not found:" + rn);
		}

		byte[] buffer = new byte[100000];
		int t = IOUtils.read(f, buffer);
		String s = new String(buffer, 0, t, "UTF-8");
		// System.err.println("========="+rn+"============"+t);
		// System.err.println(s);
		// System.err.println("========================");
		return s;
	}

	private void createIndex() {
		try {
			Request r = new Request("PUT", indexName);
			String crjson = ElasticPush.loadString("index-create.json");
			r.setJsonEntity(crjson);

			Response response = highClient.getLowLevelClient().performRequest(r);

			logger.debug("We created the index");
		} catch (IOException er) {
			logger.error("createIndex", er);
			close();
			throw new RuntimeException("Index couldnt be created:" + indexName, er);
		}
	}

	public void close() {
		logger.info("Closing the elastic connection");
		if (highClient != null) {
			try {
				highClient.close();
			} catch (IOException e) {
				logger.error("close", e);
			}
		}
	}

//	public void tryit() throws IOException {
//		Response response = restClient.performRequest(new Request("GET", "/"));
//		RequestLine requestLine = response.getRequestLine(); 
//		HttpHost host = response.getHost(); 
//		int statusCode = response.getStatusLine().getStatusCode();
//		System.out.println(statusCode);
//		Header[] headers = response.getHeaders(); 
//		String responseBody = EntityUtils.toString(response.getEntity());
//		System.out.println(responseBody);
//	}

//	public void tryit() throws IOException {
//		Request r = new Request("PUT", "denelogs2");
//		JSONObject settings = new JSONObject();
//		settings.put("number_of_shards", 1);
//		JSONObject jo = new JSONObject();
//		jo.put("settings", settings);
//		r.setJsonEntity(jo.toString());
//		Response response = restClient.performRequest(r);
//
//		RequestLine requestLine = response.getRequestLine();
//		HttpHost host = response.getHost();
//		int statusCode = response.getStatusLine().getStatusCode();
//		System.out.println(statusCode);
//		Header[] headers = response.getHeaders();
//		String responseBody = EntityUtils.toString(response.getEntity());
//		System.out.println(responseBody);
//	}

	public static void main(String[] args) throws InterruptedException, IOException {
		System.out.println(ElasticPush.loadString("index-create.json"));

//		ElasticPush rc = new ElasticPush(1);
//		rc.connect();
//		System.out.println("Connected");
//		rc.tryit();
//		Thread.sleep(5000);
//		rc.close();
//		System.out.println("Closed");
	}

//	public void tryit() throws IOException {
//	Request r=new Request("PUT", "denelogs1/_doc/8");
//	JSONObject jo=new JSONObject();
//	jo.put("model", "Vosvos");
//	jo.put("year", 1950);
//	r.setJsonEntity(jo.toString());
//	Response response = restClient.performRequest(r);
//	
//	RequestLine requestLine = response.getRequestLine(); 
//	HttpHost host = response.getHost(); 
//	int statusCode = response.getStatusLine().getStatusCode();
//	System.out.println(statusCode);
//	Header[] headers = response.getHeaders(); 
//	String responseBody = EntityUtils.toString(response.getEntity());
//	System.out.println(responseBody);
//}

	public void push(Map<String, Object> map) {
		
		if(logger.isDebugEnabled()) {
			logger.debug("Pushed:"+map.toString());
		}
//			logger.info("Pushing json");
		if (bulkRequest == null) {
			bulkRequest = new BulkRequest();
		}
//		Request r = new Request("PUT", indexName + "/_doc/" + (ref++));
		IndexRequest r = new IndexRequest(indexName).id((ref++) + "").source(map, XContentType.JSON);
		bulkRequest.add(r);
		this.pushed++;
		if (bulkRequest.numberOfActions() >= 100) {
			flush();
		}
	}

	public void flush() {
		if (bulkRequest == null || bulkRequest.numberOfActions() == 0) {
			return;
		}
//		if (logger.isDebugEnabled()) {
//			logger.debug("flush");
//		}
		try {
			highClient.bulk(bulkRequest, RequestOptions.DEFAULT);
		} catch (IOException e) {
			logger.error("flush", e);
			try {
				logger.error("Sleeping for a minute after error");
				Thread.sleep(60000);
			} catch (InterruptedException e1) {
			}
		} finally {
			bulkRequest = null;
		}

		if (checkExpiredIndexes) {
			expireIndexes();
		}
	}

	public void checkExpiredIndexes() {
		this.checkExpiredIndexes = true;
	}

	public int getPushed() {
		return pushed;
	}

}
