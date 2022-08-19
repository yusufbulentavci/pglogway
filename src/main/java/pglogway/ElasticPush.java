package pglogway;

import java.io.IOException;
import java.io.InputStream;
import java.util.Calendar;

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
import org.elasticsearch.client.Node;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkRequest.Builder;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;

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
	private ElasticConf confDir;

	private String indexNameWoDate;

	private RestClient restClient;
	private ElasticsearchClient esClient;
	private Builder bulkRequest;

	private boolean checkExpiredIndexes;

	private int pushed = 0;
	private int sent = 0;
//
//	public static void main(String[] args) {
//		logger.info("Hello");
//		ElasticConf cd = new ElasticConf() {
//
//			@Override
//			public String getCluster() {
//				return "denec";
//			}
//
//			@Override
//			public String getPort() {
//				return "9200";
//			}
//
//			@Override
//			public ElasticCon getEcon() {
//				return new ElasticCon("e1", "9200", "elastic", "sP3yiTXGADmAbJxh5mB=", 10000);
//			}
//
//			@Override
//			public int getElasticExpireDays() {
//				return 10;
//			}
//		};
//
//		logger.info("Starting up");
//		ElasticPush ep = new ElasticPush(cd, "2022", "12", "20", 1);
//		ep.connect();
//		
//		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSS] x");
//		LogLine ll = new LogLine(formatter, 5, "ERROR", "LOG_CSV_PARSE", "csv file modified", 10);
//		ep.push(ll);
//		
//		ep.flush();
//	}

	public ElasticPush(ElasticConf confDir, String year, String month, String day, int hour) {
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
		this.restClient = builder.build();

		ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
		esClient = new ElasticsearchClient(transport);

		ensureIndex();
	}

	private void ensureIndex() {
		logger.info("Check index is ready:" + indexName);

		try {
			Response response;
			Request r = new Request("HEAD", indexName);
			response = restClient.performRequest(r);
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
			response = restClient.performRequest(r);
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
			rn = "/" + rn;
		}

		InputStream u = ElasticPush.class.getResourceAsStream(rn);
		if (u != null)
			return u;

		rn = rn.substring(1);

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
		return s;
	}

	private void createIndex() {
		try {
			Request r = new Request("PUT", indexName);
			String crjson = ElasticPush.loadString("index-create.json");
			r.setJsonEntity(crjson);

			Response response = restClient.performRequest(r);

			logger.debug("We created the index");
		} catch (IOException er) {
			logger.error("createIndex", er);
			close();
			Main.fatal();
			throw new RuntimeException("Index couldnt be created:" + indexName, er);
		}
	}

	public void close() {
		logger.info("Closing the elastic connection");
		if (restClient != null) {
			try {
				restClient.close();
			} catch (IOException e) {
				logger.error("close", e);
				Main.fatal();
			}
		}
	}

	public synchronized void push(LogLine ll) {

		if (logger.isDebugEnabled()) {
			logger.debug("Pushed:" + toJacksonString(ll));
		}
		if (bulkRequest == null) {
			bulkRequest = new BulkRequest.Builder();
		}
		this.pushed++;
		bulkRequest.operations(op -> op.index(idx -> idx.index(indexName).id((ref++) + "").document(ll)));
		if (pushed - sent >= 100) {
			flush();
		}
	}

	ObjectMapper mapper = new ObjectMapper();
	private String toJacksonString(LogLine ll) {
		try {
			return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(ll);
		} catch (JsonProcessingException e) {
			logger.error("Jackson can not generate text from LogLine", e);
			return "error";
		}
	}

	public synchronized void flush() {
		if (bulkRequest == null || pushed - sent == 0) {
			return;
		}
		sent = pushed;
		try {
			esClient.bulk(bulkRequest.build());
		} catch (IOException e) {
			logger.error("flush", e);
			Main.fatal();
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
