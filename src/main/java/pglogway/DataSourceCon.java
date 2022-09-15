package pglogway;

public class DataSourceCon {
	private final String host;
	private final String port;
	private final String user;
	private final String pwd;
	private final int sentLimit;

	public DataSourceCon(String elasticHost, String elasticPort, String elasticUser, String elasticPwd, int elasticSentLimit) {
		this.host=elasticHost;
		this.port=elasticPort;
		this.user=elasticUser;
		this.pwd=elasticPwd;
		this.sentLimit=elasticSentLimit;
	}

	public String getHost() {
		return host;
	}

	public String getPort() {
		return port;
	}
	
	public int getPortInt() {
		return Integer.parseInt(port);
	}

	public String getUser() {
		return user;
	}

	public String getPwd() {
		return pwd;
	}

	public int getSentLimit() {
		return sentLimit;
	}

	@Override
	public String toString() {
		return "DataSourceCon [host=" + host + ", port=" + port + ", user=" + user + ", pwd=" + pwd + ", sentLimit="
				+ sentLimit + "]";
	}

}
