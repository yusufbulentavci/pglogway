package pglogway;

public class ElasticCon {
	private final String host;
	private final String port;
	private final String user;
	private final String pwd;

	public ElasticCon(String elasticHost, String elasticPort, String elasticUser, String elasticPwd) {
		this.host=elasticHost;
		this.port=elasticPort;
		this.user=elasticUser;
		this.pwd=elasticPwd;
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

}
