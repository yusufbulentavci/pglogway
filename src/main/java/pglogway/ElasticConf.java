package pglogway;

public interface ElasticConf {

	String getCluster();

	String getPort();

	ElasticCon getEcon();

	int getElasticExpireDays();

}
