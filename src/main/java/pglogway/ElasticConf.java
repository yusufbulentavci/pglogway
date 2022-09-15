package pglogway;

public interface ElasticConf {

	String getCluster();

	String getPort();

	DataSourceCon getEcon();

	int getElasticExpireDays();

}
