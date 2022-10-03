package pglogway.elastic;

import pglogway.DataSourceCon;

public interface ElasticConf {

	String getCluster();

	String getPort();

	DataSourceCon getEcon();

	int getElasticExpireDays();

}
