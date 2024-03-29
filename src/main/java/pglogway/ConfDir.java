package pglogway;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pglogway.elastic.ElasticConf;

public class ConfDir implements ElasticConf{
	static final Logger logger = LogManager.getLogger(ConfDir.class);
	private static String hostName;
	private final String path;
	private final String cluster;
	private final String port;

	private final int expireDays;
	private final DataSourceCon econ;

	private final HourList noZip;
	private final HourList noCopy;
	private final int letLogStayInMins;
	private final int hourlyGzipTimeoutInMins;
	private final int hourlyStoreTimeoutInMins;

	private final String storeMethod;
	private final String storeHost;
	private final String storePath;

	private final FilterByProp filterCommand;
	private final FilterByProp filterDb;
	private final FilterByProp filterUser;
	private final FilterByProp filterLevel;
	private final Double filterMinDuration;
	private final Boolean elasticDir;
	private final Boolean alarm;
	private final String alarmLevel;
	
	private boolean pushPg;
	private DataSourceCon ppCon;

	public ConfDir(Boolean elasticDir, DataSourceCon econ, String path, String cluster, String port, int expireDays,
			HourList noZip, HourList noCopy, int letLogStayInMins, int hourlyGzipTimeoutInMins,
			int hourlyStoreTimeoutInMins, String storeMethod, String storeHost, String storePath,
			FilterByProp filterCommmand, FilterByProp filterDb, FilterByProp filterUser, FilterByProp filterLevel,
			Double filterMinDuration, Boolean alarm, String alarmlevel, Boolean pushPg, DataSourceCon ppcon) {
		this.elasticDir = elasticDir == null ? false : elasticDir;
		this.econ = econ;
		this.pushPg = pushPg == null ? false : pushPg;
		this.ppCon = ppcon;
		this.path = path;
		this.cluster = cluster;
		this.port = port;
		this.expireDays = expireDays;
		this.noZip = noZip;
		this.noCopy = noCopy;
		this.storeMethod = storeMethod;
		this.storeHost = storeHost;
		this.storePath = storePath;
		this.letLogStayInMins = letLogStayInMins;
		this.hourlyGzipTimeoutInMins = hourlyGzipTimeoutInMins;
		this.hourlyStoreTimeoutInMins = hourlyStoreTimeoutInMins;
		this.filterCommand = filterCommmand;
		this.filterDb = filterDb;
		this.filterUser = filterUser;
		this.filterLevel = filterLevel;
		this.filterMinDuration = filterMinDuration;
		this.alarm=alarm;
		this.alarmLevel = alarmlevel;
	}

	public String getPath() {
		return path;
	}

	public String getCluster() {
		return cluster;
	}

	public String getPort() {
		return port;
	}

	public int getExpireDays() {
		return expireDays;
	}

	public DataSourceCon getEcon() {
		return econ;
	}

	public static String getHostName() throws UnknownHostException {
			if (hostName == null)
				hostName = InetAddress.getLocalHost().getHostName();
			return hostName;
		
	}

	public HourList getNoZip() {
		return noZip;
	}

	public HourList getNoCopy() {
		return noCopy;
	}

	public int getLetLogStayInMins() {
		return letLogStayInMins;
	}

	public int getHourlyGzipTimeoutInMins() {
		return hourlyGzipTimeoutInMins;
	}

	public boolean isDontCopy() {
		return storeMethod == null || storeMethod.equalsIgnoreCase("remove");
	}

	public String getStorePath() {
		return storePath;
	}

	public String getStoreHost() {
		return storeHost;
	}

	public int getHourlyStoreTimeoutInMins() {
		return hourlyStoreTimeoutInMins;
	}

	public String getStoreMethod() {
		return storeMethod;
	}

	public FilterByProp getFilterCommand() {
		return filterCommand;
	}

	public FilterByProp getFilterDb() {
		return filterDb;
	}

	public FilterByProp getFilterUser() {
		return filterUser;
	}

	public FilterByProp getFilterLevel() {
		return filterLevel;
	}

	public Double getFilterMinDuration() {
		return filterMinDuration;
	}

	public Boolean getElasticDir() {
		return elasticDir;
	}

	public Integer getPortInt() {
		if (port == null)
			return 5432;
		try {
			return Integer.parseInt(this.port);
		} catch (Exception e) {
			logger.error("Can not parse port:"+port, e);
			return -1;
		}
	}

	public Boolean getAlarm() {
		return alarm!=null && alarm;
	}

	public String getAlarmLevel() {
		return alarmLevel;
	}

	public boolean isPushPg() {
		return pushPg;
	}

	

	public DataSourceCon getPpCon() {
		return ppCon;
	}

	@Override
	public String toString() {
		return "ConfDir [path=" + path + ", cluster=" + cluster + ", port=" + port + ", elasticExpireDays="
				+ expireDays + ", econ=" + econ + ", noZip=" + noZip + ", noCopy=" + noCopy
				+ ", letLogStayInMins=" + letLogStayInMins + ", hourlyGzipTimeoutInMins=" + hourlyGzipTimeoutInMins
				+ ", hourlyStoreTimeoutInMins=" + hourlyStoreTimeoutInMins + ", storeMethod=" + storeMethod
				+ ", storeHost=" + storeHost + ", storePath=" + storePath + ", filterCommand=" + filterCommand
				+ ", filterDb=" + filterDb + ", filterUser=" + filterUser + ", filterLevel=" + filterLevel
				+ ", filterMinDuration=" + filterMinDuration + ", elasticDir=" + elasticDir + ", alarm=" + alarm
				+ ", alarmLevel=" + alarmLevel + ", pushPg=" + pushPg + ", ppCon=" + ppCon + "]";
	}

	
}
