package pglogway;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Ini {
	static final Logger logger = LogManager.getLogger(Ini.class.getName());

	private IniFile file;
	private List<ConfDir> dirs = new ArrayList<>();

	public Ini(String fn) throws Exception {
		this.file = new IniFile(fn);

		int expireDays = this.file.getInt("global", "expiredays", 14);
		
		Boolean alarm=this.file.getBoolean("global", "alarm", false);
		String alarmlevel = this.file.getString("global", "alarmlevel", null);
		

		Boolean elastic = this.file.getBoolean("global", "elastic", null);
		String elasticHost = this.file.getString("global", "elastichost", "localhost");
		String elasticPort = this.file.getString("global", "elasticport", "9200");
		String elasticUser = this.file.getString("global", "elasticuser", null);
		String elasticPwd = this.file.getString("global", "elasticpwd", null);
		int elasticSentLimit = this.file.getInt("global", "elasticmaxsentperconnectionfile", Integer.MAX_VALUE);
		DataSourceCon econ = new DataSourceCon(elasticHost, elasticPort, elasticUser, elasticPwd, elasticSentLimit);
		
		Boolean pushPg = this.file.getBoolean("global", "pushpg", null);
		String pushPgHost = this.file.getString("global", "pushpg_host", "localhost");
		String pushPgPort = this.file.getString("global", "pushpg_port", "5432");
		String pushPgUser = this.file.getString("global", "pushpg_user", null);
		String pushPgPwd = this.file.getString("global", "pushpg_pwd", null);
		int pushPgSentLimit = this.file.getInt("global", "pushpg_maxsentperconnectionfile", Integer.MAX_VALUE);
		DataSourceCon ppcon = new DataSourceCon(pushPgHost, pushPgPort, pushPgUser, pushPgPwd, pushPgSentLimit);
		

		String noziphours = this.file.getString("global", "noziphours", null);
		String nocopyphours = this.file.getString("global", "nocopyhours", null);

		HourList nozip = new HourList(noziphours);
		HourList nocopu = new HourList(nocopyphours);

		int letLogStayInMins = this.file.getInt("global", "letlogstayinmins", 120);
		int hourlyGzipTimeoutInMins = this.file.getInt("global", "hourlygziptimeoutinmins", 30);
		int hourlyStoreTimeoutInMins = this.file.getInt("global", "hourlystoretimeoutinmins", 30);

		String storemethod = this.file.getString("global", "storemethod", "remove");
		String storehost = this.file.getString("global", "storehost", null);
		String storepath = this.file.getString("global", "storepath", null);

		String filtercommand = this.file.getString("global", "filtercommand", null);
		String filterdb = this.file.getString("global", "filterdb", null);
		String filteruser = this.file.getString("global", "filteruser", null);
		String filterlevel = this.file.getString("global", "filterlevel", null);

		Double filterminduration = this.file.getDouble("global", "filterminduration", null);


		for (int i = 0; i < 100; i++) {
			String section = "dir-" + i;
			if (!file.containsSection(section))
				continue;
			String path = file.getString(section, "path", null);
			if (path == null) {
				logger.error("Configuration path is missing in section:" + section);
				continue;
			}
			String cluster = file.getString(section, "cluster", null);
			if (cluster == null) {
				logger.error("Configuration cluster is missing in section:" + section);
				continue;
			}
			String port = file.getString(section, "port", null);
			if (port == null) {
				port = "5432";
			}
			
			Boolean elasticDir = this.file.getBoolean("global", "elastic", elastic);
			
			int dirExpireDays = this.file.getInt(section, "expiredays", expireDays);

			String snoziphours = this.file.getString(section, "noziphours", null);
			String snocopyphours = this.file.getString(section, "nocopyhours", null);

			HourList snozip = nozip;
			HourList snocopu = nocopu;
			if (snoziphours != null) {
				snozip = new HourList(snoziphours);
			}
			if (snocopyphours != null) {
				snocopu = new HourList(snocopyphours);
			}

			int sletLogStayInMins = this.file.getInt(section, "letlogstayinmins", letLogStayInMins);
			int shourlyGzipTimeoutInMins = this.file.getInt(section, "hourlygziptimeoutinmins",
					hourlyGzipTimeoutInMins);
			int shourlyStoreTimeoutInMins = this.file.getInt(section, "hourlystoretimeoutinmins",
					hourlyStoreTimeoutInMins);

			String sstoremethod = this.file.getString(section, "storemethod", storemethod);
			String sstorehost = this.file.getString(section, "storehost", storehost);
			String sstorepath = this.file.getString(section, "storepath", storepath);

			String sfiltercommand = this.file.getString(section, "filtercommand", filtercommand);
			String sfilterdb = this.file.getString(section, "filterdb", filterdb);
			String sfilteruser = this.file.getString(section, "filteruser", filteruser);
			String sfilterlevel = this.file.getString(section, "filterlevel", filterlevel);

			Double sfilterminduration = this.file.getDouble(section, "filterminduration", filterminduration);

			FilterByProp cfiltercommand = sfiltercommand == null ? null
					: new FilterByProp("command_tag", sfiltercommand);
			FilterByProp cfilterdb = sfilterdb == null ? null : new FilterByProp("postgresql.log.database", sfilterdb);
			FilterByProp cfilteruser = sfilteruser == null ? null
					: new FilterByProp("postgresql.log.user", sfilteruser);
			FilterByProp cfilterlevel = sfilterlevel == null ? null
					: new FilterByProp("postgresql.log.level", sfilterlevel);

			ConfDir cd = new ConfDir(elasticDir, econ, path, cluster, port, dirExpireDays, snozip, snocopu, sletLogStayInMins,
					shourlyGzipTimeoutInMins, shourlyStoreTimeoutInMins, sstoremethod, sstorehost, sstorepath,
					cfiltercommand, cfilterdb, cfilteruser, cfilterlevel, sfilterminduration, alarm, alarmlevel, pushPg, ppcon);

			String[] ms = new String[] { "ssh", "remove" };
			if (cd.getStoreMethod() == null || !Arrays.stream(ms).anyMatch(x -> x.equals(cd.getStoreMethod()))) {
				throw new Exception(
						"storeMethod is important, allowed values are ssh or remove. But not given or entered falsely");
			}
			getDirs().add(cd);
		}

	}

	public List<ConfDir> getDirs() {
		return dirs;
	}

}
