package pglogway;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;

/*
 * Base test 
 */
public class Insert extends ScenarioTest {

	private static ConfDir confDir;

	@BeforeClass
	public static void kur() throws Exception {
		Main.testing = true;
		ElasticCon econ=new ElasticCon("localhost", "9200", "euser", "epwd", 1000);
		FilterByProp filterCommmand=new FilterByProp("command_tag", "-hede,-idle");
		confDir=new ConfDir(true, econ, "/tmp/insert", "mycluster", "5433", 5, new HourList(), new HourList(), 0, 0, 0,
				null, null, null,
				filterCommmand,null,null,null,null, false, "WARN");
	}

	@Test
	public void denebakalim() throws IOException {
		File dir = new File("/tmp/insert");
		FileUtils.deleteDirectory(dir);
		dir.mkdir();
		ExtraFileUtils.copyResourcesRecursively(
				new URL(ExtraFileUtils.class.getResource("/scenarios/insert").toString()), new File("/tmp"));

		LogDir ld = new LogDir(confDir, 0, 0);
		ld.run();

		defaultControl("/tmp/insert/","postgresql-2021-02-08_10_31_38.csv");

	}


	

//	public static void main(String[] args) throws IOException {
//		insertTest.denebakalim();
//
//	}

}