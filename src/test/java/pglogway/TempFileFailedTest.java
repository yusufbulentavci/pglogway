package pglogway;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import pglogway.logdir.LogDirMainWorker;

/*
 * tempfilefailed test 
 */
@Ignore
public class TempFileFailedTest extends ScenarioTest {

	private static ConfDir confDir;

	@BeforeClass
	public static void kur() throws Exception {
		Main.testing = true;
		DataSourceCon econ=new DataSourceCon("localhost", "9200", "euser", "epwd", 1000);
		FilterByProp filterCommmand=new FilterByProp("command_tag", "-hede,-idle");
		confDir=new ConfDir(true, econ, "/tmp/tempfilefailed", "mycluster", "5433", 5, new HourList(), new HourList(), 0, 0, 0,
				null, null, null,
				filterCommmand,null,null,null,null, false, "WARN", false, null);
	}

	@Test
	public void denebakalim() throws IOException {
		File dir = new File("/tmp/tempfilefailed");
		FileUtils.deleteDirectory(dir);
		dir.mkdir();
		ExtraFileUtils.copyResourcesRecursively(
				new URL(ExtraFileUtils.class.getResource("/scenarios/tempfilefailed").toString()), new File("/tmp"));

		LogDirMainWorker ld = new LogDirMainWorker(confDir, 0, 0);
		ld.run();

		defaultControl("/tmp/tempfilefailed/","postgresql-2021-02-08_10_31_38.csv");

	}


	

//	public static void main(String[] args) throws IOException {
//		tempfilefailedTest.denebakalim();
//
//	}

}
