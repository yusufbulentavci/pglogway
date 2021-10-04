package pglogway;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;

public class NoElasticTest extends ScenarioTest {
	
	private static ConfDir confDir;


	@BeforeClass
	public static void kur() {
		Main.testing = true;
		ElasticCon econ=new ElasticCon("localhost", "9200", "euser", "epwd");
		confDir=new ConfDir(false, econ, "/tmp/noelastic", "mycluster", "5433", 5, new HourList(), new HourList(), 0, 0, 0,
				null, null, null,
				null,null,null,null,null);
	}


	@Test
	public void denebakalim() throws IOException {
		File dir = new File("/tmp/noelastic");
		FileUtils.deleteDirectory(dir);
		dir.mkdir();
		ExtraFileUtils.copyResourcesRecursively(new URL(ExtraFileUtils.class.getResource("/scenarios/noelastic").toString()),
				new File("/tmp"));

		LogDir ld = new LogDir(confDir, 1, 1);

		new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					Thread.sleep(1000);
					File file = new File("/tmp/noelastic/postgresql-2021-02-08_10_32_00.csv-wait");
					file.renameTo(new File("/tmp/noelastic/postgresql-2021-02-08_10_32_00.csv"));
					System.out.println("!!Test Renamed waiting");
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

			}
		}).start();

		ld.run();
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		
		assertTrue(new File("/tmp/noelastic/postgresql-2021-02-08_10_31_38.csv-done").exists());
		assertTrue(!new File("/tmp/noelastic/postgresql-2021-02-08_10_31_38.csv").exists());
		assertTrue(new File("/tmp/noelastic/postgresql-2021-02-08_10_32_00.csv").exists());
		assertTrue(!new File("/tmp/noelastic/postgresql-2021-02-08_10_32_00.csv-done").exists());

	}

//	public static void main(String[] args) throws IOException {
//		tailTest.denebakalim();
//
//	}

}
