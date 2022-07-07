package pglogway;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;

public class MaintainGzipStore extends ScenarioTest {

	private static ConfDir confDir;

	@BeforeClass
	public static void kur() {
		Main.testing = true;
		ElasticCon econ = new ElasticCon("localhost", "9200", "euser", "epwd");
		confDir = new ConfDir(true, econ, "/tmp/maintain_gzip_store", "mycluster", "5433", 5, new HourList(), new HourList(),
				0, 0, 0, null, null, null,
				null,null,null,null,null, false, "WARN");
	}

	@Test
	public void denebakalim() throws IOException {
		File dir = new File("/tmp/maintain_gzip_store");
		FileUtils.deleteDirectory(dir);
		dir.mkdir();
		ExtraFileUtils.copyResourcesRecursively(
				new URL(ExtraFileUtils.class.getResource("/scenarios/maintain_gzip_store").toString()),
				new File("/tmp"));

		Gzip gzip = new Gzip();
		gzip.compressFiles(dir, 0, 0, false);

		assertTrue(!new File("/tmp/maintain_gzip_store/postgresql-2021-02-08_10.csv-done").exists());
		assertTrue(new File("/tmp/maintain_gzip_store/postgresql-2021-02-08_10.csv-done.gz").exists());

		Store store = new Store();
		assertEquals(1, store.doit("host", "storePath", "cluster", "server", "5432", "/tmp/maintain_gzip_store/", 0, false));
		assertTrue(!new File("/tmp/maintain_gzip_store/postgresql-2021-02-08_10.csv-done.gz").exists());
	}

//	public static void main(String[] args) throws IOException {
//		MergeTest.denebakalim();
//
//	}

}
