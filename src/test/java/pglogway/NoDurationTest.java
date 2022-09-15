package pglogway;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.github.difflib.DiffUtils;
import com.github.difflib.patch.AbstractDelta;
import com.github.difflib.patch.Patch;

import pglogway.ConfDir;
import pglogway.DataSourceCon;
import pglogway.ExtraFileUtils;
import pglogway.LogDir;
import pglogway.Main;

@Ignore
public class NoDurationTest extends ScenarioTest {

	private static ConfDir confDir;

	@BeforeClass
	public static void kur() {
		Main.testing = true;
		DataSourceCon econ=new DataSourceCon("localhost", "9200", "euser", "epwd", 1000);
		confDir=new ConfDir(true, econ, "/tmp/noduration", "mycluster", "5433", 5, new HourList(), new HourList(), 0, 0, 0,
				null, null, null,
				null,null,null,null,null, false, "WARN", false, null);
	}

	@Test
	public void denebakalim() throws IOException {
		File dir = new File("/tmp/noduration");
		FileUtils.deleteDirectory(dir);
		dir.mkdir();
		ExtraFileUtils.copyResourcesRecursively(
				new URL(ExtraFileUtils.class.getResource("/scenarios/noduration").toString()), new File("/tmp"));

		LogDir ld = new LogDir(confDir, 0, 0);
		ld.run();

		List<String> original = Files
				.readAllLines(new File("/tmp/noduration/postgresql-2021-02-08_10_31_38.csv.json").toPath());
		List<String> revised = Files
				.readAllLines(new File("/tmp/noduration/expection/postgresql-2021-02-08_10_31_38.csv.json").toPath());

//		//compute the patch: this is the diffutils part
		Patch<String> patch = DiffUtils.diff(original, revised);

		if (patch.getDeltas().size() > 0) {
			// simple output the computed patch to console
			for (AbstractDelta<String> delta : patch.getDeltas()) {
				System.out.println(delta);
			}
			assertTrue(false);
		}

		assertTrue(!new File("/tmp/noduration/postgresql-2021-02-08_10_31_38.csv-done").exists());
		assertTrue(new File("/tmp/noduration/postgresql-2021-02-08_10_31_38.csv").exists());

	}

//	public static void main(String[] args) throws IOException {
//		nodurationTest.denebakalim();
//
//	}

}
