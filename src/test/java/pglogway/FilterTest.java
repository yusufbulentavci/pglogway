package pglogway;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import com.github.difflib.DiffUtils;
import com.github.difflib.patch.AbstractDelta;
import com.github.difflib.patch.Patch;

import pglogway.ConfDir;
import pglogway.ElasticCon;
import pglogway.ExtraFileUtils;
import pglogway.LogDir;
import pglogway.Main;

public class FilterTest extends ScenarioTest {

	private static ConfDir confDir;

	@BeforeClass
	public static void kur() throws Exception {
		Main.testing = true;
		ElasticCon econ=new ElasticCon("localhost", "9200", "euser", "epwd");
		FilterByProp filterCommmand=new FilterByProp("command_tag", "-hede,-idle");
		confDir=new ConfDir(true, econ, "/tmp/filter", "mycluster", "5433", 5, new HourList(), new HourList(), 0, 0, 0,
				null, null, null,
				filterCommmand,null,null,null,null);
	}

	@Test
	public void denebakalim() throws IOException {
		File dir = new File("/tmp/filter");
		FileUtils.deleteDirectory(dir);
		dir.mkdir();
		ExtraFileUtils.copyResourcesRecursively(
				new URL(ExtraFileUtils.class.getResource("/scenarios/filter").toString()), new File("/tmp"));

		LogDir ld = new LogDir(confDir, 0, 0);
		ld.run();

		List<String> original = Files
				.readAllLines(new File("/tmp/filter/postgresql-2021-02-08_10_31_38.csv.json").toPath());
		List<String> revised = Files
				.readAllLines(new File("/tmp/filter/expection/postgresql-2021-02-08_10_31_38.csv.json").toPath());

//		//compute the patch: this is the diffutils part
		Patch<String> patch = DiffUtils.diff(original, revised);

		if (patch.getDeltas().size() > 0) {
			// simple output the computed patch to console
			for (AbstractDelta<String> delta : patch.getDeltas()) {
				System.out.println(delta);
			}
			assertTrue(false);
		}

		assertTrue(!new File("/tmp/filter/postgresql-2021-02-08_10_31_38.csv-done").exists());
		assertTrue(new File("/tmp/filter/postgresql-2021-02-08_10_31_38.csv").exists());

	}

//	public static void main(String[] args) throws IOException {
//		filterTest.denebakalim();
//
//	}

}
