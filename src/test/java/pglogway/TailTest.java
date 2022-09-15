package pglogway;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
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
public class TailTest extends ScenarioTest {
	
	private static ConfDir confDir;


	@BeforeClass
	public static void kur() {
		Main.testing = true;
		DataSourceCon econ=new DataSourceCon("localhost", "9200", "euser", "epwd", 1000);
		confDir=new ConfDir(true, econ, "/tmp/tail", "mycluster", "5433", 5, new HourList(), new HourList(), 0, 0, 0,
				null, null, null,
				null,null,null,null,null, false, "WARN", false, null);
	}


	@Test
	public void denebakalim() throws IOException {
		File dir = new File("/tmp/tail");
		FileUtils.deleteDirectory(dir);
		dir.mkdir();
		ExtraFileUtils.copyResourcesRecursively(new URL(ExtraFileUtils.class.getResource("/scenarios/tail").toString()),
				new File("/tmp"));

		LogDir ld = new LogDir(confDir, 0, 1);

		new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					Thread.sleep(500);
					File add = new File("/tmp/tail/add.txt");
					String string = FileUtils.readFileToString(add);
					File file = new File("/tmp/tail/postgresql-2021-02-08_10_31_38.csv");
					FileUtils.writeStringToFile(file, string, Charset.defaultCharset(), true);
					System.out.println("!!Appended");
				} catch (IOException | InterruptedException e) {
					e.printStackTrace();
				}

			}
		}).start();

		ld.run();
		System.out.println();
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		List<String> original = Files
				.readAllLines(new File("/tmp/tail/postgresql-2021-02-08_10_31_38.csv.json").toPath());
		List<String> revised = Files
				.readAllLines(new File("/tmp/tail/expection/postgresql-2021-02-08_10_31_38.csv.json").toPath());
		
		revised=fixList(revised);
//
//		//compute the patch: this is the diffutils part
		Patch<String> patch = DiffUtils.diff(original, revised);

		if (patch.getDeltas().size() > 0) {
			// simple output the computed patch to console
			for (AbstractDelta<String> delta : patch.getDeltas()) {
				System.out.println(delta);
			}
			assertTrue(false);
		}
		
		assertTrue(!new File("/tmp/tail/postgresql-2021-02-08_10_31_38.csv-done").exists());
		assertTrue(new File("/tmp/tail/postgresql-2021-02-08_10_31_38.csv").exists());

	}

//	public static void main(String[] args) throws IOException {
//		tailTest.denebakalim();
//
//	}

}
