package pglogway;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.channels.Channels;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import pglogway.exceptions.LogDirParsingException;
import pglogway.logdir.LogParser;

public class LogParserTest extends ScenarioTest {

	private static ConfDir confDir;

	@BeforeClass
	public static void kur() {
		Main.testing = true;
		DataSourceCon econ = new DataSourceCon("localhost", "9200", "euser", "epwd", 1000);
		confDir = new ConfDir(true, econ, "/tmp/logparser", "mycluster", "5433", 5, new HourList(), new HourList(), 0,
				0, 0, null, null, null, null, null, null, null, null, false, "WARN", false, null);
	}

	@Test
	public void denebakalim() throws IOException, LogDirParsingException {
		File dir = new File("/tmp/logparser");
		FileUtils.deleteDirectory(dir);
		dir.mkdir();
		ExtraFileUtils.copyResourcesRecursively(
				new URL(ExtraFileUtils.class.getResource("/scenarios/logparser").toString()), new File("/tmp"));

		RandomAccessFile raf = new RandomAccessFile(new File(dir, "postgresql-2021-02-08_10_31_38.csv"), "r");

		BufferedReader is = new BufferedReader(new InputStreamReader(Channels.newInputStream(raf.getChannel())));

		String[] d;
		do {
			d = LogParser.parse(is);
			if(d!=null) {
				System.out.println(Arrays.deepToString(d));
			}
		} while (d != null);

//		LogDir ld = new LogDir(confDir, 0, 0);
//		ld.run();
//
//		List<String> original = Files
//				.readAllLines(new File("/tmp/merge/postgresql-2021-02-08_10_31_38.csv.json").toPath());
//		List<String> revised = Files
//				.readAllLines(new File("/tmp/merge/expection/postgresql-2021-02-08_10_31_38.csv.json").toPath());
//
////		//compute the patch: this is the diffutils part
//		Patch<String> patch = DiffUtils.diff(original, revised);
//
//		if (patch.getDeltas().size() > 0) {
//			// simple output the computed patch to console
//			for (AbstractDelta<String> delta : patch.getDeltas()) {
//				System.out.println(delta);
//			}
//			assertTrue(false);
//		}
//
//		assertTrue(!new File("/tmp/merge/postgresql-2021-02-08_10_31_38.csv-done").exists());
//		assertTrue(new File("/tmp/merge/postgresql-2021-02-08_10_31_38.csv").exists());

	}

//	public static void main(String[] args) throws IOException {
//		MergeTest.denebakalim();
//
//	}

}
