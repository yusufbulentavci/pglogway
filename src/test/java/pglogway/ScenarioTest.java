package pglogway;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.json.JSONObject;

public class ScenarioTest {
	
	List<String> fixList(List<String> revised) {
		try {
			String hn = InetAddress.getLocalHost().getHostName();
			return revised.stream().map(element->element.replaceAll("gezen2", hn)).collect(Collectors.toList());
		} catch (UnknownHostException e) {
			throw new RuntimeException(e);
		}
	}

	
	void defaultControl(String dir, String file) throws IOException {
		List<String> original = Files
				.readAllLines(new File(dir+file+".json").toPath());
		List<String> revised = Files
				.readAllLines(new File(dir+"expection/"+file+".json").toPath());
		
		StringBuilder sb=new StringBuilder();
		
		compareJsonLines(original, revised, sb);
		if(!sb.toString().isBlank()) {
			assertFalse(true);
		}
		
		
		assertTrue(!new File(dir+file+"-done").exists());
		assertTrue(new File(dir+file).exists());
	}

	void compareJsonLines(List<String> original, List<String> expected, StringBuilder sb) {
		
		Map<Integer, JSONObject> orig = toMap(original);
		Map<Integer, JSONObject> exp = toMap(expected);
		for (Entry<Integer, JSONObject> k : exp.entrySet()) {
			Integer csvLine = k.getKey();
			JSONObject jo=orig.get(csvLine);
			if(jo == null) {
				sb.append("CsvLine:").append(csvLine).append(" Expected csv_line not found:"+csvLine).append("\n");
				continue;
			}
			compareJson(jo, k.getValue(), sb, csvLine);
		}
		
		for (Entry<Integer, JSONObject> k : orig.entrySet()) {
			Integer csvLine = k.getKey();
			if(!exp.containsKey(csvLine)) {
				sb.append("CsvLine:").append(csvLine).append(" Unexpected csv_line found:"+csvLine).append("\n");
				continue;
			}
		}
	}

	void compareJson(JSONObject jo, JSONObject expected, StringBuilder sb, int csvLine) {
		for (String it : expected.keySet()) {
			Object o=jo.opt(it);
			if(o == null) {
				sb.append("CsvLine:").append(csvLine).append(" Expected key not found:"+it).append("\n");
				continue;
			}
			Object e=expected.get(it);
			if(!e.equals(o)) {
				sb.append("CsvLine:").append(csvLine).append(" Expected key values is not equal for key:"+it).append(" Expected:"+e).append(" Written:"+o).append("\n");
			}
		}
	}

	Map<Integer, JSONObject> toMap(List<String> original) {
		Map<Integer, JSONObject> orig=new HashMap<Integer, JSONObject>();
		for (String string : original) {
			if(string.trim().length() == 0)
				continue;
			JSONObject jo=new JSONObject(string);
			Integer ind=jo.getInt("csv_ind");
			orig.put(ind, jo);
		}
		return orig;
	}

}
