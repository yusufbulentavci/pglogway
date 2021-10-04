package pglogway;

import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FilterByProp {
	static final Logger logger = LogManager.getLogger(FilterByProp.class);
	private final String prop;
	private Set<String> white;
	private Set<String> black;
	private long filtered = 0;
	private long passed = 0;

	public FilterByProp(String prop, String toParse) throws Exception {
		this.prop = prop;
		String str = toParse.replace(" ", "");
		if (str.length() == 0)
			return;
		String[] ss = str.split(",");
		for (String s : ss) {
			s = s.trim();
			if (s.length() == 1) {
				logger.warn("Invalid black white:|" + s + "| in prop:" + prop);
			} else if (s.startsWith("+")) {
				if (white == null)
					white = new HashSet<>();
				white.add(s.substring(1));
			} else if (s.startsWith("-")) {
				if (black == null)
					black = new HashSet<>();
				black.add(s.substring(1));
			} else {
				logger.warn("Invalid black white:|" + s + "| in prop:" + prop);
			}
		}
		logger.info("FilterByProp setup: prop:" + prop + " whites:" + (white != null ? white.toString() : "")
				+ " blacks:" + (black != null ? black.toString() : ""));
	}

	public boolean filter(String value) {
		boolean b = !in(value);
		if (b)
			filtered++;
		else
			passed++;
		return b;
	}

	private boolean in(String value) {
		if (white != null) {
			if (white.contains(value)) {
				return true;
			}
		}
		if (black != null) {
			if (black.contains(value)) {
				return false;
			}
		}

		return white == null;
	}

	public long getFiltered() {
		return filtered;
	}

	public long getPassed() {
		return passed;
	}

}
