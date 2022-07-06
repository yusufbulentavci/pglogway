package pglogway;

import java.util.HashSet;
import java.util.Set;

public class HourList {
	Set<Integer> hours = new HashSet<>();
	private String toParse;
	public HourList() {
	}
	

	public HourList(String toParse) throws Exception {
		this.toParse = toParse;
		String str = toParse.replace(" ", "");
		if(str.length()==0)
			return;
		String[] ss = str.split(",");
		for (String s : ss) {
			int ind = s.indexOf('-');
			if (ind < 0) {
				addHour(s);
			} else {
				String[] range = s.split("\\-");
				if (range.length != 2) {
					throw new Exception("Failed to parse range in " + toParse);
				}
				int b,t;
				try {
					b = Integer.parseInt(range[0]);
					t = Integer.parseInt(range[1]);
				} catch (Exception e) {
					throw new Exception("Failed to parse:" + toParse + ", part:" + s);
				}
				for (int i = b; i <= t; i++) {
					addHour(i);
				}
			}

		}
	}

	@Override
	public String toString() {
		return "HourList [hours=" + hours + ", toParse=" + toParse + "]";
	}


	private void addHour(String s) throws Exception {
		int i;
		try {
			i = Integer.parseInt(s);
		} catch (Exception e) {
			throw new Exception("Failed to parse:" + toParse + ", hour is not in 0-24: Hour:" + s);
		}
		addHour(i);
	}

	private void addHour(int i) throws Exception {
		if (i < 0 || i > 24) {
			throw new Exception("Failed to parse:" + toParse + ", hour is not in 0-24: Hour:" + i);
		}
		if(i==24) {
			hours.add(0);
		}else if(i==0) {
			hours.add(24);
		}
		hours.add(i);
	}

	public boolean in(int i) {
		return this.hours.contains(i);
	}

}
