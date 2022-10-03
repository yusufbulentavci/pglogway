package pglogway.logdir;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pglogway.exceptions.LogDirParsingException;

public class LogParser {

	static final Logger logger = LogManager.getLogger(LogParser.class);
	
	public static String[] parse(BufferedReader is) throws LogDirParsingException {
		String[] ret = new String[30];
		int iret = 0;

		StringBuilder sb = new StringBuilder();
		try {
			boolean start = true;
			boolean inquote = false;
			boolean celiski = false;
			char last = (char) -1, c = (char) -1;
			do {
				last = c;
				int t = is.read();
//				System.out.println(t + "-" + c);
//				System.out.println(inquote + "," + start + "," + celiski + "->" + sb.toString());
				c = (char) t;

				if (t == -1) {
					// not enough space come back
					return null;
				}

				if (start) {
					start = false;
					if (t == '"') {
						inquote = true;
						continue;
					} else {
						inquote = false;
					}
				}

				if (celiski) {
					celiski = false;
					if (t != '"') {
						inquote = false;
					}
				}else {
					if (t == '"') {
						if (inquote) {
							celiski = true;
							continue;
						}
					}
				}



				if ((!inquote || celiski) && (t == ',' || t == 10)) {

					if (sb.length() > 0) {
						if (iret >= ret.length) {
							throw new LogDirParsingException(
									"Unexpected long csv:" + sb.toString() + " Csv:" + Arrays.toString(ret));
						}
						ret[iret] = sb.toString();
//						System.out.println(sb.toString());
						sb = new StringBuilder();
					} else {
						ret[iret] = null;
					}
					iret++;
					start = true;

					if (t == 10) {
//						if(ret!=null) {
//							System.out.println(Arrays.toString(ret));
//						}
						return ret;
					}
				} else {
					if (sb.length() > 80000) {
						throw new LogDirParsingException("Too long csv item:" + sb.toString());
					}
					sb.append(c);
				}
			} while (true);
		} catch (IOException e) {
			logger.error("Parsing error", e);
			throw new LogDirParsingException("Too long csv item:" + sb.toString(), e);

		}

	}

}
