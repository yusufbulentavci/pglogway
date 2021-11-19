package pglogway;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.zip.GZIPOutputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Gzip {
	static final Logger logger = LogManager.getLogger(Gzip.class);

	public int compressFiles(File directory, int timeoutMins, int letItStayMins, boolean dontCopy) {
		File[] files = directory.listFiles(new FilenameFilter() {

			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".csv-done");
			}
		});

		if (files == null || files.length == 0) {
			logger.debug("No files to compress in directory:" + directory.toPath());
			return 0;
		}
		if (logger.isDebugEnabled()) {
			logger.debug("File count to compress is " + files.length + " directory:" + directory.getPath());
		}

		Arrays.sort(files, Comparator.comparingLong(File::lastModified));

		long now = System.currentTimeMillis();
		long compressBefore = now - letItStayMins * 60 * 1000;
		long returnAfter = now + timeoutMins * 60 * 1000;
		int done = 0;
		for (File file : files) {
			long lm = file.lastModified();
			if (lm > compressBefore) {
				return done;
			}
			
			if(dontCopy) {
				if(logger.isDebugEnabled()) {
					file.delete();
					logger.debug("File is deleted:"+file.getAbsolutePath());
				}
				done++;
			}
			
			if (compressGzipFile(file)) {
				done++;
			}
			
			if(System.currentTimeMillis()>returnAfter) {
				logger.debug("Time is over. Compression delayed for directory:"+directory.getAbsolutePath());
				return done;
			}
		}

		return done;
	}

	private boolean compressGzipFile(File file) {
		File source = file;
		File target = new File(file.getAbsolutePath() + ".gz");

		try (FileInputStream fis = new FileInputStream(source); FileOutputStream fos = new FileOutputStream(target);) {
			GZIPOutputStream gzipOS = new GZIPOutputStream(fos);
			byte[] buffer = new byte[1024];
			int len;
			while ((len = fis.read(buffer)) != -1) {
				gzipOS.write(buffer, 0, len);
			}
			gzipOS.close();
			fos.close();
			fis.close();
			source.delete();
			if (logger.isDebugEnabled()) {
				logger.debug("Compressed:" + file.getAbsolutePath());
			}
			return true;
		} catch (IOException e) {
			logger.error("Failed to zip file:" + file.getAbsolutePath(), e);
			Main.fatal();
			return false;
		}
	}

}
