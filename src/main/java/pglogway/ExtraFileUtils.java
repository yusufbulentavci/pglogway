package pglogway;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.JarURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pglogway.exceptions.ChOwnFailedException;

public class ExtraFileUtils {
	static final Logger logger = LogManager.getLogger(ExtraFileUtils.class.getName());
	public static boolean copyFile(final File toCopy, final File destFile) {
		try {
			return ExtraFileUtils.copyStream(new FileInputStream(toCopy), new FileOutputStream(destFile));
		} catch (final FileNotFoundException e) {
			e.printStackTrace();
		}
		return false;
	}

	private static boolean copyFilesRecusively(final File toCopy, final File destDir) {
		assert destDir.isDirectory();

		if (!toCopy.isDirectory()) {
			return ExtraFileUtils.copyFile(toCopy, new File(destDir, toCopy.getName()));
		} else {
			final File newDestDir = new File(destDir, toCopy.getName());
			if (!newDestDir.exists() && !newDestDir.mkdir()) {
				return false;
			}
			for (final File child : toCopy.listFiles()) {
				if (!ExtraFileUtils.copyFilesRecusively(child, newDestDir)) {
					return false;
				}
			}
		}
		return true;
	}

	public static boolean copyJarResourcesRecursively(final File destDir, final JarURLConnection jarConnection)
			throws IOException {

		final JarFile jarFile = jarConnection.getJarFile();

		for (final Enumeration<JarEntry> e = jarFile.entries(); e.hasMoreElements();) {
			final JarEntry entry = e.nextElement();
			if (entry.getName().startsWith(jarConnection.getEntryName())) {
				final String filename = StringUtils.removeStart(entry.getName(), //
						jarConnection.getEntryName());

				final File f = new File(destDir, filename);
				if (!entry.isDirectory()) {
					final InputStream entryInputStream = jarFile.getInputStream(entry);
					if (!ExtraFileUtils.copyStream(entryInputStream, f)) {
						return false;
					}
					entryInputStream.close();
				} else {
					if (!ExtraFileUtils.ensureDirectoryExists(f)) {
						throw new IOException("Could not create directory: " + f.getAbsolutePath());
					}
				}
			}
		}
		return true;
	}

	public static boolean copyResourcesRecursively( //
			final URL originUrl, final File destination) {
		try {
			final URLConnection urlConnection = originUrl.openConnection();
			if (urlConnection instanceof JarURLConnection) {
				return ExtraFileUtils.copyJarResourcesRecursively(destination, (JarURLConnection) urlConnection);
			} else {
				return ExtraFileUtils.copyFilesRecusively(new File(originUrl.getPath()), destination);
			}
		} catch (final IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	private static boolean copyStream(final InputStream is, final File f) {
		try {
			return ExtraFileUtils.copyStream(is, new FileOutputStream(f));
		} catch (final FileNotFoundException e) {
			e.printStackTrace();
		}
		return false;
	}

	private static boolean copyStream(final InputStream is, final OutputStream os) {
		try {
			final byte[] buf = new byte[1024];

			int len = 0;
			while ((len = is.read(buf)) > 0) {
				os.write(buf, 0, len);
			}
			is.close();
			os.close();
			return true;
		} catch (final IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	private static boolean ensureDirectoryExists(final File f) {
		return f.exists() || f.mkdir();
	}
	
	public static void main(String[] args) throws MalformedURLException {
//		System.out.println(FileUtils.class.getResource("/log4j.properties"));
//		copyResourcesRecursively(new URL(FileUtils.class.getResource("/csv").toString()), new File("/tmp"));
	}
	
	public static void chown(String f, String usr, String grp) throws ChOwnFailedException {
		Path p = new File(f).toPath();
		try {
			UserPrincipalLookupService lookupService = FileSystems.getDefault().getUserPrincipalLookupService();
			GroupPrincipal group = lookupService.lookupPrincipalByGroupName(grp);
			UserPrincipal user = lookupService.lookupPrincipalByName(usr);
			Files.getFileAttributeView(p, PosixFileAttributeView.class, LinkOption.NOFOLLOW_LINKS).setOwner(user);
			Files.getFileAttributeView(p, PosixFileAttributeView.class, LinkOption.NOFOLLOW_LINKS).setGroup(group);
		} catch (IOException e) {
			logger.error("Failed to chwon:"+f, e);
			throw new ChOwnFailedException("Failed to chwon:"+f, e);
		}
	}
}