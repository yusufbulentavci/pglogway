package pglogway;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.common.IOUtils;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.connection.channel.direct.Session.Command;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import net.schmizz.sshj.xfer.FileSystemFile;

public class Store {
	static final Logger logger = LogManager.getLogger(Store.class.getName());

	private Set<String> existingFolders = new HashSet<String>();

	public int doit(String host, String storePath, String cluster, String server, String port, String localDir,
			int timeoutMins, boolean dontCopy) {

		File directory = new File(localDir);
		File[] files = directory.listFiles(new FilenameFilter() {

			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".csv-done.gz");
			}
		});

		if (files == null || files.length == 0) {
			logger.debug("No files to scopy in directory:" + directory.toPath());
			return 0;
		}
		if (logger.isDebugEnabled()) {
			logger.debug("File count to scopy is " + files.length + " directory:" + directory.getPath());
		}

		Arrays.sort(files, Comparator.comparingLong(File::lastModified));

		long now = System.currentTimeMillis();
		long returnAfter = now + timeoutMins * 60 * 1000;
		int done = 0;
		for (File file : files) {
			String pattern = "postgresql-(\\d+)-(\\d+)-(\\d+)_(\\d+)(.*)";

			// Create a Pattern object
			Pattern r = Pattern.compile(pattern);

			// Now create matcher object.
			Matcher m = r.matcher(file.getName());

			if (m.find()) {
				String year = m.group(1);
				String month = m.group(2);

				if (scp(host, storePath, cluster, server, port, year + "-" + month, file)) {
					done++;
				}

				if (System.currentTimeMillis() > returnAfter) {
					logger.info("Time is over. Scp delayed for directory:" + directory.getAbsolutePath());
					return done;
				}

			} else {
				logger.error(
						"Unexpected file name for scp, should be in pattern <postgresql-(\\\\d+)-(\\\\d+)-(\\\\d+)_(\\\\d+)(.*)>:"
								+ file.getName());
			}

		}

		return done;
	}

	public boolean scp(String host, String storePath, String cluster, String server, String port, String dateFolder,
			File lfile) {

		String rdir = storePath + "/" + cluster + "/" + server + "-" + port + "/" + dateFolder;
		String rdirAtHost = host + ":" + rdir;
		String user = System.getProperty("user.name");

		if (logger.isDebugEnabled()) {
			logger.debug("Rdir:" + rdirAtHost);
		}

		if (!Main.testing) {
			if (!this.existingFolders.contains(rdirAtHost)) {

				try (final SSHClient ssh = new SSHClient();) {
					ssh.addHostKeyVerifier(new PromiscuousVerifier());

					ssh.loadKnownHosts();
					ssh.connect(host);
					ssh.authPublickey(user);
					try (Session session = ssh.startSession();) {

						String mkdir = "mkdir -p " + rdir;
						final Command cmd = session.exec(mkdir);
						String out = IOUtils.readFully(cmd.getInputStream()).toString();
						cmd.join(10, TimeUnit.SECONDS);
						if (cmd.getExitStatus() != 0) {
							logger.error("Failed to make dir:" + mkdir);
							logger.error("Output for mkdir:" + out);
							return false;
						}
						existingFolders.add(rdirAtHost);
						logger.info("Folder created:" + rdir + " in host:" + host);
					}
				} catch (IOException e1) {
					logger.error("Failed make directory for host" + host, e1);
					return false;
				}
			}

			try (final SSHClient ssh = new SSHClient();) {
				ssh.addHostKeyVerifier(new PromiscuousVerifier());
				ssh.loadKnownHosts();
				ssh.connect(host);
				ssh.authPublickey(user);

				// Present here to demo algorithm renegotiation - could have just put this
				// before connect()
				// Make sure JZlib is in classpath for this to work
				ssh.useCompression();
				ssh.newSCPFileTransfer().upload(new FileSystemFile(lfile), rdir);
				if (logger.isDebugEnabled()) {
					logger.debug("File sent:" + lfile.getAbsolutePath());
				}

			} catch (IOException e1) {
				logger.error("Failed make directory for host" + host, e1);
				return false;
			}
		}

		lfile.delete();
		logger.info("Log file transferred and deleted:" + lfile.getAbsolutePath());
		return true;
	}
//	public boolean scp(String host, String storePath, String cluster, String server, String month, File lfile) {
//		Session session = null;
//		try {
//
//			String rdir = storePath + "/" + cluster + "/" + server + "/" + month;
//			String rdirAtHost = host + ":" + rdir;
//			
//			if(logger.isDebugEnabled()) {
//				logger.debug("Rdir:"+rdirAtHost);
//			}
//
//			if (!Main.testing) {
//
//				
//				java.util.Properties config = new java.util.Properties();
//				config.put("StrictHostKeyChecking", "no");
//
//				JSch jsch = new JSch();
//				
//				String user = "postgres";
//				String privateKey = "/var/lib/pgsql/.ssh/id_rsa";
//				jsch.addIdentity(privateKey);
//
//				session = jsch.getSession(user, host, 22);
//				session.setConfig(config);
//				session.connect();
//				
//				boolean ptimestamp = true;
//
//
//				if (!this.existingFolders.contains(rdirAtHost)) {
//					Channel channel = session.openChannel("exec");
//					channel.connect();
//					try (OutputStream out = channel.getOutputStream(); InputStream in = channel.getInputStream();) {
//						String mkdir = "mkdir -p " + rdir;
//						((ChannelExec) channel).setCommand(mkdir);
//
//						byte[] tmp = new byte[1024];
//						while (true) {
//							while (in.available() > 0) {
//								int i = in.read(tmp, 0, 1024);
//								if (i < 0)
//									break;
//								if (logger.isDebugEnabled()) {
//									logger.debug(new String(tmp, 0, i));
//								}
//							}
//							if (channel.isClosed()) {
//								if (in.available() > 0)
//									continue;
//								if (channel.getExitStatus() != 0) {
//									logger.error("Failed to mkdir for host:" + host + " mkdir=" + mkdir);
//									return false;
//								}
//								existingFolders.add(rdirAtHost);
//								if (logger.isDebugEnabled()) {
//									logger.debug("Directory created:" + rdir + " host:" + host);
//								}
//								break;
//							}
//							try {
//								Thread.sleep(10);
//							} catch (Exception ee) {
//							}
//						}
//					}
//				}
//
//				String rfile = rdir + "/" + lfile.getName();
//
//				Channel channel = session.openChannel("exec");
//				String command = "scp " + (ptimestamp ? "-p" : "") + " -t " + rfile;
//				if(logger.isDebugEnabled()) {
//					logger.debug("Scp command:"+command);
//				}
//				((ChannelExec) channel).setCommand(command);
//				channel.connect();
//
//				File _lfile;
//				try (OutputStream out = channel.getOutputStream(); InputStream in = channel.getInputStream();) {
//					if (checkAck(in) != 0) {
//						logger.error("Failed to scp " + lfile.getAbsolutePath());
//						return false;
//					}
//					_lfile = lfile;
//					// get I/O streams for remote scp
//
//					if (ptimestamp) {
//						command = "T " + (_lfile.lastModified() / 1000) + " 0";
//						// The access time should be sent here,
//						// but it is not accessible with JavaAPI ;-<
//						command += (" " + (_lfile.lastModified() / 1000) + " 0\n");
//						out.write(command.getBytes());
//						out.flush();
//						if (checkAck(in) != 0) {
//							logger.error("Failed to scp " + lfile.getAbsolutePath());
//							return false;
//						}
//					}
//					// send "C0644 filesize filename", where filename should not include '/'
//					long filesize = _lfile.length();
//					command = "C0644 " + filesize + " " + lfile.getName();
//					command += "\n";
//					out.write(command.getBytes());
//					out.flush();
//					if (checkAck(in) != 0) {
//						logger.error("Failed to scp " + lfile.getAbsolutePath());
//						return false;
//					}
//					// send a content of lfile
//					try (FileInputStream fis = new FileInputStream(lfile);) {
//						byte[] buf = new byte[1024];
//						while (true) {
//							int len = fis.read(buf, 0, buf.length);
//							if (len <= 0)
//								break;
//							out.write(buf, 0, len); // out.flush();
//						}
//						buf[0] = 0;
//						out.write(buf, 0, 1);
//						out.flush();
//					}
//					if (checkAck(in) != 0) {
//						logger.error("Failed to scp " + lfile.getAbsolutePath());
//						return false;
//					}
//					// send '\0'
//				}
//			}
//
//			lfile.delete();
//			if (logger.isDebugEnabled()) {
//				logger.debug("Log file transferred and deleted:" + lfile.getAbsolutePath());
//			}
//
//			return true;
//		} catch (Exception e) {
//			logger.error("Error scp for file:" + lfile, e);
//			return false;
//		} finally {
//			if (session != null) {
//				try {
//					session.disconnect();
//				} catch (Exception e) {
//				}
//			}
//		}
//
//	}
//
//	static int checkAck(InputStream in) throws IOException {
//		int b = in.read();
//		// b may be 0 for success,
//		// 1 for error,
//		// 2 for fatal error,
//		// -1
//		if (b == 0)
//			return b;
//		if (b == -1)
//			return b;
//
//		if (b == 1 || b == 2) {
//			StringBuffer sb = new StringBuffer();
//			int c;
//			do {
//				c = in.read();
//				sb.append((char) c);
//			} while (c != '\n');
//			if (b == 1) { // error
//				System.out.print(sb.toString());
//			}
//			if (b == 2) { // fatal error
//				System.out.print(sb.toString());
//			}
//		}
//		return b;
//	}

}
