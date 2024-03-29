package pglogway;

import java.io.IOException;

import net.nbug.hexprobe.server.telnet.EasyShellServer;
import net.nbug.hexprobe.server.telnet.EasyTerminal;
import net.nbug.hexprobe.server.telnet.EasyShellServer.Command;

public class Telnet {
	static EasyShellServer srv = new EasyShellServer();
	public static void start() throws IOException {

		srv.registerCommand("status", new Command() {
			@Override
			public void execute(String name, String argument, EasyTerminal terminal) throws IOException {
				Main.one().status(terminal);
				terminal.flush();
			}
		});
		
		srv.registerCommand("counters", new Command() {
			@Override
			public void execute(String name, String argument, EasyTerminal terminal) throws IOException {
				Counters.one().status(terminal);
				terminal.flush();
			}
		});
		
		srv.registerCommand("version", new Command() {
			@Override
			public void execute(String name, String argument, EasyTerminal terminal) throws IOException {
				terminal.writeLine("Version:"+Main.version);
				terminal.flush();
			}
		});

		srv.start(2300);
	}

	public static void terminate() {
		try {
			srv.stop();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
