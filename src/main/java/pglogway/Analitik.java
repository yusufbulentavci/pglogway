package pglogway;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import org.hsqldb.server.Server;


public class Analitik extends Thread{
	private Server server;
	private Connection con;
	
	List<LogLine> queue=new LinkedList();
	

	protected void kur() {
		server = new Server();
		server.setDatabaseName(0, "mainDb");
		server.setDatabasePath(0, "mem:mainDb");
		server.setDatabaseName(1, "standbyDb");
		server.setDatabasePath(1, "mem:standbyDb");
		server.setPort(9001); // this is the default port
		server.start();
	
		String url="jdbc:hsqldb:mem:mainDb;DB_CLOSE_DELAY=-1";
		try {
			Class.forName("org.hsqldb.jdbc.JDBCDriver");
			this.con = DriverManager.getConnection(url, "SA", "");
		} catch (ClassNotFoundException | SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	protected void bitir(boolean dostca) {
		if(this.server!=null) {
			server.shutdown();
		}
	}
	
	public void run() {
		synchronized (queue) {
			try {
				queue.wait();
				LogLine el = queue.remove(queue.size()-1);
				process(el);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	private void process(LogLine el) {
		// TODO Auto-generated method stub
		
	}

	public void doit(PgConnection c, LogLine ll) {
		synchronized (queue) {
			queue.add(ll);
		}
	}
	
}
