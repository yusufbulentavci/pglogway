package pglogway.logdir;

public interface DirState {
	boolean checkRollover() throws LogDirCanNotAccessDirectory;

}
