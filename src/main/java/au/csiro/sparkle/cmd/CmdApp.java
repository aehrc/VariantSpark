package au.csiro.sparkle.cmd;

public abstract class CmdApp {

	protected void runApp(String[] args) throws Exception {
		if (args.length == 1 && "-h".equals(args[0])) {
			usage();
		} else {
			run(args);
		}
	}
	
	protected abstract void usage();
	protected abstract void run(String[] args) throws Exception;

	public static void runApp(String[] args,CmdApp app) {
		try {
			app.runApp(args);
		} catch (Exception ex) {
	    	ex.printStackTrace();
	    	System.exit(2);
	    }
	}
}
