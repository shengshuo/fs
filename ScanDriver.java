import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Date;
import java.util.Properties;


public class ScanDriver {
	static PathMapBuilder pathBuilder = null;
	static FileScanner scanner = null;
	static File topDirFile = null;
	static File pathMapFile = null;
	static File configFile = null;
	static Properties configProp = null;
 	static String runtimeDir = "";
 	static String configDir = "config";
 	static String configFileName = "config.xml";
 	static String pathMapFileName = "pathmap";
 	static String topDirFileName = "topmap";
 	static OutputStream configOutput  = null;
 	static FileInputStream configInput = null;	
	static File heartBeatFile = null;
	static String heartBeatName = "scannerHeartBeat.txt";
	static int beatChkIntvl = 30; // sec
	static int listenTime = 120; // sec
	
	static BufferedReader hbr = null;
	
	public static void main(String[] args) throws Exception {
		runtimeDir = System.getProperty("user.dir");
		String configPath = runtimeDir + "\\" + configDir;
		new File(configPath).mkdir();
		configFile = new File(configPath + "\\" + configFileName);
		configProp = new Properties();
		heartBeatFile = new File(runtimeDir + "\\" + heartBeatName);
		try {
			boolean keepListen = true;
			Long startT = new Date().getTime();
			while (keepListen) {
				if (checkHeartBeat()) {
					System.out.println("Alive!");
				} else {
					System.out.println("Dead!");
					FileScanner.startScan(new String[0]);
					keepListen = false;
				}

				if (new Date().getTime() - startT >= listenTime * 1000) {
					keepListen = false;
				}
			}
		} finally {
			if (hbr != null) {
				try {
					hbr.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	private static Boolean checkHeartBeat() {	
		BufferedReader hbr = null;
		try {
			hbr = new BufferedReader(new FileReader(heartBeatFile));
			String beat1 = null;
			String beat2 = null;
			beat1 = hbr.readLine();
			hbr.close();
			try {
				Thread.sleep(beatChkIntvl * 1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			hbr = new BufferedReader(new FileReader(heartBeatFile));
			beat2 = hbr.readLine();
			hbr.close();			
			boolean isAlive = !beat1.toString().equalsIgnoreCase(beat2.toString());
			return (isAlive);
		} catch (IOException ioe) {
			System.out.println(ioe.getMessage());
			return null;
		} 
	}
}