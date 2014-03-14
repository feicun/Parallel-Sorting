//package osprj1;

import java.io.*;
import java.util.*;
import java.net.*;

class BinaryFileBuffer {
	// public static int BUFFERSIZE = 2048;
	public static int BUFFERSIZE = 1024 * 1000 * 5; // 8KB
	public BufferedReader bufferReader;
	public File originalFile;
	private String cache;
	private boolean empty;

	public BinaryFileBuffer(File f) throws IOException {
		originalFile = f;
		bufferReader = new BufferedReader(new FileReader(f), BUFFERSIZE);
		reload();
	}

	public boolean isEmpty() {
		return empty;
	}

	private void reload() throws IOException {
		try {
			if ((this.cache = bufferReader.readLine()) == null) {
				empty = true;
				cache = null;
			} else {
				empty = false;
			}
		} catch (EOFException oef) {
			empty = true;
			cache = null;
		}
	}

	public void close() throws IOException {
		bufferReader.close();
	}

	public String peek() {
		if (isEmpty())
			return null;
		return cache.toString();
	}

	public String pop() throws IOException {
		String answer = peek();
		reload();
		return answer;
	}
}


class readerLock {
	private int readers;
	private int writers;

	public readerLock() {
		readers = 0;
		writers = 0;
	}

	public int getReaders() {
		return readers;
	}

	public int getWriters() {
		return writers;
	}

	public synchronized void lockRead() {
		while (writers > 0) {
			try {
				this.wait();
			} catch (InterruptedException ex) {
				System.out.println(ex);
			}
		}
		readers++;
	}

	public synchronized void unlockRead() {
		readers--;
		notifyAll();
	}

	public synchronized void lockWrite() {
		while (writers > 0 || readers > 0) {
			try {
				this.wait();
			} catch (InterruptedException ex) {
				System.out.println(ex);
			}
		}
		writers++;
	}

	public synchronized void unlockWrite() {
		writers--;
		notifyAll();
	}
}




public class psort {
	String fileIn = null;
	int count = 0;
	int flag = 0;
	ArrayList<String> remoteSorted = null;
	boolean finish = false;
	int threadNum = 0;
	WorkQueue workQueue = new WorkQueue();
	//boolean hasServer = false;
	Server server = null;
	int countForClient = 10000;

	public psort() {

	}

	public psort(String fileIn) {
		this.fileIn = fileIn;
	}

	public synchronized void addCount() {
		count++;
	}
	
	public synchronized void addFlag() {
		flag++;
	}
	
	public synchronized int getCount() {
		return count;
	}
	
	public synchronized boolean isFlag() {
		if (flag < 5)
			return true;
		else
			return false;
		//return true;
	}
	
	public synchronized void addTread() {
		threadNum++;
	}
	
	public synchronized void decreaseTread() {
		threadNum--;
		
		if (threadNum <= 0) {
			this.notifyAll();
		}
	}
	
	public synchronized void getThreadNum() {
		try {
			while (threadNum > 0) {
				this.wait();
			}
		}
		catch (InterruptedException e)
		{ }
	}
	
	public synchronized void shutDown() {
		getThreadNum();
		workQueue.shutdown();
	}
	
	public void createServer(int port, String tmpDir) {
		try {
			server = new Server(port, tmpDir);
		} catch (IOException e) {
			e.printStackTrace(System.err);
		}
	}
	
	public class Server {
		private ServerSocket serverSock = null;
		private Socket sock = null;
		private String tmpDir;

		public Server(int serverPort, String tmpDir) throws IOException {
			this.tmpDir = tmpDir;
			serverSock = new ServerSocket(serverPort);
			
		}

		public void waitForConnections(byte[] bytIn) {
			// while (true) {
			try {
				Runtime.getRuntime().exec("./wakeUpTest");
				System.out.println("Trying to wake up client");
				sock = serverSock.accept();
				System.err
						.println("Server:Accepted new socket, creating new handler for it.");
				addTread();
				workQueue.execute(new Handler(sock, bytIn, tmpDir));
				//Handler handler = new Handler(sock, bytIn, tmpDir);
				//handler.start();
				System.err
						.println("Server:Finished with socket, waiting for next connection.");
				decreaseTread();
			}

			catch (IOException e) {
				e.printStackTrace(System.err);
			}

			// }
		}

	}

	public class Handler implements Runnable {
		private Socket sock = null;
		private InputStream sockInput = null;
		private OutputStream sockOutput = null;
		private Thread myThread = null;
		private String tmpDir;
		//private ArrayList<String> arrayList = new ArrayList<String>();
		private byte[] bytIn;

		public Handler(Socket sock, byte[] bytIn, String tmpDir)
				throws IOException {
			this.sock = sock;
			this.bytIn = bytIn;
			sockInput = sock.getInputStream();
			sockOutput = sock.getOutputStream();
			this.myThread = new Thread(this);
			this.tmpDir = tmpDir;
			
			System.out.println("Handler: New handler created.");
		}

		public void start() {
			myThread.start();
		}


		public void run() {
			System.out.println("Handler: Handler run() starting.");
			// while(true) {
			try {
				OutputStream ops = sockOutput;
				OutputStreamWriter opsw = new OutputStreamWriter(ops);
				BufferedWriter bw = new BufferedWriter(opsw);
				String str = new String(bytIn);
				bw.write(str);
				bw.flush();
				
				System.out.println("the length of input bytIn :" + str.length());

				sock.shutdownOutput();

				InputStream ips = sockInput;
				InputStreamReader ipsr = new InputStreamReader(ips);
				BufferedReader brIn = new BufferedReader(ipsr);

				//psort temp = new psort();
				System.out.println("before write the length of back data: " + str.length());
				//writeSmaillFile(brIn, tmpDir);
				//LocalWriter localWriter = new LocalWriter(brIn, tmpDir);
				
				//Thread t = new Thread(new Runnable(){ public void run() { 
				
				
				
				
				
				
				
				java.text.SimpleDateFormat df = new java.text.SimpleDateFormat(
						"yyyy-MM-dd-HH-mm-ss-SSS");
				java.util.Date d = new java.util.Date();
				String fileOut = tmpDir + "/part" + df.format(d) + ".txt";
				
				
				String toS = brIn.readLine();
				System.out.println("BR length: " + toS.length());
				int len = toS.length();
				String tmp = toS.substring(1, len - 1);
				System.out.println("Start to write "+tmp);
				List<String> elephantList = Arrays.asList(tmp.split(", "));
				

				FileWriter writer = new FileWriter(fileOut);		
			
			

			
			
			for (String aux : elephantList) {
				writer.write(aux + "\n");
			}

			writer.close();
				
				
				
				
				
				
				
				
				

				


			} catch (Exception e) {
				e.printStackTrace(System.err);
				// break;
			}
			// }

			try {
				System.err.println("Handler:Closing socket.");
				sock.close();
			} catch (Exception e) {
				System.err
						.println("Handler: Exception while closing socket, e="
								+ e);
				e.printStackTrace(System.err);
			}

		}
	}
	
	public class LocalWriter implements Runnable {
		private BufferedReader brIn = null;
		private Thread myThread = null;
		private String tmpDir;


		public LocalWriter(BufferedReader brIn, String tmpDir)
				throws IOException {
			this.brIn = brIn;
			this.myThread = new Thread(this);
			this.tmpDir = tmpDir;
			
			System.out.println("LocalWriter: New Writer created.");
		}

		public void start() {
			myThread.start();
		}


		public void run() {
			java.text.SimpleDateFormat df = new java.text.SimpleDateFormat(
					"yyyy-MM-dd-HH-mm-ss-SSS");
			java.util.Date d = new java.util.Date();
			String fileOut = tmpDir + "/part" + df.format(d) + ".txt";
			
			String toS = null;
			try {
				toS = brIn.readLine();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			int len = toS.length();
			String tmp = toS.substring(1, len - 1);
			System.out.println("I'm here "+tmp);
			List<String> elephantList = Arrays.asList(tmp.split(", "));
			
			FileWriter writer = null;
			try {
				writer = new FileWriter(fileOut);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}		
			
			for (String aux : elephantList) {
				try {
					writer.write(aux + "\n");
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			try {
				writer.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

	public void createSortedSmallFile(String fileIn, String tmpDir)
			throws IOException, InterruptedException {

		InputStream fis = new FileInputStream(fileIn);
		BufferedInputStream input = new BufferedInputStream(fis);
		// byte[] bytIn = new byte[1000 * 1000 * 20]; // The size of small file
		// byte[] bytIn = new byte[1000 * 1000 * 250]; // 200MB
		byte[] bytIn = new byte[1000*100*200]; // 200MB
		//readerLock lock = new readerLock();
		//lock.lockWrite();
		while ((input.read(bytIn)) != -1) {
			
			
			//server.waitForConnections(bytIn);
			
			
			if (isFlag()) {
			//if (true) {
				server.waitForConnections(bytIn);
			} else {
				String fileOut = tmpDir + "/part" + count + ".txt";
				addCount();
				FileWriter writer = new FileWriter(fileOut);

				for (String str : sort(bytIn)) {
					writer.write(str);
				}
				writer.close();
			}
			addFlag();
			//System.out.println("flag is " + flag);
			//System.out.println("How many thread: " + threadNum);
		}
		//lock.unlockWrite();
		//fis.close();
		System.out.println("Thread Num " + threadNum);
		getThreadNum();
		System.out.println("Thread NUm" + threadNum);
	}

	public synchronized void writeSmaillFile(BufferedReader brIn, String tmpDir) throws IOException {
		System.out.println("Enter write");
		//java.text.SimpleDateFormat df = new java.text.SimpleDateFormat(
		//		"yyyy-MM-dd-HH-mm-ss-SSS");
		//java.util.Date d = new java.util.Date();
		//String fileOut = tmpDir + "/part" + df.format(d) + ".txt";
		
		
		String toS = brIn.readLine();
		System.out.println("BR length: " + toS.length());
		int len = toS.length();
		String tmp = toS.substring(1, len - 1);
		System.out.println("Start to write "+tmp);
		List<String> elephantList = Arrays.asList(tmp.split(", "));
		
		String fileOut = tmpDir + "/part" + countForClient + ".txt";
		countForClient++;
		FileWriter writer = new FileWriter(fileOut);		
		
		

		
		
		for (String aux : elephantList) {
			writer.write(aux + "\n");
		}

		writer.close();
		
	}

	public ArrayList<String> sort(byte[] bytIn) {
		String str = new String(bytIn);
		ArrayList<String> tmpArray = new ArrayList<String>();
		for (String retval : str.split("\n")) {
			tmpArray.add(retval);
		}
		Collections.sort(tmpArray);

		return tmpArray;
	}

	public ArrayList<File> getFileList(String tmpDir) {
		ArrayList<File> splitFileList = new ArrayList<File>();
		File dir = new File(tmpDir);
		File[] files = dir.listFiles();
		for (int i = 0; i < files.length; i++) {
			File file = files[i];
			splitFileList.add(file);
		}

		return splitFileList;
	}

	public static void mergeSortedFiles(List<File> files, File outputfile,
			final Comparator<String> cmp) throws IOException {
		PriorityQueue<BinaryFileBuffer> pq = new PriorityQueue<BinaryFileBuffer>(
				11, new Comparator<BinaryFileBuffer>() {
					public int compare(BinaryFileBuffer i, BinaryFileBuffer j) {
						return cmp.compare(i.peek(), j.peek());
					}
				});

		for (File f : files) {
			BinaryFileBuffer binaryBuffer = new BinaryFileBuffer(f);
			pq.add(binaryBuffer);
		}

		BufferedWriter bufferWriter = new BufferedWriter(new FileWriter(
				outputfile));
		try {
			while (pq.size() > 0) {
				BinaryFileBuffer tmpBFB = pq.poll();
				String r = tmpBFB.pop();
				bufferWriter.write(r);
				bufferWriter.newLine();
				if (tmpBFB.isEmpty()) {
					tmpBFB.bufferReader.close();
					//tmpBFB.originalFile.delete();//Don't need this temp file anymore
				} else {
					pq.add(tmpBFB); //Add it back
				}
			}
		} finally {
			bufferWriter.close();
			for (BinaryFileBuffer bfb : pq)
				bfb.close();
		}
	}

	public static void main(String[] args) throws InterruptedException {
		java.text.SimpleDateFormat df = new java.text.SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss");
		java.util.Date d = new java.util.Date();
		System.out.println("System time is: " + df.format(d));

		/*
		if (args.length < 3) {
			System.out.println("Need input and output file names, and a tmp dir path");
			return;
		}

		String inputfile = args[0];
		String outputfile = args[1];
		String tmpDir = args[2];
		*/

		String inputfile = "/home/feicun/workspace/ostest/testFiles";
		String outputfile = "/home/feicun/workspace/ostest/testFiles.sorted";
		String tmpDir = "/home/feicun/ostmp/";

		int port = 29023;
		
		psort sortBigFile = new psort(inputfile);
		sortBigFile.createServer(port, tmpDir);		

		try {
			sortBigFile.createSortedSmallFile(inputfile, tmpDir);
			// wait();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//sortBigFile.shutDown();
		sortBigFile.getThreadNum();

		List<File> splitFilePathList = new ArrayList<File>();
		File dir = new File(tmpDir);
		File[] files = dir.listFiles();
		for (int i = 0; i < files.length; i++) {
			File file = files[i];
			splitFilePathList.add(file);
		}
		
		System.out.println("How many temp files: " + splitFilePathList.size());

		if (splitFilePathList.size() == 0) {
			System.out.println("No temp files existing");
			System.out.println("Please check the file path");
		}

		Comparator<String> comparator = new Comparator<String>() {
			public int compare(String r1, String r2) {
				return r1.compareTo(r2);
			}
		};

		try {
			mergeSortedFiles(splitFilePathList, new File(outputfile),
					comparator);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		java.text.SimpleDateFormat uf = new java.text.SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss");
		java.util.Date u = new java.util.Date();
		System.out.println("Now system time is: " + uf.format(u));
		
		for (File f : splitFilePathList) {
			f.delete();
		}
	}
}
