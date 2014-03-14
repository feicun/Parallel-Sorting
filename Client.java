//package osprj1;

import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class Client {
	private String serverHostname = null;
	private int serverPort = 0;
	private Socket sock = null;
	private InputStream sockInput = null;
	private OutputStream sockOutput = null;
	public StringBuilder stringBui = new StringBuilder();

	public Client(String serverHostname, int serverPort) {
		this.serverHostname = serverHostname;
		this.serverPort = serverPort;
	}

	public void processMessages() throws IOException {
		System.err.println("Opening connection to " + serverHostname + " port "
				+ serverPort);
		try {
			sock = new Socket(serverHostname, serverPort);
			sockInput = sock.getInputStream();
			sockOutput = sock.getOutputStream();
		} catch (IOException e) {
			e.printStackTrace(System.err);
			return;
		}

		System.err.println("Handler start to work.");

		InputStream ips = sockInput;
		InputStreamReader ipsr = new InputStreamReader(ips);
		BufferedReader br = new BufferedReader(ipsr);

		ArrayList<String> tmpArray = new ArrayList<String>();
		
		//StringBuilder builder = new StringBuilder();
		String aux = "";

		while ((aux = br.readLine()) != null) {
			tmpArray.add(aux);
		}

		//String text = builder.toString();

		
		//for (String retval : text.split("\n")) {
		//	tmpArray.add(retval);
		//}
		System.out.println(tmpArray);
		Collections.sort(tmpArray);
		System.out.println(tmpArray);

		sock.shutdownInput();

		OutputStream out = sockOutput;
		OutputStreamWriter outStream = new OutputStreamWriter(out);
		BufferedWriter writer = new BufferedWriter(outStream);

		writer.write(tmpArray.toString());
		writer.flush();

		sock.shutdownOutput();
		writer.close();
		sock.close();
	}

	public static void main(String args[]) {
		//String hostname = "localhost";
		String hostname = args[0];
		int port = 29023;
		//byte[] data = "Hello World".getBytes();

		Client client = new Client(hostname, port);
		try {
			client.processMessages();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}