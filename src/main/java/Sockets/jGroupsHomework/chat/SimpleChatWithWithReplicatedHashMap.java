package Sockets.jGroupsHomework.chat;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.blocks.ReplicatedHashMap;
import org.jgroups.protocols.BARRIER;
import org.jgroups.protocols.FD_ALL;
import org.jgroups.protocols.FD_SOCK;
import org.jgroups.protocols.FRAG2;
import org.jgroups.protocols.MERGE3;
import org.jgroups.protocols.MFC;
import org.jgroups.protocols.PING;
import org.jgroups.protocols.SEQUENCER;
import org.jgroups.protocols.UDP;
import org.jgroups.protocols.UFC;
import org.jgroups.protocols.UNICAST3;
import org.jgroups.protocols.VERIFY_SUSPECT;
import org.jgroups.protocols.pbcast.FLUSH;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.protocols.pbcast.STATE;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;

public class SimpleChatWithWithReplicatedHashMap extends ReceiverAdapter{
	
	
	private static final String CLASTER_NAME = "ChatCluster";
	private JChannel channel;
	
	private final List<String> history = new ArrayList<>();
	private final Lock historyLock = new ReentrantLock();
	
	private ReplicatedHashMap<String, Integer> wordCounter;
	
	public static void main(String[] args) {
		try {
			new SimpleChatWithWithReplicatedHashMap().runClient();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public void receive(Message msg) {
		System.out.println(msg.src() + ":" + msg.dest() + "  |  " + msg.getObject());
	}
	
	@Override
	public void viewAccepted(View view) {
		System.out.println("View : " + view);
	}
	
	
	@Override
	public void getState(OutputStream output) {
		DataOutput out = new DataOutputStream(output);
		historyLock.lock();
		try {
			Util.objectToStream(history, out);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			historyLock.unlock();
		}
		System.out.println("Current history state serialized");
	}
	
	@Override
	public void setState(InputStream input) {
		historyLock.lock();
		try {
			DataInput in = new DataInputStream(input);
			@SuppressWarnings("unchecked")
			List<String> newState = (List<String>) Util.objectFromStream(in);
			history.clear();
			history.addAll(newState);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			historyLock.unlock();
		}
		System.out.println("Current history state deserialized " + history);
	}
	
	
	private void runClient() throws Exception{
		
		System.setProperty("java.net.preferIPv4Stack", "true"); // zeby program dzialal na IPv4
		
		this.channel = new JChannel();
		ProtocolStack stack = new ProtocolStack(); // hierarchia protokolow  
		channel.setProtocolStack(stack); // ustaw stos protokolow dla naszego kanalu
		
		UDP udp = new UDP(); // adres multicastowy taki by nie zaklocac pracy przez inne aplikacje
		udp.setValue("mcast_group_addr", InetAddress.getByName("230.0.0.9"));
		
		stack.addProtocol(udp)
		.addProtocol(new PING())
		.addProtocol(new MERGE3())
		.addProtocol(new FD_SOCK())
		.addProtocol(new FD_ALL().setValue("timeout", 12000).setValue("interval", 3000)) // protokol wykrywania bledow (oparty na heartbeat kazdego klienta)
		.addProtocol(new VERIFY_SUSPECT())
		.addProtocol(new BARRIER())
		.addProtocol(new NAKACK2())
		.addProtocol(new UNICAST3())
		.addProtocol(new STABLE())
		.addProtocol(new GMS())
		.addProtocol(new UFC())
		.addProtocol(new MFC())
		.addProtocol(new FRAG2())
		.addProtocol(new STATE()) // protokol przesylania stanu (jeden z wielu) (minimalizuje uzycie pamieci)
		.addProtocol(new SEQUENCER())
		.addProtocol(new FLUSH());
		stack.init();
		

//		@SuppressWarnings("resource")
//		RpcDispatcher dispatcher = new RpcDispatcher(channel, this, this, this);

		wordCounter = new ReplicatedHashMap<>(channel);
		channel.connect(CLASTER_NAME); // polacz sie z grupa 
//		dispatcher.start();
		wordCounter.start(10_000);
		//pierwszy wezel zostaje "koordynatorem" grupy nodow (co sie dzieje jak najstarszy odpada ?!?!?!?!!?!)
//	    channel.getState(null, 10_000); // uzyskaj stan po dolaczeniu do grupy (najczesciej od najstarszego klienta)
		
		
	    @SuppressWarnings("resource")
		Scanner reader = new Scanner(System.in);  // Reading from System.in
	    while(true) {
	    	String str = reader.nextLine();
	    	switch(str) {
	    		case "count":
	    			System.out.println(wordCounter);
	    			break;
	    		default:
	    	    	printOut(str);
	    	    	break;
	    	}
	    }
	}
	
	public Integer printOut(String text) {
		System.out.println("new message: " + text);
		Integer valueFromCache = wordCounter.get(text);
		int currentCount = valueFromCache == null ? 0 : valueFromCache;
		wordCounter.put(text, ++currentCount);
		return 10;
	}
}
