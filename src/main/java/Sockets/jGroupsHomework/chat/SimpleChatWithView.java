package Sockets.jGroupsHomework.chat;

import java.net.InetAddress;
import java.util.Scanner;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
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

public class SimpleChatWithView {
	
	
	private static final String CLASTER_NAME = "ChatCluster";
	private JChannel channel;
	public SimpleChatWithView() {
		// TODO Auto-generated constructor stub
	}
	
	public static void main(String[] args) {
		try {
			new SimpleChatWithView().runClient();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
		
	    channel.setReceiver(new ReceiverAdapter() {
	    	@Override
	    	public void receive(Message msg) {
	    		System.out.println(msg.src() + ":" + msg.dest() + "  |  " + msg.getObject());
	    	}
	    	
	    	@Override
	    	public void viewAccepted(View view) {
	    		System.out.println("View : " + view);
	    	}
	    }); // ustaw odbiorce naszego kanalu jako nasza klase
		channel.connect(CLASTER_NAME); // polacz sie z grupa 
	    channel.getState(null, 0); // uzyskaj stan po dolaczeniu do grupy (0 czyli czekaj az uzyskasz) (najczesciej od najstarszego klienta)
	    
	    @SuppressWarnings("resource")
		Scanner reader = new Scanner(System.in);  // Reading from System.in
	    while(true) {
	    	String str = reader.nextLine();
	    	Message msg = new Message(null,null,str);
	    	channel.send(msg);
	    }
	}
}
