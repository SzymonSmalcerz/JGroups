package Sockets.jGroupsHomework.callableFuture;
//Java program to illustrate Callable and FutureTask
	// for random number generation
	import java.util.Random;
	import java.util.concurrent.Callable;
	import java.util.concurrent.FutureTask;
public class CallableExample implements  Callable<Integer>{
 
	public Integer call() throws Exception
	  {
	    Random generator = new Random();
	    Integer randomNumber = generator.nextInt(1000);
	 
	    Thread.sleep(randomNumber * 10);
	 
	    return randomNumber;
	  }
	
	 public static void main(String[] args) throws Exception
	  {
	 
	    // FutureTask is a concrete class that
	    // implements both Runnable and Future
	    @SuppressWarnings("unchecked")
		FutureTask<Integer>[] randomNumberTasks = new FutureTask[5];
	 
	    for (int i = 0; i < 5; i++)
	    {
	      Callable<Integer> callable = new CallableExample();
	 
	      // Create the FutureTask with Callable
	      randomNumberTasks[i] = new FutureTask<Integer>(callable);
	 
	      // As it implements Runnable, create Thread
	      // with FutureTask
	      Thread t = new Thread(randomNumberTasks[i]);
	      t.start();
	    }
	 
	    for (int i = 0; i < 5; i++)
	    {
	      // As it implements Future, we can call get()
	      System.out.println(randomNumberTasks[i].get());
	 
	      // This method blocks till the result is obtained
	      // The get method can throw checked exceptions
	      // like when it is interrupted. This is the reason
	      // for adding the throws clause to main
	    }
	  }

}
