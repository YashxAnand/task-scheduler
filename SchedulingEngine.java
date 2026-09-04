
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


public class SchedulingEngine {
    private final PriorityQueue<Task> taskQueue;
    private final ReentrantLock lock;
    private final Condition emptyCondition;
    private final Condition waitCondition;
    private final ExecutorService executor;
    private final int MAX_THREADS;
    private Thread executorThread;
    private boolean isRunning = false;

    public SchedulingEngine(int maxThreads){
        this.taskQueue = new PriorityQueue<>((Task a, Task b)->Long.compare(a.getNextExecutionTime(), b.getNextExecutionTime()));
        this.lock = new ReentrantLock();
        this.emptyCondition = lock.newCondition();
        this.waitCondition = lock.newCondition();
        this.MAX_THREADS = maxThreads;
        this.executor = Executors.newFixedThreadPool(MAX_THREADS);
    }

    public void submit(Task task){
        lock.lock();

        try {
            long currentNextExecutionTime = !taskQueue.isEmpty()?taskQueue.peek().getNextExecutionTime():Long.MAX_VALUE;
            taskQueue.offer(task);

            if (taskQueue.size() == 1) {
                emptyCondition.signal();
            } else if (task.getNextExecutionTime() < currentNextExecutionTime) {
                waitCondition.signal();
            }
        } catch (Exception e) {
            // To be handled 
        }finally{
            lock.unlock();
        }
    }

    public void start(){
        this.isRunning = true;
        this.executorThread = new Thread(() -> {
            while (isRunning && !Thread.currentThread().isInterrupted()) {
                lock.lock();
                try {
                    while (taskQueue.isEmpty()) {
                        emptyCondition.await();
                    }

                    while (taskQueue.peek().getNextExecutionTime() > System.currentTimeMillis()) {
                        waitCondition.await(Math.max(0l, taskQueue.peek().getNextExecutionTime() - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
                    }

                    Task taskToExecute = taskQueue.poll();
                    
                    if(!taskToExecute.isCancelled()){
                        executor.execute(()->{
                            taskToExecute.run();
                            long nextExecutionTime = taskToExecute.getNextExecutionTime();

                            if(nextExecutionTime > 0l)
                                submit(taskToExecute);
                        });

                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } finally {
                    lock.unlock();
                }
            }
        });
        
        this.executorThread.start();
    }

    public void stop(){
        if(this.executorThread!=null)
            this.executorThread.interrupt();

        executor.shutdown();

        try {
            if(!executor.awaitTermination(60, TimeUnit.SECONDS)){
                executor.shutdownNow();

                if(!executor.awaitTermination(10, TimeUnit.SECONDS)){
                    System.err.println("Executor did not terminate cleanly.");
                }
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();

            Thread.currentThread().interrupt();
        }
    }
}