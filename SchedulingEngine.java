
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


public class SchedulingEngine {
    private final PriorityQueue<Task<?>> taskQueue;
    private final ReentrantLock lock;
    private final Condition emptyCondition;
    private final Condition waitCondition;
    private final ExecutorService executor;
    private final int MAX_THREADS;
    private final ThreadFactory threadFactory;
    private Thread executorThread;
    private boolean isRunning = false;

    public SchedulingEngine(int maxThreads){
        this.taskQueue = new PriorityQueue<>((Task a, Task b)->Long.compare(a.getNextExecutionTime(), b.getNextExecutionTime()));
        this.lock = new ReentrantLock();
        this.emptyCondition = lock.newCondition();
        this.waitCondition = lock.newCondition();
        this.threadFactory = runnable->{
            Thread t = new Thread(runnable);
            t.setName("TaskScheduler-Worker-" + t.getName());
            return t;
        };
        this.MAX_THREADS = maxThreads;
        this.executor = Executors.newFixedThreadPool(MAX_THREADS, threadFactory);
    }

    public <R> CompletableFuture<R> submit(Task<R> task){
        lock.lock();

        try {
            long currentNextExecutionTime = !taskQueue.isEmpty()?taskQueue.peek().getNextExecutionTime():Long.MAX_VALUE;
            taskQueue.offer(task);

            if (taskQueue.size() == 1) {
                emptyCondition.signal();
            } else if (task.getNextExecutionTime() < currentNextExecutionTime) {
                waitCondition.signal();
            }

            return task.getFuture();
        } catch (Exception e) {
            // To be handled 
        }finally{
            lock.unlock();
        }

        return null;
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

                    while (taskQueue.peek().getNextExecutionTime() > System.nanoTime()) {
                        waitCondition.await(Math.max(0l, taskQueue.peek().getNextExecutionTime() - System.nanoTime()), TimeUnit.NANOSECONDS);
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