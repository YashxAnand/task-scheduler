
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.concurrent.TimeUnit;

public class Task<T> implements Runnable{
    private SchedulerStrategy strategy;
    private final String taskId;
    private long initialDelay;
    private long intervalM;
    private long nextExecution;
    private final Supplier<T> function;
    private volatile boolean cancelled;
    private CompletableFuture<T> future;

    public Task(long initialDelay, long intervalM, Supplier<T> function, SchedulerStrategy strategy){
        this.taskId = UUID.randomUUID().toString();
        this.initialDelay = initialDelay;
        this.intervalM = intervalM;
        this.nextExecution = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(initialDelay);
        this.function = function;
        this.strategy = strategy;
        this.cancelled = false;
        this.future = new CompletableFuture<>();
    }

    @Override
    public void run(){
        try {
            T result = function.get();
            System.out.printf("Execution of task : %s completed. Result: %s\n", taskId, result.toString());

            this.nextExecution = strategy.getNextExecutionTime(intervalM);
            this.future.complete(result);
        } catch (Exception e) {
            this.future.completeExceptionally(e);
        }
    }

    public CompletableFuture<T> getFuture(){return this.future;}

    public String getTaskId(){return this.taskId;}

    public long getNextExecutionTime(){return this.nextExecution;}

    public void cancel(){this.cancelled = true;}

    public boolean isCancelled(){return this.cancelled;}
}