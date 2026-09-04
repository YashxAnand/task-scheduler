
import java.util.UUID;
import java.util.function.Supplier;


public class Task<T> implements Runnable{
    private SchedulerStrategy strategy;
    private final String taskId;
    private long initialDelay;
    private long intervalM;
    private long nextExecution;
    private final Supplier<T> function;

    public Task(long initialDelay, long intervalM, Supplier<T> function, SchedulerStrategy strategy){
        this.taskId = UUID.randomUUID().toString();
        this.initialDelay = initialDelay;
        this.intervalM = intervalM;
        this.nextExecution = System.currentTimeMillis() + initialDelay;
        this.function = function;
        this.strategy = strategy;
    }

    @Override
    public void run(){
        try {
            T result = function.get();
            System.out.printf("Execution of task : %s completed. Result: %s\n", taskId, result.toString());

            this.nextExecution = strategy.getNextExecutionTime(intervalM);
        } catch (Exception e) {
        }
    }

    public String getTaskId(){return this.taskId;}

    public long getNextExecutionTime(){return this.nextExecution;}
}