
import java.util.UUID;
import java.util.function.Supplier;


public class OnceTask<T> implements ITask {
    private final String taskId;
    private long initialDelay;
    private long nextExecution;
    private final Supplier<T> function;

    public OnceTask(long initialDelay, Supplier<T> function){
        this.taskId = UUID.randomUUID().toString();
        this.initialDelay = initialDelay;
        this.nextExecution = System.currentTimeMillis() + initialDelay;
        this.function = function;
    }

    @Override
    public void run(){
        try {
            T result = function.get();
            System.out.printf("Execution of task : %s completed. Result: %s\n", taskId, result.toString());

            this.nextExecution = 0l;
        } catch (Exception e) {
        }
    }

    @Override
    public String getTaskId(){return this.taskId;}

    @Override
    public long getNextExecutionTime(){return this.nextExecution;}
}