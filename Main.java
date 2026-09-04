
import java.util.concurrent.CompletableFuture;


public class Main{
    public static void main(String []args){
        SchedulerStrategy onceStrategy = new OnceSchedulerStrategy();
        SchedulerStrategy recurringStrategy = new RecurringSchedulerStrategy();
        SchedulingEngine schedulingEngine = new SchedulingEngine(10);

        schedulingEngine.start();

        Task task1 = new Task(2000, 0, ()->{
            return true;
        }, onceStrategy);

        Task task2 = new Task(2000, 5000, ()->{
            return false;
        }, recurringStrategy);

        CompletableFuture<Boolean> future = schedulingEngine.submit(task1);

        future.thenApply(result->{
            System.out.println("Success!");
        }).exceptionally(e->{
            System.out.printerr(e.getMessage());
        });
    }
}