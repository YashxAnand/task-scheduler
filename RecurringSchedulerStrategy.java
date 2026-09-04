
public class RecurringSchedulerStrategy implements SchedulerStrategy{
    @Override
    public long getNextExecutionTime(long interval){
        return System.nanoTime() + (interval * 1000);
    }
}