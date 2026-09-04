
interface SchedulerStrategy {
    long getNextExecutionTime(long interval);
}