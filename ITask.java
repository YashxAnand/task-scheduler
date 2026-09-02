
interface ITask extends Runnable {
    String getTaskId();
    long getNextExecutionTime();
}