
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


public class SchedulingEngine {
    private final PriorityQueue<ITask> taskQueue;
    private final ReentrantLock lock;
    private final ReentrantLock cancelSetLock;
    private final Condition emptyCondition;
    private final Condition waitCondition;
    private final Set<String> cancelledTasks;
    private final ExecutorService executor;
    private int MAX_THREADS;

    public SchedulingEngine(int maxThreads){
        this.taskQueue = new PriorityQueue<>((ITask a, ITask b)->Long.compare(a.getNextExecutionTime(), b.getNextExecutionTime()));
        this.lock = new ReentrantLock();
        this.emptyCondition = lock.newCondition();
        this.waitCondition = lock.newCondition();
        this.cancelledTasks = new HashSet<>();
        this.cancelSetLock = new ReentrantLock();
        this.MAX_THREADS = maxThreads;
        this.executor = Executors.newFixedThreadPool(MAX_THREADS);
    }

    public void submit(ITask task){
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

    public void cancel(String taskId){
        cancelSetLock.lock();

        try {
            cancelledTasks.add(taskId);
        } catch (Exception e) {
            // TBD
        }finally{
            cancelSetLock.unlock();
        }
    }

    public void start(){
        new Thread(() -> {
            while (true) {
                lock.lock();
                try {
                    while (taskQueue.isEmpty()) {
                        emptyCondition.await();
                    }

                    long nextTaskExecutionTime = taskQueue.peek().getNextExecutionTime();

                    while (taskQueue.peek().getNextExecutionTime() > System.currentTimeMillis()) {
                        waitCondition.await(Math.max(0l, nextTaskExecutionTime), TimeUnit.MILLISECONDS);
                    }

                    ITask taskToExecute = taskQueue.poll();
                    
                    cancelSetLock.lock();

                    try{
                        if(!cancelledTasks.contains(taskToExecute.getTaskId())){
                            executor.execute(taskToExecute);

                            long nextExecutionTime = taskToExecute.getNextExecutionTime();

                            if(nextExecutionTime > 0l)
                                taskQueue.offer(taskToExecute);
                        }else{
                            cancelledTasks.remove(taskToExecute.getTaskId());
                        }
                    }catch(Exception e){
                        taskQueue.offer(taskToExecute);
                    }finally{
                        cancelSetLock.unlock();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } finally {
                    lock.unlock();
                }
            }
        }).start();

        executor.shutdown();
    }
}