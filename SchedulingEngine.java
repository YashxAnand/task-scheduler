
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;


public class SchedulingEngine {
    private final PriorityQueue<ITask> taskQueue;
    private final ReentrantLock lock;
    private final Set<ITask> cancelledTasks;
    private final ExecutorService executor;
    private int MAX_THREADS;

    public SchedulingEngine(int maxThreads){
        this.taskQueue = new PriorityQueue<>((ITask a, ITask b)->Long.compare(a.getNextExecutionTime(), b.getNextExecutionTime()));
        this.lock = new ReentrantLock();
        this.cancelledTasks = new HashSet<>();
        this.MAX_THREADS = maxThreads;
        this.executor = Executors.newFixedThreadPool(MAX_THREADS);
    }
}