package ds.tutorials.sycnhronization;

public interface DistributedTxListener {
    void onGlobalCommit();
    void onGlobalAbort();
} 