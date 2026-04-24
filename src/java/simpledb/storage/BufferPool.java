package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private final int numPages;
    private final ConcurrentHashMap<PageId, Page> pages;
    private final LinkedList<PageId> lruOrder;
    private final LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        this.pages = new ConcurrentHashMap<>();
        this.lruOrder = new LinkedList<>();
        this.lockManager = new LockManager();
    }

    public static int getPageSize() {
      return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        lockManager.acquire(tid, pid, perm);

        synchronized (this) {
            Page cached = pages.get(pid);
            if (cached != null) {
                touchPage(pid);
                return cached;
            }
            if (pages.size() >= numPages) {
                evictPage();
            }
            DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = file.readPage(pid);
            cachePage(page);
            return page;
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        lockManager.release(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        try {
            if (commit) {
                flushPages(tid);
            } else {
                restorePages(tid);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            lockManager.releaseAll(tid);
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> modified = file.insertTuple(tid, t);
        synchronized (this) {
            for (Page p : modified) {
                p.markDirty(true, tid);
                cachePage(p);
            }
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> modified = file.deleteTuple(tid, t);
        synchronized (this) {
            for (Page p : modified) {
                p.markDirty(true, tid);
                cachePage(p);
            }
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId pid : new ArrayList<>(pages.keySet())) {
            flushPage(pid);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.

        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        pages.remove(pid);
        lruOrder.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        Page page = pages.get(pid);
        if (page == null) return;
        if (page.isDirty() != null) {
            DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
            file.writePage(page);
            page.markDirty(false, null);
            page.setBeforeImage();
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        for (PageId pid : new ArrayList<>(pages.keySet())) {
            Page page = pages.get(pid);
            if (page != null && tid.equals(page.isDirty())) {
                flushPage(pid);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        Iterator<PageId> iterator = lruOrder.iterator();
        while (iterator.hasNext()) {
            PageId pid = iterator.next();
            Page page = pages.get(pid);
            if (page.isDirty() == null) {
                pages.remove(pid);
                iterator.remove();
                return;
            }
        }
        throw new DbException("All pages in buffer pool are dirty, cannot evict");
    }

    private synchronized void cachePage(Page page) {
        pages.put(page.getId(), page);
        touchPage(page.getId());
    }

    private synchronized void touchPage(PageId pid) {
        lruOrder.remove(pid);
        lruOrder.addLast(pid);
    }

    private synchronized void restorePages(TransactionId tid) {
        Set<PageId> pagesToRestore = new HashSet<>(lockManager.getPagesHeld(tid));
        for (Map.Entry<PageId, Page> entry : pages.entrySet()) {
            if (tid.equals(entry.getValue().isDirty())) {
                pagesToRestore.add(entry.getKey());
            }
        }

        for (PageId pid : pagesToRestore) {
            if (!pages.containsKey(pid)) {
                continue;
            }
            DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page cleanPage = file.readPage(pid);
            pages.put(pid, cleanPage);
            touchPage(pid);
        }
    }

    private static class LockState {
        private TransactionId exclusiveHolder;
        private final Set<TransactionId> sharedHolders = new HashSet<>();
    }

    private static class LockManager {
        private final Map<PageId, LockState> pageLocks = new HashMap<>();
        private final Map<TransactionId, Set<PageId>> transactionLocks = new HashMap<>();
        private final Map<TransactionId, Set<TransactionId>> waitsFor = new HashMap<>();

        public synchronized void acquire(TransactionId tid, PageId pid, Permissions perm)
                throws TransactionAbortedException {
            while (true) {
                if (canGrant(tid, pid, perm)) {
                    grant(tid, pid, perm);
                    clearOutgoingWaits(tid);
                    return;
                }

                Set<TransactionId> blockers = getBlockers(tid, pid, perm);
                waitsFor.put(tid, blockers);
                if (hasCycle(tid)) {
                    clearOutgoingWaits(tid);
                    throw new TransactionAbortedException();
                }

                try {
                    wait();
                } catch (InterruptedException e) {
                    clearOutgoingWaits(tid);
                    Thread.currentThread().interrupt();
                    throw new TransactionAbortedException();
                }
                clearOutgoingWaits(tid);
            }
        }

        public synchronized void release(TransactionId tid, PageId pid) {
            LockState state = pageLocks.get(pid);
            if (state == null) {
                return;
            }

            boolean changed = false;
            if (tid.equals(state.exclusiveHolder)) {
                state.exclusiveHolder = null;
                changed = true;
            }
            if (state.sharedHolders.remove(tid)) {
                changed = true;
            }
            if (!changed) {
                return;
            }

            Set<PageId> heldPages = transactionLocks.get(tid);
            if (heldPages != null) {
                heldPages.remove(pid);
                if (heldPages.isEmpty()) {
                    transactionLocks.remove(tid);
                }
            }
            if (state.exclusiveHolder == null && state.sharedHolders.isEmpty()) {
                pageLocks.remove(pid);
            }
            notifyAll();
        }

        public synchronized void releaseAll(TransactionId tid) {
            Set<PageId> heldPages = transactionLocks.remove(tid);
            if (heldPages != null) {
                for (PageId pid : new HashSet<>(heldPages)) {
                    LockState state = pageLocks.get(pid);
                    if (state == null) {
                        continue;
                    }
                    if (tid.equals(state.exclusiveHolder)) {
                        state.exclusiveHolder = null;
                    }
                    state.sharedHolders.remove(tid);
                    if (state.exclusiveHolder == null && state.sharedHolders.isEmpty()) {
                        pageLocks.remove(pid);
                    }
                }
            }
            clearAllWaits(tid);
            notifyAll();
        }

        public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
            LockState state = pageLocks.get(pid);
            if (state == null) {
                return false;
            }
            return tid.equals(state.exclusiveHolder) || state.sharedHolders.contains(tid);
        }

        public synchronized Set<PageId> getPagesHeld(TransactionId tid) {
            Set<PageId> heldPages = transactionLocks.get(tid);
            if (heldPages == null) {
                return Collections.emptySet();
            }
            return new HashSet<>(heldPages);
        }

        private boolean canGrant(TransactionId tid, PageId pid, Permissions perm) {
            LockState state = pageLocks.get(pid);
            if (state == null) {
                return true;
            }

            if (perm == Permissions.READ_ONLY) {
                return state.exclusiveHolder == null || tid.equals(state.exclusiveHolder);
            }

            if (tid.equals(state.exclusiveHolder)) {
                return true;
            }

            if (state.exclusiveHolder != null) {
                return false;
            }

            if (state.sharedHolders.isEmpty()) {
                return true;
            }

            return state.sharedHolders.size() == 1 && state.sharedHolders.contains(tid);
        }

        private void grant(TransactionId tid, PageId pid, Permissions perm) {
            LockState state = pageLocks.computeIfAbsent(pid, ignored -> new LockState());
            transactionLocks.computeIfAbsent(tid, ignored -> new HashSet<>()).add(pid);

            if (perm == Permissions.READ_ONLY) {
                if (!tid.equals(state.exclusiveHolder)) {
                    state.sharedHolders.add(tid);
                }
                return;
            }

            state.sharedHolders.remove(tid);
            state.exclusiveHolder = tid;
        }

        private Set<TransactionId> getBlockers(TransactionId tid, PageId pid, Permissions perm) {
            Set<TransactionId> blockers = new HashSet<>();
            LockState state = pageLocks.get(pid);
            if (state == null) {
                return blockers;
            }

            if (perm == Permissions.READ_ONLY) {
                if (state.exclusiveHolder != null && !tid.equals(state.exclusiveHolder)) {
                    blockers.add(state.exclusiveHolder);
                }
                return blockers;
            }

            if (state.exclusiveHolder != null && !tid.equals(state.exclusiveHolder)) {
                blockers.add(state.exclusiveHolder);
            }
            for (TransactionId holder : state.sharedHolders) {
                if (!tid.equals(holder)) {
                    blockers.add(holder);
                }
            }
            return blockers;
        }

        private boolean hasCycle(TransactionId startTid) {
            Deque<TransactionId> stack = new ArrayDeque<>();
            Set<TransactionId> visited = new HashSet<>();
            stack.push(startTid);

            while (!stack.isEmpty()) {
                TransactionId current = stack.pop();
                Set<TransactionId> neighbors = waitsFor.get(current);
                if (neighbors == null) {
                    continue;
                }
                for (TransactionId neighbor : neighbors) {
                    if (startTid.equals(neighbor)) {
                        return true;
                    }
                    if (visited.add(neighbor)) {
                        stack.push(neighbor);
                    }
                }
            }
            return false;
        }

        private void clearOutgoingWaits(TransactionId tid) {
            waitsFor.remove(tid);
        }

        private void clearAllWaits(TransactionId tid) {
            waitsFor.remove(tid);
            for (Set<TransactionId> blockers : waitsFor.values()) {
                blockers.remove(tid);
            }
        }
    }

}
