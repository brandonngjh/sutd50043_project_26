package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc tupleDesc;
    private final int tableId;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tupleDesc = td;
        this.tableId = f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return tableId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int pageNumber = pid.getPageNumber();
        if (pageNumber < 0 || pageNumber >= numPages()) {
            throw new IllegalArgumentException("Page does not exist");
        }

        int pageSize = BufferPool.getPageSize();
        byte[] pageData = new byte[pageSize];

        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            raf.seek((long) pageNumber * pageSize);
            raf.readFully(pageData);
            return new HeapPage((HeapPageId) pid, pageData);
        } catch (IOException e) {
            throw new IllegalArgumentException("Unable to read page", e);
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) (file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new DbFileIterator() {
            private int currentPageNo;
            private Iterator<Tuple> currentTupleIterator;
            private boolean opened;

            private Iterator<Tuple> getTupleIteratorForPage(int pageNo)
                    throws DbException, TransactionAbortedException {
                if (pageNo < 0 || pageNo >= numPages()) {
                    return null;
                }

                HeapPageId heapPageId = new HeapPageId(getId(), pageNo);
                HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(
                        tid,
                        heapPageId,
                        Permissions.READ_ONLY);
                return heapPage.iterator();
            }

            @Override
            public void open() throws DbException, TransactionAbortedException {
                opened = true;
                currentPageNo = 0;
                currentTupleIterator = getTupleIteratorForPage(currentPageNo);
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (!opened || currentTupleIterator == null) {
                    return false;
                }

                while (!currentTupleIterator.hasNext()) {
                    currentPageNo++;
                    if (currentPageNo >= numPages()) {
                        return false;
                    }
                    currentTupleIterator = getTupleIteratorForPage(currentPageNo);
                }
                return true;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return currentTupleIterator.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }

            @Override
            public void close() {
                opened = false;
                currentPageNo = 0;
                currentTupleIterator = null;
            }
        };
    }

}
