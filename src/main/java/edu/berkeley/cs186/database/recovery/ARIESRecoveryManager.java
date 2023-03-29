package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.recovery.records.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Implementation of ARIES.
 */
public class ARIESRecoveryManager implements RecoveryManager {
    // Disk space manager.
    DiskSpaceManager diskSpaceManager;
    // Buffer manager.
    BufferManager bufferManager;

    // Function to create a new transaction for recovery with a given
    // transaction number.
    private Function<Long, Transaction> newTransaction;

    // Log manager
    LogManager logManager;
    // Dirty page table (page number -> recLSN).
    Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
    // Transaction table (transaction number -> entry).
    Map<Long, TransactionTableEntry> transactionTable = new ConcurrentHashMap<>();
    // true if redo phase of restart has terminated, false otherwise. Used
    // to prevent DPT entries from being flushed during restartRedo.
    boolean redoComplete;

    public ARIESRecoveryManager(Function<Long, Transaction> newTransaction) {
        this.newTransaction = newTransaction;
    }

    /**
     * Initializes the log; only called the first time the database is set up.
     * The master record should be added to the log, and a checkpoint should be
     * taken.
     */
    @Override
    public void initialize() {
        this.logManager.appendToLog(new MasterLogRecord(0));
        this.checkpoint();
    }

    /**
     * Sets the buffer/disk managers. This is not part of the constructor
     * because of the cyclic dependency between the buffer manager and recovery
     * manager (the buffer manager must interface with the recovery manager to
     * block page evictions until the log has been flushed, but the recovery
     * manager needs to interface with the buffer manager to write the log and
     * redo changes).
     * @param diskSpaceManager disk space manager
     * @param bufferManager buffer manager
     */
    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
        this.diskSpaceManager = diskSpaceManager;
        this.bufferManager = bufferManager;
        this.logManager = new LogManager(bufferManager);
    }

    // Forward Processing //////////////////////////////////////////////////////

    /**
     * Called when a new transaction is started.
     *
     * The transaction should be added to the transaction table.
     *
     * @param transaction new transaction
     */
    @Override
    public synchronized void startTransaction(Transaction transaction) {
        this.transactionTable.put(transaction.getTransNum(), new TransactionTableEntry(transaction));
    }

    /**
     * Called when a transaction is about to start committing.
     *
     * A commit record should be appended, the log should be flushed,
     * and the transaction table and the transaction status should be updated.
     *
     * @param transNum transaction being committed
     * @return LSN of the commit record
     */
    @Override
    public long commit(long transNum) {
        // TODO(proj5): implement




        // append commit record
        TransactionTableEntry transactionEntry = new TransactionTableEntry(transactionTable.get(transNum).transaction);

        // long prevLSN = lastRecord.getLSN();
        CommitTransactionLogRecord commitRecord = new CommitTransactionLogRecord(transNum, transactionEntry.lastLSN);
        Long commitRecLSN = logManager.appendToLog(commitRecord);

        // flush log
        flushToLSN(commitRecLSN);


        // update transaction table and transaction status
        transactionTable.get(transNum).lastLSN = commitRecLSN;
        transactionEntry.transaction.setStatus(Transaction.Status.COMMITTING);




        return commitRecLSN;

        //return -1L;
    }

    /**
     * Called when a transaction is set to be aborted.
     *
     * An abort record should be appended, and the transaction table and
     * transaction status should be updated. Calling this function should not
     * perform any rollbacks.
     *
     * @param transNum transaction being aborted
     * @return LSN of the abort record
     */
    @Override
    public long abort(long transNum) {
        // TODO(proj5): implement
        // find LSN of abort record
        // Transaction table's lastLSN for T1
        TransactionTableEntry transactionTableEntry = transactionTable.get(transNum);

        // append abort record
        AbortTransactionLogRecord abortLogRec = new AbortTransactionLogRecord(transNum, transactionTableEntry.lastLSN);
        transactionTable.get(transNum).lastLSN = logManager.appendToLog(abortLogRec);


        // update transaction table and change transaction state to abort
        transactionTableEntry.transaction.setStatus(Transaction.Status.ABORTING);


        // from transactionTableEntry, find lastLSN for T1
        return transactionTable.get(transNum).lastLSN;
        //return -1L;
    }

    /**
     * Called when a transaction is cleaning up; this should roll back
     * changes if the transaction is aborting (see the rollbackToLSN helper
     * function below).
     *
     * Any changes that need to be undone should be undone, the transaction should
     * be removed from the transaction table, the end record should be appended,
     * and the transaction status should be updated.
     *
     * @param transNum transaction to end
     * @return LSN of the end record
     */
    @Override
    public long end(long transNum) {
        // TODO(proj5): implement
        // if transaction is aborting
        long newLSN = 0;
        Transaction trans = transactionTable.get(transNum).transaction;
        if (trans.getStatus().equals(Transaction.Status.ABORTING)) {

            // get first lsn to undo of transaction
            rollbackToLSN(transNum, 0);
        }

        newLSN = transactionTable.get(transNum).lastLSN;

        LogRecord endTransactionLogRecord = new EndTransactionLogRecord(transNum, transactionTable.get(transNum).lastLSN);

        //remove transaction from transaction table
        transactionTable.remove(transNum);
        trans.setStatus(Transaction.Status.COMPLETE); // update transaction status

        // append end record
        newLSN = logManager.appendToLog(endTransactionLogRecord);

        return newLSN;
    }

    /**
     * Recommended helper function: performs a rollback of all of a
     * transaction's actions, up to (but not including) a certain LSN.
     * Starting with the LSN of the most recent record that hasn't been undone:
     * - while the current LSN is greater than the LSN we're rolling back to:
     *    - if the record at the current LSN is undoable:
     *       - Get a compensation log record (CLR) by calling undo on the record
     *       - Append the CLR
     *       - Call redo on the CLR to perform the undo
     *    - update the current LSN to that of the next record to undo
     *
     * Note above that calling .undo() on a record does not perform the undo, it
     * just creates the compensation log record.
     *
     * @param transNum transaction to perform a rollback for
     * @param LSN LSN to which we should rollback
     */
    private void rollbackToLSN(long transNum, long LSN) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        LogRecord lastRecord = logManager.fetchLogRecord(transactionEntry.lastLSN);
        long lastRecordLSN = lastRecord.getLSN();
        // Small optimization: if the last record is a CLR we can start rolling
        // back from the next record that hasn't yet been undone.
        long currentLSN = lastRecord.getUndoNextLSN().orElse(lastRecordLSN);
        // TODO(proj5) implement the rollback logic described above

        /*
         * Starting with the LSN of the most recent record that hasn't been undone:
         * - while the current LSN is greater than the LSN we're rolling back to:
         *    - if the record at the current LSN is undoable:
         *       - Get a compensation log record (CLR) by calling undo on the record
         *       - Append the CLR
         *       - Call redo on the CLR to perform the undo
         *    - update the current LSN to that of the next record to undo
         **/
        //LogRecord currentRecord = logManager.fetchLogRecord(currentLSN);


        LogRecord allocCLR;
        LogRecord updateCLR;


        updateCLR = lastRecord; // currentRecord
        long newLSN = 0;

        ARIESRecoveryManager recoveryManager = this;
        long allocCLRLSN = 0;

        while (currentLSN > LSN) {
            if (lastRecord.isUndoable()) {
                allocCLR = lastRecord.undo(lastRecordLSN); // alloc CRL
                allocCLRLSN = logManager.appendToLog(allocCLR);
                allocCLR.redo(recoveryManager, diskSpaceManager, bufferManager);


            }

            lastRecord = logManager.fetchLogRecord(currentLSN);
            currentLSN = lastRecord.getPrevLSN().get(); //currentRecord.getUndoNextLSN().orElse(currentRecord.getPrevLSN().get());

        }

        if (lastRecord.getType().equals(LogType.UPDATE_PAGE)) {
            updateCLR = lastRecord.undo(allocCLRLSN);
            long updateCLRLSN = logManager.appendToLog(updateCLR);
            updateCLR.redo(recoveryManager, diskSpaceManager, bufferManager);
            transactionTable.get(transNum).lastLSN = (updateCLRLSN > allocCLRLSN ? updateCLRLSN : allocCLRLSN);

        }


    }

    /**
     * Called before a page is flushed from the buffer cache. This
     * method is never called on a log page.
     *
     * The log should be as far as necessary.
     *
     * @param pageLSN pageLSN of page about to be flushed
     */
    @Override
    public void pageFlushHook(long pageLSN) {
        logManager.flushToLSN(pageLSN);
    }

    /**
     * Called when a page has been updated on disk.
     *
     * As the page is no longer dirty, it should be removed from the
     * dirty page table.
     *
     * @param pageNum page number of page updated on disk
     */
    @Override
    public void diskIOHook(long pageNum) {
        if (redoComplete) dirtyPageTable.remove(pageNum);
    }

    /**
     * Called when a write to a page happens.
     *
     * This method is never called on a log page. Arguments to the before and after params
     * are guaranteed to be the same length.
     *
     * The appropriate log record should be appended, and the transaction table
     * and dirty page table should be updated accordingly.
     *
     * @param transNum transaction performing the write
     * @param pageNum page number of page being written
     * @param pageOffset offset into page where write begins
     * @param before bytes starting at pageOffset before the write
     * @param after bytes starting at pageOffset after the write
     * @return LSN of last record written to log
     */
    @Override
    public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                             byte[] after) {
        assert (before.length == after.length);
        assert (before.length <= BufferManager.EFFECTIVE_PAGE_SIZE / 2);
        // TODO(proj5): implement


        long currPageOffset = 0;
        // append appropriate log
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        Transaction transaction = transactionEntry.transaction;
        long lastLSN = transactionEntry.lastLSN;
        UpdatePageLogRecord updateLogRec = new UpdatePageLogRecord(transNum, pageNum, lastLSN, pageOffset, before, after);
        //AllocPageLogRecord allocPageLogRecord = new AllocPageLogRecord(transNum, pageNum, lastLSN);
        //long allocPageLSN = logManager.appendToLog(allocPageLogRecord);
        long updatePageLSN = logManager.appendToLog(updateLogRec);

        //transactionEntry.lastLSN = allocPageLogRecord.getLSN();
        transactionTable.get(transNum).lastLSN = updateLogRec.getLSN();


        // update transaction table
        // if transaction is not in the table, insert it
        if (!(transactionEntry.transaction.getTransNum() == transNum)){
            TransactionTableEntry newTransactionEntry = new TransactionTableEntry(transactionTable.get(transNum).transaction);
            transactionTable.put(transNum, newTransactionEntry);
            //transactionEntry.lastLSN = allocPageLSN;
            transactionTable.get(transNum).lastLSN = updatePageLSN;
        }

        // update data page
        // if page doesn't already exist
        if (dirtyPageTable.get(pageNum)== null) {
            dirtyPageTable.put(pageNum, updatePageLSN);
            //dirtyPageTable.put(pageNum, allocPageLSN);
        }

        // if page already exists
        else{
            dirtyPageTable.put(pageNum, dirtyPageTable.get(pageNum));
        }


        return transactionTable.get(transNum).lastLSN;
    }

    /**
     * Called when a new partition is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param partNum partition number of the new partition
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) return -1L;
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a partition is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the partition be freed
     * @param partNum partition number of the partition being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a new page is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param pageNum page number of the new page
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a page is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the page be freed
     * @param pageNum page number of the page being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        dirtyPageTable.remove(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Creates a savepoint for a transaction. Creating a savepoint with
     * the same name as an existing savepoint for the transaction should
     * delete the old savepoint.
     *
     * The appropriate LSN should be recorded so that a partial rollback
     * is possible later.
     *
     * @param transNum transaction to make savepoint for
     * @param name name of savepoint
     */
    @Override
    public void savepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        transactionEntry.addSavepoint(name);
    }

    /**
     * Releases (deletes) a savepoint for a transaction.
     * @param transNum transaction to delete savepoint for
     * @param name name of savepoint
     */
    @Override
    public void releaseSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        transactionEntry.deleteSavepoint(name);
    }

    /**
     * Rolls back transaction to a savepoint.
     *
     * All changes done by the transaction since the savepoint should be undone,
     * in reverse order, with the appropriate CLRs written to log. The transaction
     * status should remain unchanged.
     *
     * @param transNum transaction to partially rollback
     * @param name name of savepoint
     */
    @Override
    public void rollbackToSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        // All of the transaction's changes strictly after the record at LSN should be undone.
        long savepointLSN = transactionEntry.getSavepoint(name);

        // TODO(proj5) implement the rollback logic described above
        LogRecord lastRecord = logManager.fetchLogRecord(transactionEntry.lastLSN);
        //long lastRecordLSN = lastRecord.getLSN();
        //long currentLSN = lastRecord.getUndoNextLSN().orElse(lastRecordLSN);
        //LogRecord currentRecord = logManager.fetchLogRecord(currentLSN);

        //rollbackToLSN(currentRecord.getTransNum().get(), savepointLSN);
        rollbackToLSN(lastRecord.getTransNum().get(), savepointLSN);

        return;









    }

    /**
     * Create a checkpoint.
     *
     * First, a begin checkpoint record should be written.
     *
     * Then, end checkpoint records should be filled up as much as possible first
     * using recLSNs from the DPT, then status/lastLSNs from the transactions
     * table, and written when full (or when nothing is left to be written).
     * You may find the method EndCheckpointLogRecord#fitsInOneRecord here to
     * figure out when to write an end checkpoint record.
     *
     * Finally, the master record should be rewritten with the LSN of the
     * begin checkpoint record.
     */
    @Override
    public synchronized void checkpoint() {
        // Create begin checkpoint log record and write to log
        LogRecord beginRecord = new BeginCheckpointLogRecord();
        long beginLSN = logManager.appendToLog(beginRecord);
        Map<Long, Long> chkptDPT = new HashMap<>(); // initially empty
        Map<Long, Pair<Transaction.Status, Long>> chkptTxnTable = new HashMap<>(); // initially empty

        // TODO(proj5): generate end checkpoint record(s) for DPT and transaction table

        int DPTsize = 0;//chkptDPT.size();
        int trxnTableSize = 0;//chkptTxnTable.size();

        long endRecLSN=0;

        // check chkptDPT
        for (Map.Entry<Long, Long> DPTEntry: dirtyPageTable.entrySet()){
            if (!EndCheckpointLogRecord.fitsInOneRecord(chkptDPT.size()+1, chkptTxnTable.size())){
                // create EndCheckpointLogRecord
                EndCheckpointLogRecord endRec = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
                endRecLSN = logManager.appendToLog(endRec);
                chkptDPT.clear(); // new stuff
                //break;

            }
            chkptDPT.put(DPTEntry.getKey(),DPTEntry.getValue());
            DPTsize = chkptDPT.size();//+= 1;//
        }


        // check chkptTxnTable
        for (Map.Entry<Long, TransactionTableEntry> trxnEntry: transactionTable.entrySet()){
            // no space to insert trxn values
            if (!EndCheckpointLogRecord.fitsInOneRecord(chkptDPT.size(), chkptTxnTable.size()+1)){
                // create EndCheckpointLogRecord
                EndCheckpointLogRecord endRec = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
                endRecLSN = logManager.appendToLog(endRec);
                chkptTxnTable.clear(); // new stuff
                chkptDPT.clear(); // new

            }

            long transNum = trxnEntry.getKey();
            TransactionTableEntry tableEntry = trxnEntry.getValue();
            Pair<Transaction.Status, Long> trxnValue = new Pair<>(tableEntry.transaction.getStatus(), tableEntry.lastLSN);
            chkptTxnTable.put(transNum, trxnValue);
            trxnTableSize = chkptTxnTable.size();

        }

        // if there are remaining transaction entries left
        // Last end checkpoint record
        LogRecord endRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
        logManager.appendToLog(endRecord);

        // Ensure checkpoint is fully flushed before updating the master record
        flushToLSN(endRecord.getLSN());


        // Update master record
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    /**
     * Flushes the log to at least the specified record,
     * essentially flushing up to and including the page
     * that contains the record specified by the LSN.
     *
     * @param LSN LSN up to which the log should be flushed
     */
    @Override
    public void flushToLSN(long LSN) {
        this.logManager.flushToLSN(LSN);
    }

    @Override
    public void dirtyPage(long pageNum, long LSN) {
        dirtyPageTable.putIfAbsent(pageNum, LSN);
        // Handle race condition where earlier log is beaten to the insertion by
        // a later log.
        dirtyPageTable.computeIfPresent(pageNum, (k, v) -> Math.min(LSN,v));
    }

    @Override
    public void close() {
        this.checkpoint();
        this.logManager.close();
    }

    // Restart Recovery ////////////////////////////////////////////////////////

    /**
     * Called whenever the database starts up, and performs restart recovery.
     * Recovery is complete when the Runnable returned is run to termination.
     * New transactions may be started once this method returns.
     *
     * This should perform the three phases of recovery, and also clean the
     * dirty page table of non-dirty pages (pages that aren't dirty in the
     * buffer manager) between redo and undo, and perform a checkpoint after
     * undo.
     */
    @Override
    public void restart() {
        this.restartAnalysis();
        this.restartRedo();
        this.redoComplete = true;
        this.cleanDPT();
        this.restartUndo();
        this.checkpoint();
    }

    /**
     * This method performs the analysis pass of restart recovery.
     *
     * First, the master record should be read (LSN 0). The master record contains
     * one piece of information: the LSN of the last successful checkpoint.
     *
     * We then begin scanning log records, starting at the beginning of the
     * last successful checkpoint.
     *
     * If the log record is for a transaction operation (getTransNum is present)
     * - update the transaction table
     *
     * If the log record is page-related (getPageNum is present), update the dpt
     *   - update/undoupdate page will dirty pages
     *   - free/undoalloc page always flush changes to disk
     *   - no action needed for alloc/undofree page
     *
     * If the log record is for a change in transaction status:
     * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
     * - update the transaction table
     * - if END_TRANSACTION: clean up transaction (Transaction#cleanup), remove
     *   from txn table, and add to endedTransactions
     *
     * If the log record is an end_checkpoint record:
     * - Copy all entries of checkpoint DPT (replace existing entries if any)
     * - Skip txn table entries for transactions that have already ended
     * - Add to transaction table if not already present
     * - Update lastLSN to be the larger of the existing entry's (if any) and
     *   the checkpoint's
     * - The status's in the transaction table should be updated if it is possible
     *   to transition from the status in the table to the status in the
     *   checkpoint. For example, running -> aborting is a possible transition,
     *   but aborting -> running is not.
     *
     * After all records in the log are processed, for each ttable entry:
     *  - if COMMITTING: clean up the transaction, change status to COMPLETE,
     *    remove from the ttable, and append an end record
     *  - if RUNNING: change status to RECOVERY_ABORTING, and append an abort
     *    record
     *  - if RECOVERY_ABORTING: no action needed
     */
    void restartAnalysis() {
        // Read master record
        LogRecord record = logManager.fetchLogRecord(0L);
        // Type checking
        assert (record != null && record.getType() == LogType.MASTER);
        MasterLogRecord masterRecord = (MasterLogRecord) record;
        // Get start checkpoint LSN
        long LSN = masterRecord.lastCheckpointLSN;
        // Set of transactions that have completed
        Set<Long> endedTransactions = new HashSet<>();
        // TODO(proj5): implement

        /* We then begin scanning log records, starting at the beginning of the
         * last successful checkpoint.*/
        // masterRecord is beginning of last checkpoint
        Iterator<LogRecord> logScanner = logManager.scanFrom(LSN);
        while (logScanner.hasNext()) {
            LogRecord currRecord = logScanner.next();


            // log records for transaction operations
            if (currRecord.getTransNum().isPresent()) {
                if (transactionTable.containsKey(currRecord.getTransNum().get())) {
                    transactionTable.get(currRecord.getTransNum().get()).lastLSN = currRecord.getLSN();
                }
                else{
                    Transaction currNewTrans = newTransaction.apply(currRecord.getTransNum().get());
                    startTransaction(currNewTrans);
                    transactionTable.get(currRecord.getTransNum().get()).lastLSN = currRecord.getLSN();
                }

            }

            // log records for page operations
            if (currRecord.getPageNum().isPresent()) {
                // if update page or undoupdate page
                if (currRecord.getType().equals(LogType.UPDATE_PAGE) || currRecord.getType().equals(LogType.UNDO_UPDATE_PAGE)) {
                    if (!dirtyPageTable.containsKey(currRecord.getPageNum().get())) {
                        // put page in DPT
                        dirtyPageTable.put(currRecord.getPageNum().get(), currRecord.getLSN());
                    }
                }
                // if free page or undoallocpage
                else if (currRecord.getType().equals(LogType.FREE_PAGE) || currRecord.getType().equals(LogType.UNDO_ALLOC_PAGE)) {
                    // remove page from DPT
                    if (dirtyPageTable.containsKey(currRecord.getPageNum().get())) {
                        dirtyPageTable.remove(currRecord.getPageNum().get());
                    }

                }
            }

            // log records for transaction status changes
            if ((currRecord.getType().equals(LogType.COMMIT_TRANSACTION) || currRecord.getType().equals(LogType.ABORT_TRANSACTION) || currRecord.getType().equals(LogType.END_TRANSACTION))) {
                // transaction not in table


                // update status
                if (currRecord.getType().equals(LogType.COMMIT_TRANSACTION)) {
                    transactionTable.get(currRecord.getTransNum().get()).transaction.setStatus(Transaction.Status.COMMITTING);
                } else if (currRecord.getType().equals(LogType.ABORT_TRANSACTION)) {
                    transactionTable.get(currRecord.getTransNum().get()).transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                } else if (currRecord.getType().equals(LogType.END_TRANSACTION)) {
                    // clean up transaction before setting status
                    transactionTable.get(currRecord.getTransNum().get()).transaction.cleanup();
                    transactionTable.get(currRecord.getTransNum().get()).transaction.setStatus(Transaction.Status.COMPLETE);
                    // remove entry from transaction table

                    transactionTable.remove(currRecord.getTransNum().get());

                    // add ended transactions transaction number into endedTransactions
                    endedTransactions.add(currRecord.getTransNum().get());

                }
            }
            // checkpoint records: end checkpoint
            if (currRecord.getType().equals(LogType.END_CHECKPOINT)) {
                // go through checkpoint's DPT
                for (Map.Entry<Long, Long> dptEntry : currRecord.getDirtyPageTable().entrySet()) {
                    // check if dptEntry is in current dirty page table
                    if (dirtyPageTable.size() != 0 && dirtyPageTable.containsKey(dptEntry.getKey())) {
                        // replace dirtyPageTable recLSN w checkpoint recLSN
                        Long recLSN = dirtyPageTable.get(dptEntry.getKey());

                        if (recLSN != dptEntry.getValue()) {
                            // replace
                            dirtyPageTable.remove(dptEntry.getKey()); // remove
                            dirtyPageTable.put(dptEntry.getKey(), dptEntry.getValue()); // replace
                        }
                    } else {
                        // put dptEntry in dirtyPageTAble
                        dirtyPageTable.put(dptEntry.getKey(), dptEntry.getValue());
                    }
                }

                // go through checkpoints transactionTable
                for (Map.Entry<Long, Pair<Transaction.Status, Long>> trxnTable : currRecord.getTransactionTable().entrySet()) {
                    // check if transaction is alreadt in endedTransactions
                    if (endedTransactions.contains(trxnTable.getKey())) {
                        continue; // ignore
                    }
                    // if transactionTable doesnt contain transaction
                    else if (!transactionTable.containsKey(trxnTable.getKey())) {
                        // add transaction
                        // add to table
                        long transNum = trxnTable.getKey();

                        Transaction currNewTrans = newTransaction.apply(transNum);
                        startTransaction(currNewTrans);

                    }
                    // update last lsn of transaction table
                    // check if The lastLSN of a transaction is > than in memeroy lsn
                    long lastLSN = trxnTable.getValue().getSecond();
                    if (lastLSN > transactionTable.get(trxnTable.getKey()).lastLSN) {
                        transactionTable.get(trxnTable.getKey()).lastLSN = trxnTable.getValue().getSecond();
                    }


                    // update transaction statuses
                    Transaction inMemTrxn = transactionTable.get(trxnTable.getKey()).transaction;

                    // In memory status: running
                    if (inMemTrxn.getStatus().equals(Transaction.Status.RUNNING)) {
                        if (!trxnTable.getValue().getFirst().equals(Transaction.Status.RUNNING)) {
                            // trxn has more advaned status. update
                            if (trxnTable.getValue().getFirst().equals(Transaction.Status.ABORTING)) {
                                transactionTable.get(trxnTable.getKey()).transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                            } else
                                transactionTable.get(trxnTable.getKey()).transaction.setStatus(trxnTable.getValue().getFirst());
                        }
                    } else if (inMemTrxn.getStatus().equals(Transaction.Status.COMMITTING)) {
                        if (trxnTable.getValue().getFirst().equals(Transaction.Status.ABORTING)) {
                            transactionTable.get(trxnTable.getKey()).transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                        } else if (trxnTable.getValue().getFirst().equals(Transaction.Status.COMPLETE)) {
                            transactionTable.get(trxnTable.getKey()).transaction.setStatus(Transaction.Status.COMPLETE);
                        }
                    } else if (inMemTrxn.getStatus().equals(Transaction.Status.ABORTING)) {
                        if (trxnTable.getValue().getFirst().equals(Transaction.Status.COMPLETE)) {
                            transactionTable.get(trxnTable.getKey()).transaction.setStatus(Transaction.Status.COMPLETE);
                        }
                    }

                }

            }

        }

        // Ending transactions
        // go thru all transactions
        for (Map.Entry<Long, TransactionTableEntry> trxnTable: transactionTable.entrySet()){

            LogRecord lastRecord = logManager.fetchLogRecord(transactionTable.get(trxnTable.getKey()).lastLSN);
            // committing state
            if (trxnTable.getValue().transaction.getStatus().equals(Transaction.Status.COMMITTING)) {
                // end
                transactionTable.get(trxnTable.getKey()).transaction.cleanup();
                transactionTable.get(trxnTable.getKey()).transaction.setStatus(Transaction.Status.COMPLETE);

                EndTransactionLogRecord endRec = new EndTransactionLogRecord(trxnTable.getKey(), trxnTable.getValue().lastLSN);
                //EndTransactionLogRecord endRec = new EndTransactionLogRecord(trxnTable.getKey(), lastRecord.getPrevLSN().get());
                long lastLSN = logManager.appendToLog(endRec);
                transactionTable.get(trxnTable.getKey()).lastLSN = lastLSN;
                transactionTable.remove(trxnTable.getKey());



            }
            else if (trxnTable.getValue().transaction.getStatus().equals(Transaction.Status.RUNNING)) {
                transactionTable.get(trxnTable.getKey()).transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                AbortTransactionLogRecord abortRec = new AbortTransactionLogRecord(trxnTable.getKey(), trxnTable.getValue().lastLSN);
                //AbortTransactionLogRecord abortRec = new AbortTransactionLogRecord(trxnTable.getKey(), lastRecord.getPrevLSN().get());
                long lastLSN = logManager.appendToLog(abortRec);
                transactionTable.get(trxnTable.getKey()).lastLSN = lastLSN;


            }
        }






        return;
    }

    /**
     * This method performs the redo pass of restart recovery.
     *
     * First, determine the starting point for REDO from the dirty page table.
     *
     * Then, scanning from the starting point, if the record is redoable and
     * - partition-related (Alloc/Free/UndoAlloc/UndoFree..Part), always redo it
     * - allocates a page (AllocPage/UndoFreePage), always redo it
     * - modifies a page (Update/UndoUpdate/Free/UndoAlloc....Page) in
     *   the dirty page table with LSN >= recLSN, the page is fetched from disk,
     *   the pageLSN is checked, and the record is redone if needed.
     */
    void restartRedo() {
        // TODO(proj5): implement
        // go thru the dirtypagetable and find the smallest recLSN (value)
        if (dirtyPageTable.isEmpty()) return;

        long startLSN = 0;
        int count =0;
        for (Map.Entry<Long, Long> dptEntry: dirtyPageTable.entrySet()){
            if (count == 0){
                // set startLSN to first LSN that appears in dpt
                startLSN = dptEntry.getValue();
                count++;
            }

            if (dptEntry.getValue() < startLSN){
                startLSN = dptEntry.getValue();
            }
        }




        Iterator<LogRecord> logScanner = logManager.scanFrom(startLSN);
        ARIESRecoveryManager recoveryManager = this;
        while (logScanner.hasNext()){
            LogRecord currRecord = logScanner.next();

            // redo record if record is redoable
            if (currRecord.isRedoable()){

                if (currRecord.getType().equals(LogType.ALLOC_PART) || currRecord.getType().equals(LogType.UNDO_ALLOC_PART) || currRecord.getType().equals(LogType.FREE_PART) || currRecord.getType().equals(LogType.UNDO_FREE_PART)){
                    // redo record
                    currRecord.redo(recoveryManager, diskSpaceManager, bufferManager);

                }
                else if (currRecord.getType().equals(LogType.ALLOC_PAGE) || currRecord.getType().equals(LogType.UNDO_FREE_PAGE)){
                    // redo record
                    currRecord.redo(recoveryManager, diskSpaceManager, bufferManager);
                }
                else if (currRecord.getType().equals(LogType.UPDATE_PAGE) || currRecord.getType().equals(LogType.UNDO_UPDATE_PAGE) || currRecord.getType().equals(LogType.UNDO_ALLOC_PAGE) || currRecord.getType().equals(LogType.FREE_PAGE)){
                    if (currRecord.getPageNum().isPresent()) {
                        Page page = bufferManager.fetchPage(new DummyLockContext(), currRecord.getPageNum().get());
                        long LSN = currRecord.getLSN();
                        long recLSN = dirtyPageTable.get(page.getPageNum()); // recLSN corresponding to page
                        try {
                            // check more conditions
                            if (dirtyPageTable.containsKey(page.getPageNum()) && LSN >= recLSN && page.getPageLSN() < LSN) {
                                // redo record
                                currRecord.redo(recoveryManager, diskSpaceManager, bufferManager);
                            }
                        } finally {
                            page.unpin();
                        }
                    }
                }

            }

        }


        return;
    }

    /**
     * This method performs the undo pass of restart recovery.

     * First, a priority queue is created sorted on lastLSN of all aborting
     * transactions.
     *
     * Then, always working on the largest LSN in the priority queue until we are done,
     * - if the record is undoable, undo it, and append the appropriate CLR
     * - replace the entry with a new one, using the undoNextLSN if available,
     *   if the prevLSN otherwise.
     * - if the new LSN is 0, clean up the transaction, set the status to complete,
     *   and remove from transaction table.
     */
    void restartUndo() {
        // TODO(proj5): implement
        PriorityQueue<Long> pqLastLSN = new PriorityQueue<>(Collections.reverseOrder());


        // go through transaction table and add all lastLSNs of aborting transactions
        for (Map.Entry<Long, TransactionTableEntry> trxnEntry: transactionTable.entrySet()){
            if (trxnEntry.getValue().transaction.getStatus().equals(Transaction.Status.RECOVERY_ABORTING)){
                pqLastLSN.add(trxnEntry.getValue().lastLSN);

            }
        }

        ARIESRecoveryManager recoveryManager = this;

        long newLSN = -1;
        long currTransNum = 0;
        long newLastLSN = 0;
        while (!pqLastLSN.isEmpty()){

            long currLSN = pqLastLSN.poll(); // current last LSN
            LogRecord currRecord = logManager.fetchLogRecord(currLSN);
            //currTransNum = currRecord.getTransNum().get();

            // undo if unodable
            if (currRecord.isUndoable() && currRecord.getTransNum().isPresent()) {
                currTransNum = currRecord.getTransNum().get();
                LogRecord CLR = currRecord.undo(transactionTable.get(currTransNum).lastLSN);
                Long CLRLsn = logManager.appendToLog(CLR);
                newLastLSN = CLRLsn;
                transactionTable.get(currRecord.getTransNum().get()).lastLSN = CLRLsn; // update transactionTable
                CLR.redo(recoveryManager, diskSpaceManager, bufferManager);

            }
            //if (currRecord.getTransNum().isPresent()) {


            // get newLSN and replace in PQ set
            newLSN = currRecord.getUndoNextLSN().orElse(currRecord.getPrevLSN().get());
            pqLastLSN.remove(currLSN);
            pqLastLSN.add(newLSN);
            //}
            if (newLSN==0 && transactionTable.containsKey(currTransNum)){
                // remove newLSN from the set (PQ)
                pqLastLSN.remove(newLSN);

                // cleanup transaction
                transactionTable.get(currTransNum).transaction.cleanup();
                transactionTable.get(currTransNum).transaction.setStatus(Transaction.Status.COMPLETE);

                // append end transaction
                EndTransactionLogRecord endRec = new EndTransactionLogRecord(currTransNum, newLastLSN);
                long endLSN = logManager.appendToLog(endRec);
                transactionTable.get(currTransNum).lastLSN = endLSN;

                // remove from table
                transactionTable.remove(currTransNum);


                // append End transaction
                //EndTransactionLogRecord endRec = new EndTransactionLogRecord(currTransNum, transactionTable.get(currTransNum).lastLSN);
                //long endLSN = logManager.appendToLog(endRec);

            }

        }








        /*
        // find largest LSN from transaction table
        long startLSN = 0;
        int count =0;
        for (Map.Entry<Long, TransactionTableEntry> trxnEntry: transactionTable.entrySet()){
            if (count == 0){
                // set startLSN to first LSN that appears in dpt
                startLSN = trxnEntry.getValue().lastLSN;
                count++;
            }

            if (trxnEntry.getValue().lastLSN > startLSN){
                startLSN = trxnEntry.getValue().lastLSN;
            }
        }

        // iterate through log
        Iterator<LogRecord> logScanner = logManager.scanFrom(0);
        while (logScanner.hasNext()){
            // store all log lsns in PQ
            LogRecord currRecord = logScanner.next();
            // if (currRecord.getType().equals(LogType.ABORT_TRANSACTION) )
            // !currRecord.getType().equals(LogType.MASTER) && (!currRecord.equals(LogType.BEGIN_CHECKPOINT)) &&
            if (!currRecord.getType().equals(LogType.MASTER) && (!currRecord.equals(LogType.BEGIN_CHECKPOINT)) && (!currRecord.equals(LogType.END_CHECKPOINT))) pqLastLSN.add(currRecord.getLSN());//
        }


        ARIESRecoveryManager recoveryManager = this;

        long newLSN = -1;
        long currTransNum = 0;
        long newLastLSN = 0;
        while (!pqLastLSN.isEmpty() && newLSN!=0){

            long currLSN = pqLastLSN.poll();
            LogRecord currRecord = logManager.fetchLogRecord(currLSN);
            //currTransNum = currRecord.getTransNum().get();
            if (currRecord.isUndoable() && currRecord.getTransNum().isPresent()) {
                currTransNum = currRecord.getTransNum().get();
                LogRecord CLR = currRecord.undo(transactionTable.get(currTransNum).lastLSN);
                Long CLRLsn = logManager.appendToLog(CLR);
                newLastLSN = CLRLsn;
                transactionTable.get(currRecord.getTransNum().get()).lastLSN = CLRLsn; // update transactionTable
                CLR.redo(recoveryManager, diskSpaceManager, bufferManager);

            }
            if (currRecord.getTransNum().isPresent()) {
                newLSN = currRecord.getUndoNextLSN().orElse(currRecord.getPrevLSN().get());
                pqLastLSN.remove(currLSN);
                pqLastLSN.add(newLSN);
            }


        }
        if (newLSN==0 && transactionTable.containsKey(currTransNum)){
            transactionTable.get(currTransNum).transaction.cleanup();
            transactionTable.get(currTransNum).transaction.setStatus(Transaction.Status.COMPLETE);

            EndTransactionLogRecord endRec = new EndTransactionLogRecord(currTransNum, newLastLSN);
            long endLSN = logManager.appendToLog(endRec);
            transactionTable.get(currTransNum).lastLSN = endLSN;
            transactionTable.remove(currTransNum);


            // append End transaction
            //EndTransactionLogRecord endRec = new EndTransactionLogRecord(currTransNum, transactionTable.get(currTransNum).lastLSN);
            //long endLSN = logManager.appendToLog(endRec);

        }**/






        return;
    }

    /**
     * Removes pages from the DPT that are not dirty in the buffer manager.
     * This is slow and should only be used during recovery.
     */
    void cleanDPT() {
        Set<Long> dirtyPages = new HashSet<>();
        bufferManager.iterPageNums((pageNum, dirty) -> {
            if (dirty) dirtyPages.add(pageNum);
        });
        Map<Long, Long> oldDPT = new HashMap<>(dirtyPageTable);
        dirtyPageTable.clear();
        for (long pageNum : dirtyPages) {
            if (oldDPT.containsKey(pageNum)) {
                dirtyPageTable.put(pageNum, oldDPT.get(pageNum));
            }
        }
    }

    // Helpers /////////////////////////////////////////////////////////////////
    /**
     * Comparator for Pair<A, B> comparing only on the first element (type A),
     * in reverse order.
     */
    private static class PairFirstReverseComparator<A extends Comparable<A>, B> implements
            Comparator<Pair<A, B>> {
        @Override
        public int compare(Pair<A, B> p0, Pair<A, B> p1) {
            return p1.getFirst().compareTo(p0.getFirst());
        }
    }
}
