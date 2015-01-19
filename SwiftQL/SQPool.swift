//
// SQPool.swift
//
// Copyright (c) 2015 Ryan Fowler
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

import Foundation

// MARK: SQPool

public class SQPool {
    
    private let path: String?
    private var connIndex = 1
    private let flags = SQDatabase.Flag.ReadWriteCreate
    private var connPool: [Int:SQDatabase] = [:]
    private var inUsePool: [Int:SQDatabase] = [:]
    
    // Queue for database write operations
    private var writeQueue = dispatch_queue_create("swiftql.write", DISPATCH_QUEUE_SERIAL)
    
    // Queue for getting/releasing databases in pool
    // To prevent weird behaviour from accessing properties from multiple threads
    private var poolQueue = dispatch_queue_create("swiftql.pool", DISPATCH_QUEUE_SERIAL)
    
    /**
    The maximum number of connections to be kept in the connection pool
    
    Note: this only limits the number of available connections in the connection pool.
    Connections will be created as required and returned to the connection pool, or destroyed if the connection pool contains the maximum connection limit.
    */
    public var maxSustainedConnections = 5
    
    /**
    Returns the number of idle connections
    
    Note: Will always be less than or equal to maxSustainedConnections
    */
    public func numberOfFreeConnections() -> Int {
        return connPool.count
    }
    
    /**
    Create an SQPool instance with the default path
    
    The default path is a file called "SwiftQL.sqlite" in the "Library Driectory"
    
    :returns:   An initialized SQPool instance
    */
    public init() {
        path = SQDatabase.defaultPath()
        useWALMode()
    }
    
    /**
    Create an SQPool instance with the specified path and flags
    
    :returns:   An initialized SQPool instance
    */
    public init(path: String?, withFlags flags: SQDatabase.Flag) {
        self.path = path
        self.flags = flags
        useWALMode()
    }
    
    deinit {
        connPool = [:]
        inUsePool = [:]
    }
    
    // Does not use the proper getConnection/releaseConnection - only meant for use in init()
    // Only call in init!
    private func useWALMode() {
        let db = SQDatabase(path: path)
        db.openWithFlags(flags)
        if !db.useJournalMode(.WAL) {
            SQError.printWarning("While opening an SQPool instance", next: "Cannot verify that the database is in WAL mode")
        }
        connPool[1] = db
    }
    
    // Obtain an SQDatabase object from the connection pool,
    // otherwise create a new connection
    private func getConnection() -> (Int, SQDatabase) {
        var db: SQDatabase?
        var index = 0
        dispatch_sync(poolQueue, {
            if self.connPool.isEmpty {
                self.connIndex++
                let database = SQDatabase(path: self.path)
                database.openWithFlags(self.flags)
                self.inUsePool[self.connIndex] = database
                db = database
                index = self.connIndex
                return
            }
            for ind in self.connPool.keys {
                let database = self.connPool.removeValueForKey(ind)!
                self.inUsePool[ind] = database
                db = database
                index = ind
                return
            }
        })
        return (index, db!)
    }
    
    // Release an SQDatabase object to the connection pool,
    // or delete it if connPool is greater than the maxSustainedConnections
    private func releaseConnection(index: Int) {
        dispatch_sync(poolQueue, {
            if self.connPool.count < self.maxSustainedConnections {
                let database = self.inUsePool.removeValueForKey(index)!
                self.connPool[index] = database
                return
            }
            self.inUsePool[index] = nil
        })
    }
    
    /**
    Execute a write (non-query) operation(s) on the database
    
    Note: write() should be used over read() if ANY operation in the closure is a non-query.
    Otherwise, locked database errors can occur.
    
    :param: closure     A closure that accepts an SQDatabase instance to be used for non-query operations
    */
    public func write(closure: (SQDatabase)->Void) {
        let (index, db) = getConnection()
        dispatch_sync(writeQueue, {
            closure(db)
        })
        releaseConnection(index)
    }
    
    /**
    Execute a write (non-query) operation(s) on the database asynchronously
    
    Note: write() should be used over read() if ANY operation in the closure is a non-query.
    Otherwise, locked database errors can occur.
    
    This function will return immediately and the closure will run on a background thread.
    
    :param: closure     A closure that accepts an SQDatabase instance to be used for non-query operations
    */
    public func writeAsync(closure: (SQDatabase)->Void) {
        dispatch_async(dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {self.write(closure)})
    }
    
    /**
    Execute read (query) operations on the database
    
    Note: write() should be used over read() if ANY operation in the closure is a non-query.
    Otherwise, locked database errors can occur.
    
    :param: closure     A closure that accepts an SQDatabase instance to be used for query operations
    */
    public func read(closure: (SQDatabase)->Void) {
        let (index, db) = getConnection()
        closure(db)
        releaseConnection(index)
    }
    
    /**
    Execute read (query) operations on the database asynchronously
    
    Note: write() should be used over read() if ANY operation in the closure is a non-query.
    Otherwise, locked database errors can occur.
    
    This function will return immediately and the closure will run on a background thread.
    
    :param: closure     A closure that accepts an SQDatabase instance to be used for query operations
    */
    public func readAsync(closure: (SQDatabase)->Void) {
        dispatch_async(dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {self.read(closure)})
    }
    
    /**
    Execute a transaction
    
    :param: closure     A closure that accepts an SQDatabase instance that returns true to commit, or false to rollback
    
    :returns:   True if the transaction was successfully committed, false if rolled back
    */
    public func transaction(closure: (SQDatabase)->Bool) -> Bool {
        
        var status = false
        let (index, db) = self.getConnection()
        dispatch_sync(writeQueue, {
            db.beginTransaction()
            if closure(db) {
                if db.commitTransaction() {
                    status = true
                } else {
                    db.rollbackTransaction()
                }
            } else {
                db.rollbackTransaction()
            }
        })
        releaseConnection(index)
        
        return status
    }
    
    /**
    Execute a transaction asynchronously
    
    Note: This function will return immediately and the closure will run on a background thread.
    
    :param: closure     A closure that accepts an SQDatabase instance that returns true to commit, or false to rollback
    */
    public func transactionAsync(closure: (SQDatabase)->Bool) {
        dispatch_async(dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {let suc = self.transaction(closure)})
    }
    
}
