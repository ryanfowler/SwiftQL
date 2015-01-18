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


public class SQPool {
    
    private let path: String?
    private var connIndex = 1
    private let flags = SQDatabase.Flag.ReadWriteCreate
    private var connPool: [Int:SQDatabase] = [:]
    private var inUsePool: [Int:SQDatabase] = [:]
    
    // Queue for database write operations
    private lazy var writeQueue: dispatch_queue_t = {
        [unowned self] in
        var queue = dispatch_queue_create("swiftdata.pool.\(self)", DISPATCH_QUEUE_SERIAL)
        return queue
    }()
    // Queue for getting/releasing databases in pool
    // To prevent weird behaviour from accessing properties from multiple threads
    private lazy var poolQueue: dispatch_queue_t = {
        [unowned self] in
        var queue = dispatch_queue_create("swiftdata.pool.conn.\(self)", DISPATCH_QUEUE_SERIAL)
        return queue
    }()
    
    // Max number of connections to keep in the connection pool
    // Note: this only limits the connections in the connPool array
    public var maxSustainedConnections = 5
    
    public init() {
        path = SQDatabase.defaultPath()
        useWALMode()
    }
    
    public init(path: String?, withFlags flags: SQDatabase.Flag) {
        self.path = path
        self.flags = flags
        if !useWALMode() {
            SQError.printWarning("While opening an SQPool instance", next: "Cannot verify that the database is in WAL mode")
        }
    }
    
    deinit {
        connPool = [:]
        inUsePool = [:]
    }
    
    // Does not use the proper getConnection/releaseConnection
    // Only call in init!
    private func useWALMode() -> Bool {
        let db = SQDatabase(path: path)
        db.openWithFlags(flags)
        let success = db.useJournalMode(.WAL)
        connPool[1] = db
        return success
    }
    
    // Obtain an SQDatabase object (already opened) from the connection pool,
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
    
    public func transactionAsync(closure: (SQDatabase)->Bool) {
        dispatch_async(dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {let suc = self.transaction(closure)})
    }
    
    public func write(closure: (SQDatabase)->Void) {
        let (index, db) = getConnection()
        dispatch_sync(writeQueue, {
            closure(db)
        })
        releaseConnection(index)
    }
    
    public func writeAsync(closure: (SQDatabase)->Void) {
        dispatch_async(dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {self.write(closure)})
    }
    
    public func read(closure: (SQDatabase)->Void) {
        let (index, db) = getConnection()
        closure(db)
        releaseConnection(index)
    }
    
    public func readAsync(closure: (SQDatabase)->Void) {
        dispatch_async(dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {self.read(closure)})
    }
    
}
