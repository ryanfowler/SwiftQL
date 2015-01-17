//
//  SQPool.swift
//  SwiftData2
//
//  Created by Ryan Fowler on 2014-12-30.
//  Copyright (c) 2014 Ryan Fowler. All rights reserved.
//


public class SQPool {
    
    private let path: String?
    private let flags = SQDatabase.Flags.ReadWriteCreate
    private var useID = 0
    private var connPool: [SQDatabase] = []
    private var inUsePool: [Int:SQDatabase] = [:]
    
    // Queue for database write operations
    private lazy var writeQueue: dispatch_queue_t = {
        [unowned self] in
        var queue = dispatch_queue_create("swiftdata.pool.\(self)", DISPATCH_QUEUE_SERIAL)
        return queue
    }()
    // Queue for getting/releasing databases in pool
    private lazy var poolQueue: dispatch_queue_t = {
        [unowned self] in
        var queue = dispatch_queue_create("swiftdata.pool.conn.\(self)", DISPATCH_QUEUE_SERIAL)
        return queue
        }()
    
    public var maxSustainedConnections = 5
    
    public init() {
        path = SQDatabase.defaultPath()
        useWALMode()
    }
    
    public init(path: String?, withFlags flags: SQDatabase.Flags) {
        self.path = path
        self.flags = flags
        useWALMode()
    }
    
    deinit {
        useDeleteMode()
        connPool = []
        inUsePool = [:]
    }
    
    private func useWALMode() {
        let db = SQDatabase(path: path)
        db.openWithFlags(flags)
        db.query("PRAGMA journal_mode=WAL", withObjects: [])
        connPool.append(db)
    }
    
    private func useDeleteMode() {
        let (index, db) = getConnection()
        db.query("PRAGMA journal_mode=DELETE", withObjects: [])
        releaseConnection(index)
    }
    
    private func getConnection() -> (Int, SQDatabase) {
        var index = 0
        var db: SQDatabase?
        dispatch_sync(poolQueue, {
            self.useID++
            index = self.useID
            if self.connPool.isEmpty {
                let database = SQDatabase(path: self.path)
                database.openWithFlags(self.flags)
                self.inUsePool[self.useID] = database
                println("Using new connection: \(self.useID)")
                db = database
                return
            }
            let database = self.connPool.removeLast()
            self.inUsePool[self.useID] = database
            println("Using connection: \(self.useID)")
            db = database
            return
        })
        return (index, db!)
    }
    
    private func releaseConnection(index: Int) {
        dispatch_sync(poolQueue, {
            if self.connPool.count < self.maxSustainedConnections {
                println("Returning connection: \(index)")
                let database = self.inUsePool.removeValueForKey(index)!
                self.connPool.append(database)
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
    
    public func write(closure: (SQDatabase)->Void) {
        let (index, db) = getConnection()
        dispatch_sync(writeQueue, {
            closure(db)
        })
        releaseConnection(index)
    }
    
    public func read(closure: (SQDatabase)->Void) {
        let (index, db) = getConnection()
        closure(db)
        releaseConnection(index)
    }
    
}
