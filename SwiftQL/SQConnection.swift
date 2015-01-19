//
// SQConnection.swift
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

// MARK: SQConnection

public class SQConnection {
    
    private var database: SQDatabase
    private var databaseQueue = dispatch_queue_create("swiftql.connection", DISPATCH_QUEUE_SERIAL)
    
    /**
    Create an SQConnection instance with the default path
    
    The default path is a file called "SwiftQL.sqlite" in the "Library Driectory"
    
    :returns:   An initialized SDConnection instance
    */
    public init() {
        database = SQDatabase()
        database.open()     // Consider making this a failable initializer
    }
    
    /**
    Create an SQConnection instance with the specified path
    
    If nil is provided as the path, an in-memory database is created
    
    :param:     path    The path to the database. If the database does not exist, it will be created.
    
    :returns:   An initialized SDConnection instance
    */
    public init(path: String?, withFlags flags: SQDatabase.Flag) {
        database = SQDatabase(path: path)
        database.openWithFlags(flags)   // Consider making this a failable initializer
    }
    
    /**
    Execute functions within a closure
    
    :param: closure     The closure that accepts an SQDatabase object
    */
    public func execute(closure: (SQDatabase)->Void) {  // Consider making the closure return a success variable
        dispatch_sync(databaseQueue, {closure(self.database)})
    }
    
    /**
    Execute functions within a closure asynchronously
    
    Note: This function will return immediately and the closure will run on a background thread.
    
    :param: closure     The closure that accepts an SQDatabase object to execute asynchronously
    */
    public func executeAsync(closure: (SQDatabase)->Void) {
        dispatch_async(dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {self.execute(closure)})
    }
    
    /**
    Execute a transaction
    
    :param: closure     The transaction closure that accepts an SQDatabase object and returns true to commit, or false to rollback
    
    :returns:   True if transaction has been successfully committed, false if rolled back
    */
    public func transaction(closure: (SQDatabase)->Bool) -> Bool {
        
        var status = false
        dispatch_sync(databaseQueue, {
            self.database.beginTransaction()
            if closure(self.database) {
                if self.database.commitTransaction() {
                    status = true
                } else {
                    self.database.rollbackTransaction()
                }
            } else {
                self.database.rollbackTransaction()
            }
        })
        
        return status
    }
    
    /**
    Execute a transaction asynchronously
    
    Note: This function will return immediately and the closure will run on a background thread.
    
    :param: closure     The transaction closure that accepts an SQDatabase object and returns true to commit, or false to rollback
    */
    public func transactionAsync(closure: (SQDatabase)->Bool) {
        dispatch_async(dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {let suc = self.transaction(closure)})
    }
    
}
