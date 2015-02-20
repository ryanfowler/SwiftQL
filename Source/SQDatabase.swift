//
// SQDatabase.swift
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

// MARK: SQDatabase

public class SQDatabase {
    
    var database: COpaquePointer = nil
    private var databasePath: String?
    lazy var openCursors: [NSValue] = []
    
    class func defaultPath() -> String {
        let libPath = NSSearchPathForDirectoriesInDomains(.LibraryDirectory, .UserDomainMask, true)[0] as String
        return libPath.stringByAppendingPathComponent("SwiftQL.sqlite")
    }

    /**
    Create an SQDatabase instance with the default path
    
    The default path is a file called "SwiftQL.sqlite" in the "Library Driectory"
    
    :returns:   An initialized SQDatabase instance
    */
    public init() {
        databasePath = SQDatabase.defaultPath()
    }
    
    /**
    Create an SQDatabase instance with the specified path
    
    If nil is provided as the path, an in-memory database is created
    
    :param:     path    The path to the database. If the database does not exist, it will be created.
    
    :returns:   An initialized SQDatabase instance
    */
    public init(path: String?) {
        databasePath = path
    }
    
    deinit {
        close()
    }
    
    
    // MARK: - Open/Close
    
    /**
    Open a connection to the database
    
    :returns:   True if connection successfully opened, false otherwise
    */
    public func open() -> Bool {
        
        if database != nil {
            return true
        }
        if databasePath == nil {
            databasePath = ":memory:"
        }
        let status = sqlite3_open(databasePath!, &database)
        if status != SQLITE_OK {
            SQError.printSQLError("While opening database", errCode: status, errMsg: String.fromCString(sqlite3_errmsg(database)))
            return false
        }
        
        return true
    }
    
    /**
    Open a connection to the database with flags
    
    If a connection is already opened, it is closed and a new connection with flags is created
    
    :returns:   True if connection successfully opened with flags, false otherwise
    */
    public func openWithFlags(flags: Flag) -> Bool {
        
        if database != nil {
            close()
        }
        if databasePath == nil {
            databasePath = ":memory:"
        }
        let status = sqlite3_open_v2(databasePath!, &database, flags.toInt(), nil)
        if status != SQLITE_OK {
            SQError.printSQLError("While opening database with flags", errCode: status, errMsg: String.fromCString(sqlite3_errmsg(database)))
            return false
        }
        
        return true
    }
    
    /**
    Available flags when opening a connection to the SQLite database
    
    Options are ReadOnly, ReadWrite, and ReadWriteCreate.
    Information at https://sqlite.org/c3ref/open.html
    */
    public enum Flag {
        case ReadOnly
        case ReadWrite
        case ReadWriteCreate
        
        private func toInt() -> Int32 {
            switch self {
            case .ReadOnly:
                return SQLITE_OPEN_READONLY
            case .ReadWrite:
                return SQLITE_OPEN_READWRITE
            case .ReadWriteCreate:
                return SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
            }
        }
    }
    
    /**
    Closes the connection to the database
    */
    public func close() {
        
        if database == nil {
            return
        }
        
        closeAllCursors()
        
        let status = sqlite3_close(database)
        if status != SQLITE_OK {
            SQError.printSQLError("While closing database", errCode: status, errMsg: String.fromCString(sqlite3_errmsg(database)))
        }
        
        database = nil
    }
    
    
    // MARK: Execute An Update
    
    /**
    Execute a non-query SQL statement
    
    :param: sql     The String of SQL to execute
    
    :returns:   True if executed successfully, false if there was an error
    */
    public func update(sql: String) -> Bool {
        return update(sql, withObjects: [])
    }
    
    /**
    Execute a non-query SQL statement with object bindings
    
    :param: sql     The String of SQL to execute
    :param: withObjects     The Array of objects to bind with the sql string
    
    :returns:   True if executed successfully, false if there was an error
    */
    public func update(sql: String, withObjects objects: [Any?]) -> Bool {
        
        var pStmt: COpaquePointer = nil
        
        var status = sqlite3_prepare_v2(database, sql, -1, &pStmt, nil)
        if status != SQLITE_OK {
            SQError.printSQLError("While preparing SQL statement: \(sql)", errCode: status, errMsg: String.fromCString(sqlite3_errmsg(database)))
            sqlite3_finalize(pStmt)
            return false
        }
        
        if objects.count != 0 {
            if sqlite3_bind_parameter_count(pStmt) != Int32(objects.count) {
                SQError.printError("While binding SQL statement: \(sql)", next: "Improper number of objects provided to bind")
                sqlite3_finalize(pStmt)
                return false
            }
            var i: Int32 = 1
            for obj in objects {
                bindObject(obj, toStatement: pStmt, withColumn: i)
                i++
            }
        }
        
        status = sqlite3_step(pStmt)
        if status != SQLITE_DONE && status != SQLITE_OK {
            SQError.printSQLError("While stepping through SQL statement: \(sql)", errCode: status, errMsg: String.fromCString(sqlite3_errmsg(database)))
            sqlite3_finalize(pStmt)
            return false
        }
        
        sqlite3_finalize(pStmt)
        
        return true
    }
    
    
    // MARK: Execute Multiple Updates
    
    /**
    Execute multiple non-query SQL statements
    
    :param: sql     The Array of SQL Strings to execute
    
    :returns:   True if all statements executed successfully, false if there was an error
    */
    public func updateMany(sql: [String]) -> Bool {
        
        var errMsg: UnsafeMutablePointer<Int8> = nil
        
        var finalStr = ""
        for obj in sql {
            finalStr += obj + ";"
        }
        
        var status = sqlite3_exec(database, finalStr, nil, nil, &errMsg)
        if status != SQLITE_OK || errMsg != nil {
            SQError.printSQLError("While executing multiple statements", errCode: status, errMsg: String.fromCString(errMsg))
            sqlite3_free(errMsg)
            return false
        }
        
        return true
    }
    
    
    // MARK: Execute A Query
    
    /**
    Execute a query SQL statement
    
    :param: sql     The String of SQL to execute
    
    :returns:   An Optional SDCursor object if executed successfully, nil if there was an error
    */
    public func query(sql: String) -> SQCursor? {
        return query(sql, withObjects: [])
    }
    
    /**
    Execute a query SQL statement with object bindings
    
    :param: sql     The String of SQL to execute
    :param: withObjects     The Array of objects to bind with the sql string
    
    :returns:   An Optional SDCursor object if executed successfully, nil if there was an error
    */
    public func query(sql: String, withObjects objects: [Any?]) -> SQCursor? {
        
        var pStmt: COpaquePointer = nil
        
        var status = sqlite3_prepare_v2(database, sql, -1, &pStmt, nil)
        if status != SQLITE_OK {
            SQError.printSQLError("While preparing SQL statement: \(sql)", errCode: status, errMsg: String.fromCString(sqlite3_errmsg(database)))
            sqlite3_finalize(pStmt)
            return nil
        }
        
        if objects.count != 0 {
            if sqlite3_bind_parameter_count(pStmt) != Int32(objects.count) {
                SQError.printError("While binding SQL statement: \(sql)", next: "Improper number of objects provided to bind")
                sqlite3_finalize(pStmt)
                return nil
            }
            var i: Int32 = 1
            for obj in objects {
                bindObject(obj, toStatement: pStmt, withColumn: i)
                i++
            }
        }
        
        return createCursor(pStmt, sql: sql)
    }
    
    
    // MARK: Transaction Functions
    
    /**
    Begin an exclusive transaction
    
    :returns:   True if transaction has successfully begun, false if otherwise
    */
    public func beginTransaction() -> Bool {
        return update("BEGIN EXCLUSIVE TRANSACTION")
    }
    
    /**
    Begin a deferred transaction
    
    :returns:   True if transaction has successfully begun, false if otherwise
    */
    public func beginDeferredTransaction() -> Bool {
        return update("BEGIN DEFERRED TRANSACTION")
    }
    
    /**
    Commit the currently open transaction
    
    :returns:   True if transaction has successfully been committed, false if otherwise
    */
    public func commitTransaction() -> Bool {
        return update("COMMIT TRANSACTION")
    }
    
    /**
    Rollback the currently open transaction
    
    :returns:   True if transaction has successfully been rolled back, false if otherwise
    */
    public func rollbackTransaction() -> Bool {
        return update("ROLLBACK TRANSACTION")
    }
    
    
    // MARK: Savepoint Functions
    
    /**
    Begin a savepoint
    
    :param: name    The savepoint name
    
    :returns:   True if savepoint successfully started, false if otherwise
    */
    public func startSavepoint(name: String) -> Bool {
        return update("SAVEPOINT \(escapeIdentifier(name))")
    }
    
    /**
    Release a savepoint
    
    :param: name    The savepoint name
    
    :returns:   True if savepoint has been successfully released, false if otherwise
    */
    public func releaseSavepoint(name: String) -> Bool {
        return update("RELEASE SAVEPOINT \(escapeIdentifier(name))")
    }
    
    /**
    Rollback a savepoint
    
    :param: name    The savepoint name
    
    :returns:   True if savepoint has been successfully rolled back, false if otherwise
    */
    public func rollbackToSavepoint(name: String) -> Bool {
        return update("ROLLCAK TO SAVEPOINT \(escapeIdentifier(name))")
    }
    
    private func escapeIdentifier(identifier: String) -> String {
        return identifier.stringByReplacingOccurrencesOfString("'", withString: "''")
    }
    
    
    // MARK: Misc
    
    /**
    Obtain the last inserted row id
    
    :returns:   An Int64 of the last inserted row id
    */
    public func lastInsertId() -> Int64 {
        return sqlite3_last_insert_rowid(database) as Int64
    }
    
    /**
    Obtain the number of rows modified by the last operation
    
    :returns:   An Int indicating the number of rows modified
    */
    public func rowsChanged() -> Int {
        return Int(sqlite3_changes(database))
    }
    
    /**
    Obtain the last error code
    
    :returns:   An Int indicating the last error code
    */
    public func lastErrorCode() -> Int {
        return Int(sqlite3_errcode(database))
    }
    
    /**
    Obtain the last error message
    
    :returns:   A String indicating the last error message
    */
    public func lastErrorMessage() -> String {
        if let mes = String.fromCString(sqlite3_errmsg(database)) {
            return mes
        }
        return ""
    }
    
    /**
    Obtain the sqlite version
    
    :returns:   A String indicating the sqlite version
    */
    public func sqliteVersion() -> String {
        if let ver = String.fromCString(sqlite3_libversion()) {
            return ver
        }
        return ""
    }
    
    /**
    Change the journal_mode of the database
    
    :param: mode    A JournalMode case (.Delete, .Truncate, .Persist, .Memory, .WAL, .Off
    
    :returns:   True if journal_mode was successfully changed, false otherwise
    */
    public func useJournalMode(mode: JournalMode) -> Bool {
        var sql = "PRAGMA journal_mode=\(mode.toString())"
        var errMsg: UnsafeMutablePointer<Int8> = nil
        let status = sqlite3_exec(database, sql, nil, nil, &errMsg)
        if (status != SQLITE_OK && status != SQLITE_DONE) || errMsg != nil {
            SQError.printSQLError("While changing to journaling mode: \(mode.toString())", errCode: status, errMsg: String.fromCString(errMsg))
            sqlite3_free(errMsg)
            return false
        }
        return true
    }
    
    /**
    Journal mode options as specified at: https://sqlite.org/pragma.html#pragma_journal_mode
    */
    public enum JournalMode {
        case Delete
        case Truncate
        case Persist
        case Memory
        case WAL
        case Off
        
        private func toString() -> String {
            switch self {
            case .Delete:
                return "DELETE"
            case .Truncate:
                return "TRUNCATE"
            case .Persist:
                return "PERSIST"
            case .Memory:
                return "MEMORY"
            case .WAL:
                return "WAL"
            case .Off:
                return "OFF"
            }
        }
    }
    
    
    // MARK: Binding
    
    private func bindObject(obj: Any?, toStatement pStmt: COpaquePointer, withColumn col: Int32) {
        
        switch obj {
        case let val as String:
            sqlite3_bind_text(pStmt, col, val, -1, SQLITE_TRANSIENT)
        case let val as Int64:
            sqlite3_bind_int64(pStmt, col, val)
        case let val as Int:
            sqlite3_bind_int64(pStmt, col, Int64(val))
        case let val as Int32:
            sqlite3_bind_int(pStmt, col, val)
        case let val as Double:
            sqlite3_bind_double(pStmt, col, val)
        case let val as Bool:
            var intVal: Int32 = 0
            if val {
                intVal = 1
            }
            sqlite3_bind_int(pStmt, col, intVal)
        case let val as NSData:
            var blob = val.bytes
            if blob == nil {
                blob = UnsafePointer<()>()
            }
            sqlite3_bind_blob(pStmt, col, blob, -1, SQLITE_TRANSIENT)
        case let val as NSDate:
            sqlite3_bind_double(pStmt, col, val.timeIntervalSince1970)
        case nil:
            sqlite3_bind_null(pStmt, col)
        default:
            sqlite3_bind_null(pStmt, col)
            SQError.printWarning("While binding object: \(obj)", next: "Unsupported object type, binding null")
        }
    }
    
    
    // MARK: SDCursor Functions
    
    func closeCursor(cursor: SQCursor) {
        let cursorValue = NSValue(nonretainedObject: cursor)
        for var i = 0; i < openCursors.count; i++ {
            if openCursors[i] == cursorValue {
                openCursors.removeAtIndex(i)
                return
            }
        }
    }
    
    private func closeAllCursors() {
        while !openCursors.isEmpty {
            if let cs = openCursors[0].nonretainedObjectValue as? SQCursor {
                cs.close()
            }
        }
    }
    
    private func createCursor(statement: COpaquePointer, sql: String) -> SQCursor {
        var cursor = SQCursor(statement: statement, fromDatabase: self, withSQL: sql)
        let cursorValue = NSValue(nonretainedObject: cursor)
        openCursors.append(cursorValue)
        return cursor
    }
    
}

// fix for Swift limitations with C
private let SQLITE_STATIC = sqlite3_destructor_type(COpaquePointer(bitPattern: 0))
private let SQLITE_TRANSIENT = sqlite3_destructor_type(COpaquePointer(bitPattern: -1))