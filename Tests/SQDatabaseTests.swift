//
// SQDatabaseTests.swift
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

import XCTest
import SwiftQL

class SQDatabaseTests: XCTestCase {
    
    override func setUp() {
        super.setUp()
        let db = SQDatabase()
        db.open()
        let success = db.updateMany(["DROP TABLE IF EXISTS test", "CREATE TABLE test (id INT PRIMARY KEY, name TEXT, age INT)"])
        XCTAssertTrue(success, "table should have been created")
        db.close()
    }
    
    func testTableCreation() {
        let db = SQDatabase()
        db.open()
        let success = db.updateMany(["DROP TABLE IF EXISTS test", "CREATE TABLE test (id INT PRIMARY KEY, name TEXT, age INT)"])
        XCTAssertTrue(success, "table should have been created")
        db.close()
    }
    
    func testRowInsert() {
        let db = SQDatabase()
        db.open()
        let success = db.update("INSERT INTO test VALUES (1, 'Ryan', 24)")
        XCTAssertTrue(success, "row should have been inserted")
        db.close()
    }
    
    func testRowInsertWithBinding() {
        let db = SQDatabase()
        db.open()
        let success = db.update("INSERT INTO test VALUES (?, ?, ?)", withObjects: [1, "Ryan", 24])
        XCTAssertTrue(success, "row shoud have been inserted with binding")
        db.close()
    }
    
    func testOpeningWithFlagsReadOnly() {
        let db = SQDatabase()
        var success = db.openWithFlags(SQDatabase.Flag.ReadOnly)
        XCTAssertTrue(success, "database should have opened as read only")
        success = db.update("INSERT INTO test VALUES (1, 'Ryan', 24)")
        XCTAssertFalse(success, "update operation should fail on read only connection")
        let cursor = db.query("SELECT * FROM test")
        XCTAssertNotNil(cursor, "cursor should not be nil")
        db.close()
        
        let libPath = NSSearchPathForDirectoriesInDomains(.LibraryDirectory, .UserDomainMask, true)[0] as String
        let newDbPath = libPath.stringByAppendingPathComponent("sample.sqlite")
        let db2 = SQDatabase(path: newDbPath)
        success = db2.openWithFlags(.ReadOnly)
        XCTAssertFalse(success, "database connection should fail on non-existent database on read only connection")
        db2.close()
    }
    
    func testOpeningWithFlagsReadWrite() {
        let db = SQDatabase()
        var success = db.openWithFlags(SQDatabase.Flag.ReadWrite)
        XCTAssertTrue(success, "database should have opened as read write")
        success = db.update("INSERT INTO test VALUES (1, 'Ryan', 24)")
        XCTAssertTrue(success, "update operation should succeed on read write connection")
        let cursor = db.query("SELECT * FROM test")
        XCTAssertNotNil(cursor, "cursor should not be nil")
        db.close()
        
        let libPath = NSSearchPathForDirectoriesInDomains(.LibraryDirectory, .UserDomainMask, true)[0] as String
        let newDbPath = libPath.stringByAppendingPathComponent("sample.sqlite")
        let db2 = SQDatabase(path: newDbPath)
        success = db2.openWithFlags(.ReadWrite)
        XCTAssertFalse(success, "database connection should fail on non-existent database on read write connection")
        db2.close()
    }
    
    func testOpeningWithFlagsReadWriteCreate() {
        let db = SQDatabase()
        var success = db.openWithFlags(SQDatabase.Flag.ReadWriteCreate)
        XCTAssertTrue(success, "database should have opened as read only")
        success = db.update("INSERT INTO test VALUES (1, 'Ryan', 24)")
        XCTAssertTrue(success, "update operation should succeed on readwrite connection")
        let cursor = db.query("SELECT * FROM test")
        XCTAssertNotNil(cursor, "cursor should not be nil")
        db.close()
        
        let libPath = NSSearchPathForDirectoriesInDomains(.LibraryDirectory, .UserDomainMask, true)[0] as String
        let newDbPath = libPath.stringByAppendingPathComponent("sample.sqlite")
        let db2 = SQDatabase(path: newDbPath)
        success = db2.openWithFlags(.ReadWriteCreate)
        XCTAssertTrue(success, "database connection should succeed on non-existent database on read write create connection")
        db2.close()
        
        let manager = NSFileManager()
        var err: NSError? = NSError()
        manager.removeItemAtPath(newDbPath, error: &err)
    }

}
