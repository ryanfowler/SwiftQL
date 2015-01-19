//
// SQPoolTests.swift
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

class SQPoolTests: XCTestCase {

    override func setUp() {
        super.setUp()
        let db = SQDatabase()
        db.open()
        let success = db.updateMany(["DROP TABLE IF EXISTS test", "CREATE TABLE test (id INT PRIMARY KEY, name TEXT, age INT)"])
        XCTAssertTrue(success, "table should have been created")
        db.close()
    }
    
    func testWrite() {
        let pool = SQPool()
        pool.write({
            db in
            var success = db.update("INSERT INTO test VALUES (?, ?, ?)", withObjects: [1, "Ryan", 24])
            XCTAssertTrue(success, "row should have been inserted")
        })
    }
    
    func testRead() {
        let pool = SQPool()
        pool.read({
            db in
            let cursor = db.query("SELECT * FROM test")
            XCTAssertNotNil(cursor, "cursor should not be nil")
            XCTAssertFalse(cursor!.next(), "cursor should not contain any rows")
        })
    }
    
    func testConcurrentWrites() {
        let pool = SQPool()
        let dGroup = dispatch_group_create()
        
        var success1 = false
        var success2 = false
        var success3 = false
        
        dispatch_group_async(dGroup, dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {
            pool.write({
                db in
                db.beginTransaction()
                for var i = 0; i < 1000; i++ {
                    db.update("INSERT INTO test VALUES (?, ?, ?)", withObjects: [i, "Ryan", 24])
                }
                success1 = db.commitTransaction()
            })
        })
        dispatch_group_async(dGroup, dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {
            pool.write({
                db in
                db.beginTransaction()
                for var i = 1000; i < 2000; i++ {
                    db.update("INSERT INTO test VALUES (?, ?, ?)", withObjects: [i, "Ryan", 24])
                }
                success2 = db.commitTransaction()
            })
        })
        dispatch_group_async(dGroup, dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {
            pool.write({
                db in
                db.beginTransaction()
                for var i = 2000; i < 3000; i++ {
                    db.update("INSERT INTO test VALUES (?, ?, ?)", withObjects: [i, "Ryan", 24])
                }
                success3 = db.commitTransaction()
            })
        })
        
        dispatch_group_wait(dGroup, DISPATCH_TIME_FOREVER)
        
        XCTAssertTrue(success1, "transaction from write1 should successfully commit")
        XCTAssertTrue(success2, "transaction from write2 should successfully commit")
        XCTAssertTrue(success3, "transaction from write3 should successfully commit")
        
        var idArr: [Int] = []
        var nameArr: [String] = []
        var ageArr: [Int] = []
        
        pool.read({
            db in
            if let cursor = db.query("SELECT * FROM test") {
                while cursor.next() {
                    idArr.append(cursor.intForColumnIndex(0)!)
                    nameArr.append(cursor.stringForColumnIndex(1)!)
                    ageArr.append(cursor.intForColumnIndex(2)!)
                }
            }
        })
        
        XCTAssertEqual(idArr.count, 3000, "id array should ahve 3000 rows")
        XCTAssertEqual(nameArr.count, 3000, "name array should ahve 3000 rows")
        XCTAssertEqual(ageArr.count, 3000, "age array should ahve 3000 rows")
    }
    
    func testConcurrentReads() {
        let pool = SQPool()
        var success = pool.transaction({
            db in
            for var i = 0; i < 1000; i++ {
                db.update("INSERT INTO test VALUES (?, ?, ?)", withObjects: [i, "Ryan", 24])
            }
            return true
        })
        XCTAssertTrue(success, "inserts should succeed in transaction")
        
        let dGroup = dispatch_group_create()
        
        var idArr1: [Int] = []
        var nameArr1: [String] = []
        var ageArr1: [Int] = []
        var idArr2: [Int] = []
        var nameArr2: [String] = []
        var ageArr2: [Int] = []
        var idArr3: [Int] = []
        var nameArr3: [String] = []
        var ageArr3: [Int] = []
        
        dispatch_group_async(dGroup, dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {
            pool.read({
                db in
                if let cursor = db.query("SELECT * FROM test") {
                    while cursor.next() {
                        idArr1.append(cursor.intForColumnIndex(0)!)
                        nameArr1.append(cursor.stringForColumnIndex(1)!)
                        ageArr1.append(cursor.intForColumnIndex(2)!)
                    }
                }
            })
        })
        dispatch_group_async(dGroup, dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {
            pool.read({
                db in
                if let cursor = db.query("SELECT * FROM test") {
                    while cursor.next() {
                        idArr2.append(cursor.intForColumnIndex(0)!)
                        nameArr2.append(cursor.stringForColumnIndex(1)!)
                        ageArr2.append(cursor.intForColumnIndex(2)!)
                    }
                }
            })
        })
        dispatch_group_async(dGroup, dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {
            pool.read({
                db in
                if let cursor = db.query("SELECT * FROM test") {
                    while cursor.next() {
                        idArr3.append(cursor.intForColumnIndex(0)!)
                        nameArr3.append(cursor.stringForColumnIndex(1)!)
                        ageArr3.append(cursor.intForColumnIndex(2)!)
                    }
                }
            })
        })
        
        dispatch_group_wait(dGroup, DISPATCH_TIME_FOREVER)
        
        XCTAssertEqual(idArr1.count, 1000, "read1 ids shoudl be 1000")
        XCTAssertEqual(nameArr1.count, 1000, "read1 names shoudl be 1000")
        XCTAssertEqual(ageArr1.count, 1000, "read1 ages shoudl be 1000")
        XCTAssertEqual(idArr2.count, 1000, "read2 ids shoudl be 1000")
        XCTAssertEqual(nameArr2.count, 1000, "read2 names shoudl be 1000")
        XCTAssertEqual(ageArr2.count, 1000, "read2 ages shoudl be 1000")
        XCTAssertEqual(idArr3.count, 1000, "read3 ids shoudl be 1000")
        XCTAssertEqual(nameArr3.count, 1000, "read3 names shoudl be 1000")
        XCTAssertEqual(ageArr3.count, 1000, "read3 ages shoudl be 1000")
    }
    
    func testConcurrentReadWrites() {
        let pool = SQPool()
        let dGroup = dispatch_group_create()
        
        dispatch_group_async(dGroup, dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {
            pool.read({
                db in
                let cursor = db.query("SELECT * FROM test")
                XCTAssertNotNil(cursor, "cursor should not be nil")
            })
        })
        dispatch_group_async(dGroup, dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {
            pool.write({
                db in
                db.beginTransaction()
                for var i = 0; i < 1000; i++ {
                    db.update("INSERT INTO test VALUES (?, ?, ?)", withObjects: [i, "Ryan", 24])
                }
                let success = db.commitTransaction()
                XCTAssertTrue(success, "transaction should successfully commit")
            })
        })
        dispatch_group_async(dGroup, dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {
            pool.write({
                db in
                db.beginTransaction()
                for var i = 1000; i < 2000; i++ {
                    db.update("INSERT INTO test VALUES (?, ?, ?)", withObjects: [i, "Ryan", 24])
                }
                let success = db.commitTransaction()
                XCTAssertTrue(success, "transaction should successfully commit")
            })
        })
        dispatch_group_async(dGroup, dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {
            pool.read({
                db in
                let cursor = db.query("SELECT * FROM test")
                XCTAssertNotNil(cursor, "cursor should not be nil")
            })
        })
        
        dispatch_group_wait(dGroup, DISPATCH_TIME_FOREVER)
        var idArr: [Int] = []
        var nameArr: [String] = []
        var ageArr: [Int] = []
        
        pool.read({
            db in
            if let cursor = db.query("SELECT * FROM test") {
                while cursor.next() {
                    idArr.append(cursor.intForColumnIndex(0)!)
                    nameArr.append(cursor.stringForColumnIndex(1)!)
                    ageArr.append(cursor.intForColumnIndex(0)!)
                }
            }
        })
        
        XCTAssertEqual(idArr.count, 2000, "id array should have 2000 values")
        XCTAssertEqual(nameArr.count, 2000, "name array should have 2000 values")
        XCTAssertEqual(ageArr.count, 2000, "age array should have 2000 values")
    }
    
    func testConcurrentWriteReads() {
        let pool = SQPool()
        let dGroup = dispatch_group_create()
        
        dispatch_group_async(dGroup, dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {
            pool.write({
                db in
                db.beginTransaction()
                for var i = 0; i < 1000; i++ {
                    db.update("INSERT INTO test VALUES (?, ?, ?)", withObjects: [i, "Ryan", 24])
                }
                let success = db.commitTransaction()
                XCTAssertTrue(success, "transaction should successfully commit")
            })
        })
        dispatch_group_async(dGroup, dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {
            pool.read({
                db in
                let cursor = db.query("SELECT * FROM test")
                XCTAssertNotNil(cursor, "cursor should not be nil")
            })
        })
        dispatch_group_async(dGroup, dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {
            pool.write({
                db in
                db.beginTransaction()
                for var i = 1000; i < 2000; i++ {
                    db.update("INSERT INTO test VALUES (?, ?, ?)", withObjects: [i, "Ryan", 24])
                }
                let success = db.commitTransaction()
                XCTAssertTrue(success, "transaction should successfully commit")
            })
        })
        dispatch_group_async(dGroup, dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {
            pool.read({
                db in
                let cursor = db.query("SELECT * FROM test")
                XCTAssertNotNil(cursor, "cursor should not be nil")
            })
        })
        
        dispatch_group_wait(dGroup, DISPATCH_TIME_FOREVER)
        var idArr: [Int] = []
        var nameArr: [String] = []
        var ageArr: [Int] = []
        
        pool.read({
            db in
            if let cursor = db.query("SELECT * FROM test") {
                while cursor.next() {
                    idArr.append(cursor.intForColumnIndex(0)!)
                    nameArr.append(cursor.stringForColumnIndex(1)!)
                    ageArr.append(cursor.intForColumnIndex(0)!)
                }
            }
        })
        
        XCTAssertEqual(idArr.count, 2000, "id array should have 2000 values")
        XCTAssertEqual(nameArr.count, 2000, "name array should have 2000 values")
        XCTAssertEqual(ageArr.count, 2000, "age array should have 2000 values")
    }
    
    func testMaxSustainedConnectionLimit() {
        let pool = SQPool()
        let dGroup = dispatch_group_create()
        
        dispatch_group_async(dGroup, dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {
            pool.read({
                db in
                let cursor = db.query("SELECT * FROM test")
            })
        })
        dispatch_group_async(dGroup, dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {
            pool.read({
                db in
                let cursor = db.query("SELECT * FROM test")
            })
        })
        dispatch_group_async(dGroup, dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {
            pool.read({
                db in
                let cursor = db.query("SELECT * FROM test")
            })
        })
        dispatch_group_async(dGroup, dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {
            pool.read({
                db in
                let cursor = db.query("SELECT * FROM test")
            })
        })
        dispatch_group_async(dGroup, dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {
            pool.read({
                db in
                let cursor = db.query("SELECT * FROM test")
            })
        })
        dispatch_group_async(dGroup, dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {
            pool.read({
                db in
                let cursor = db.query("SELECT * FROM test")
            })
        })
        dispatch_group_async(dGroup, dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {
            pool.read({
                db in
                let cursor = db.query("SELECT * FROM test")
            })
        })
        dispatch_group_wait(dGroup, DISPATCH_TIME_FOREVER)
        XCTAssertLessThan(pool.numberOfFreeConnections(), pool.maxSustainedConnections + 1, "should be less than the max sustained connections")
    }
    
    func testManyAsyncOperations() {
        let pool = SQPool()
        let dGroup = dispatch_group_create()
        
        dispatch_group_async(dGroup, dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {
            pool.write({
                db in
                for var i = 0; i < 1000; i++ {
                    db.update("INSERT INTO test VALUES (?, ?, ?)", withObjects: [i, "Ryan", 24])
                }
            })
        })
        dispatch_group_async(dGroup, dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {
            pool.read({
                db in
                let cursor = db.query("SELECT * FROM test")
                XCTAssertNotNil(cursor, "cursor should not be nil")
                if cursor != nil {
                    while cursor!.next() {
                        cursor!.intForColumn("id")
                        cursor!.stringForColumn("name")
                        cursor!.intForColumn("age")
                    }
                }
            })
        })
        dispatch_group_async(dGroup, dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {
            pool.write({
                db in
                for var i = 1000; i < 2000; i++ {
                    db.update("INSERT INTO test VALUES (?, ?, ?)", withObjects: [i, "Ryan", 24])
                }
            })
        })
        dispatch_group_async(dGroup, dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {
            pool.read({
                db in
                let cursor = db.query("SELECT * FROM test")
                XCTAssertNotNil(cursor, "cursor should not be nil")
                if cursor != nil {
                    while cursor!.next() {
                        cursor!.intForColumn("id")
                        cursor!.stringForColumn("name")
                        cursor!.intForColumn("age")
                    }
                }
            })
        })
        dispatch_group_async(dGroup, dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {
            pool.write({
                db in
                for var i = 2000; i < 3000; i++ {
                    db.update("INSERT INTO test VALUES (?, ?, ?)", withObjects: [i, "Ryan", 24])
                }
            })
        })
        dispatch_group_async(dGroup, dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {
            pool.read({
                db in
                let cursor = db.query("SELECT * FROM test")
                XCTAssertNotNil(cursor, "cursor should not be nil")
                if cursor != nil {
                    while cursor!.next() {
                        cursor!.intForColumn("id")
                        cursor!.stringForColumn("name")
                        cursor!.intForColumn("age")
                    }
                }
            })
        })
        dispatch_group_async(dGroup, dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {
            pool.write({
                db in
                for var i = 3000; i < 4000; i++ {
                    db.update("INSERT INTO test VALUES (?, ?, ?)", withObjects: [i, "Ryan", 24])
                }
            })
        })
        dispatch_group_async(dGroup, dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {
            pool.read({
                db in
                let cursor = db.query("SELECT * FROM test")
                XCTAssertNotNil(cursor, "cursor should not be nil")
                if cursor != nil {
                    while cursor!.next() {
                        cursor!.intForColumn("id")
                        cursor!.stringForColumn("name")
                        cursor!.intForColumn("age")
                    }
                }
            })
        })
        dispatch_group_async(dGroup, dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {
            pool.write({
                db in
                for var i = 4000; i < 5000; i++ {
                    db.update("INSERT INTO test VALUES (?, ?, ?)", withObjects: [i, "Ryan", 24])
                }
            })
        })
        dispatch_group_async(dGroup, dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {
            pool.read({
                db in
                let cursor = db.query("SELECT * FROM test")
                XCTAssertNotNil(cursor, "cursor should not be nil")
                if cursor != nil {
                    while cursor!.next() {
                        cursor!.intForColumn("id")
                        cursor!.stringForColumn("name")
                        cursor!.intForColumn("age")
                    }
                }
            })
        })
        
        dispatch_group_wait(dGroup, DISPATCH_TIME_FOREVER)
        var idArr: [Int] = []
        var nameArr: [String] = []
        var ageArr: [Int] = []
        
        pool.read({
            db in
            if let cursor = db.query("SELECT * FROM test") {
                while cursor.next() {
                    idArr.append(cursor.intForColumnIndex(0)!)
                    nameArr.append(cursor.stringForColumnIndex(1)!)
                    ageArr.append(cursor.intForColumnIndex(2)!)
                }
            }
        })
        
        XCTAssertEqual(idArr.count, 5000, "id array should have 5000 values")
        XCTAssertEqual(nameArr.count, 5000, "name array should have 5000 values")
        XCTAssertEqual(ageArr.count, 5000, "age array should have 5000 values")
        
    }
    
}
