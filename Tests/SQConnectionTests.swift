//
// SQConnectionTests.swift
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

class SQConnectionTests: XCTestCase {

    override func setUp() {
        super.setUp()
        let db = SQDatabase()
        db.open()
        let success = db.updateMany(["DROP TABLE IF EXISTS test", "CREATE TABLE test (id INT PRIMARY KEY, name TEXT, age INT)"])
        XCTAssertTrue(success, "table should have been created")
        db.close()
    }
    
    func testInsert() {
        let conn = SQConnection()
        conn.execute({
            db in
            let success = db.update("INSERT INTO test VALUES (?, ?, ?)", withObjects: [1, "Ryan", 24])
            XCTAssertTrue(success, "insert should succeed")
        })
    }
    
    func testReadWrite() {
        let conn = SQConnection()
        let dGroup = dispatch_group_create()
        dispatch_group_async(dGroup, dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {
            conn.execute({
                db in
                let success = db.update("INSERT INTO test VALUES (?, ?, ?)", withObjects: [1, "Ryan", 24])
            })
        })
        dispatch_group_async(dGroup, dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {
            conn.execute({
                db in
                let cursor = db.query("SELECT * FROM test")
                XCTAssertNotNil(cursor, "cursor should not be nil")
            })
        })
        dispatch_group_wait(dGroup, DISPATCH_TIME_FOREVER)
    }
    
    func testReadWriteAsync() {
        var idArr: [Int] = []
        var nameArr: [String] = []
        var ageArr: [Int] = []
        let conn = SQConnection()
        let dGroup = dispatch_group_create()
        dispatch_group_async(dGroup, dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {
            let suc = conn.transaction({
                db in
                for var i = 0; i < 1000; i++ {
                    db.update("INSERT INTO test VALUES (?, ?, ?)", withObjects: [i, "Ryan", 24])
                }
                return true
            })
        })
        dispatch_group_async(dGroup, dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {
            conn.execute({
                db in
                let cursor = db.query("SELECT * FROM test")
                XCTAssertNotNil(cursor, "cursor should not be nil")
                if let cursor = cursor {
                    while cursor.next() {
                        //idArr.append(cursor.intForColumn("id")!)
                        //nameArr.append(cursor.stringForColumn("name")!)
                        //ageArr.append(cursor.intForColumn("age")!)
                    }
                }
            })
        })
        dispatch_group_async(dGroup, dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0), {
            conn.execute({
                db in
                let success = db.update("INSERT INTO test VALUES (? , ?, ?)", withObjects: [1000, "Ryan", 24])
                XCTAssertTrue(success, "insert should succeed")
            })
        })
        dispatch_group_wait(dGroup, DISPATCH_TIME_FOREVER)
        conn.execute({
            db in
            let cursor = db.query("SELECT * FROM test")
            XCTAssertNotNil(cursor, "cursor should not be nil")
            if let cursor = cursor {
                while cursor.next() {
                    idArr.append(cursor.intForColumn("id")!)
                    nameArr.append(cursor.stringForColumn("name")!)
                    ageArr.append(cursor.intForColumn("age")!)
                }
            }
        })

        XCTAssertEqual(idArr.count, 1001, "id array should contain 1001 rows")
        XCTAssertEqual(nameArr.count, 1001, "name array should contain 1001 rows")
        XCTAssertEqual(ageArr.count, 1001, "age array should contain 1001 rows")
    }

}
