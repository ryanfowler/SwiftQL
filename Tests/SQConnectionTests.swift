//
//  SQConnection.swift
//  SwiftQL
//
//  Created by Ryan Fowler on 2015-01-19.
//  Copyright (c) 2015 Ryan Fowler. All rights reserved.
//

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
