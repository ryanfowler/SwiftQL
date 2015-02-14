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

class SQCursorTests: XCTestCase {

    override func setUp() {
        super.setUp()
        let db = SQDatabase()
        db.open()
        let success = db.updateMany(["DROP TABLE IF EXISTS test", "CREATE TABLE test (id INT PRIMARY KEY, name TEXT, age INT)"])
        XCTAssertTrue(success, "table should have been created")
        for var i = 0; i < 1000; i++ {
            db.update("INSERT INTO test VALUES (?, ?, ?)", withObjects: [i, "Ryan", 24])
        }
        db.close()
    }
    
    func testBasicCursor() {
        let db = SQDatabase()
        db.open()
        let cursor = db.query("SELECT * FROM test")
        XCTAssertNotNil(cursor, "Cursor should exist")
        db.close()
    }
    
    func testColumnByIndex() {
        let db = SQDatabase()
        db.open()
        if let cursor = db.query("SELECT * FROM test") {
            var i = 0
            while cursor.next() {
                if let id = cursor.intForColumnIndex(0) {
                    XCTAssertEqual(id, i, "id is incorrect")
                } else {
                    XCTAssertTrue(false, "Should have gotten id")
                }
                if let name = cursor.stringForColumnIndex(1) {
                    XCTAssertEqual(name, "Ryan", "Name is incorrect")
                } else {
                    XCTAssertTrue(false, "Should have gotten name")
                }
                if let age = cursor.intForColumnIndex(2) {
                    XCTAssertEqual(age, 24, "Age is incorrect")
                } else {
                    XCTAssertTrue(false, "Should have gotten age")
                }
                i++
            }
        } else {
            XCTAssertTrue(false, "Cursor should exist")
        }
    }
    
    func testColumnByName() {
        let db = SQDatabase()
        db.open()
        if let cursor = db.query("SELECT * FROM test") {
            var i = 0
            while cursor.next() {
                if let id = cursor.intForColumn("id") {
                    XCTAssertEqual(id, i, "id is incorrect")
                } else {
                    XCTAssertTrue(false, "Should have gotten id")
                }
                if let name = cursor.stringForColumn("name") {
                    XCTAssertEqual(name, "Ryan", "Name is incorrect")
                } else {
                    XCTAssertTrue(false, "Should have gotten name")
                }
                if let age = cursor.intForColumn("age") {
                    XCTAssertEqual(age, 24, "Age is incorrect")
                } else {
                    XCTAssertTrue(false, "Should have gotten age")
                }
                i++
            }
        } else {
            XCTAssertTrue(false, "Cursor should exist")
        }
    }

}
