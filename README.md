SwiftQL
=======

SQLite Handling for iOS and OS X in Swift

SwiftQL is a simple wrapper around the SQLite C API written completely in Swift.

This library is the result of lessons learned with [SwiftData](https://github.com/ryanfowler/SwiftData), with syntax inspired by the famous [FMDB](https://github.com/ccgus/fmdb) by Gus Mueller.


## Installation

Currently, the installation process is:

- Clone the SwiftQL repository to your computer
- Drag the ‘SwiftQL’ folder into your project
- Add ‘libsqlite3.dylib’ as a linked framework
- Add ‘#import “sqlite3.h”’ to your Bridging-Header.h file


## System Requirements

Xcode Version:
- Xcode 6.1

Application operating systems:
- iOS 8.0+
- OS X 10.10+


## Usage

Full API documentation is coming soon!

In the meantime, check out some sample usage below.

The public SwiftQL classes are:

**[SQDatabase](#sqdatabase)**
- the basic database class
- for use on a single thread only

**[SQCursor](#sqcursor)**
- created by a query on an SQDatabase
- used to iterate through returned rows from a query

**[SQConnection](#sqconnection)**
- a single database connection class
- safe for use on multiple threads
- all operations are executed in a FIFO order

**[SQPool](#sqpool)**
- a database pool class
- uses WAL journaling mode to allow for concurrent reading and writing on multiple threads

### SQDatabase

The basic database object.

Note: An SQDatabase instance is only to be used in a single thread! For multi-threaded use, please look at [SQConnection](#SQConnection).

Create an SQDatabase instance using either:

```swift
let db = SQDatabase()
// db uses the database “SwiftData.sqlite” located in the Library Directory
```
or
```swift
let customPath = // path to your database
let db = SQDatabase(path: customPath)
// db uses the database specified at customPath
```

A connection to the database can then be opened by calling one of:

```swift
db.open()
// Opens a default ReadWriteCreate connection to the database
```
or
```swift
let customFlags = // Either .ReadOnly, .ReadWrite, or .ReadWriteCreate
db.openWithFlags(customFlags)
```

You can close the connection manually by calling:

```swift
db.close()
```

However, the connection is automatically closed when the database object is released from memory.

To execute all non-query SQL statements (any statement that does not return a value), use the function:

```swift
let success = db.update(“INSERT INTO test VALUES (1, ‘Hello world’)”)
```

Additionally, you can use standard SQLite binding to bind objects to your SQL:

```swift
let success = db.update(“INSERT INTO test VALUES (?, ?)”, withObjects: [1, “Hello world”])
```

To execute multiple non-query SQL statements, use the function:

```swift
let success = db.updateMany([“DROP TABLE IF EXISTS test”, “CREATE TABLE test (id INT PRIMARY KEY, val TEXT)])
```

To execute all query SQL statements (any statement that returns some value), use the function:

```swift
if let cursor = db.query(“SELECT * FROM test”) {
    // query successful
}
```

Additionally, you can use standard SQLite binding to bind objects to your SQL:

```swift
if let cursor = db.query(“SELECT * FROM test”) {
    // query successful
}
```

### SQCursor

In the queries above, an Optional SQCursor is returned.
To iterate through the result rows and obtain column values:

```swift
if let cursor = db.query(“SELECT * FROM test”) {
    while cursor.next() {
        if let id = cursor.intForColumnIndex(1) {
            // column value exists
        }
        if let val = cursor.stringForColumnIndex(2) {
            // column value exists
        }
    }
}
```

Alternatively, you can obtain column values by name:

```swift
if let cursor = db.query(“SELECT * FROM test”) {
    while cursor.next() {
        if let id = cursor.intForColumn(“id”) {
            // column value exists
        }
        if let val = cursor.stringForColumn(“val”) {
            // column value exists
        }
    }
}
```

### SQConnection

An SQConnection object is a safe means of accessing a single SQLite database from multiple threads.

Create an SQDatabase instance using either:

```swift
let conn = SQConnection()
// uses the database “SwiftData.sqlite” located in the Library Directory
```
or
```swift
let customPath = // path to your database
let conn = SQConnection(path: customPath)
// uses the database specified at customPath
```

To execute SQL statements, use the execute function.
It accepts a closure that is provided an SQDatabase instance.
Database operations can then be performed using the supplied SQDatabase:

```swift
conn.execute({
    db in
    // db is an SQDatabase instance
})
```

To execute SQL statements asynchronously, the executeAsync function may be used:

```swift
conn.executeAsync({
    db in
    // use db
})
```

This function will return immediately and execute the provided closure on another thread.

It should be noted that the provided SQDatabase instance is not thread safe itself, meaning that it should only be used in the closure it is provided to!

### SQPool

Documentation coming soon!


## License

SwiftQL is released under the MIT license.

Details are available in the LICENSE file.
