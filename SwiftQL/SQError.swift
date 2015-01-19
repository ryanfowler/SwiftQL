//
// SQError.swift
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


public struct SQError {
    
    public static func printSQLError(main: String, errCode: Int32, errMsg: String?) {
        println("SwiftData Error -> \(main)")
        println("                -> Code: \(errCode) - \(getSQLErrorMsg(errCode))")
        if let msg = errMsg {
            println("                -> Detail: \(msg)")
        }
        getAdvice(errCode)
    }
    
    public static func printError(main: String, next: String ...) {
        println("SwiftData Error -> \(main)")
        for item in next {
            println("                -> \(item)")
        }
    }
    
    public static func printWarning(main: String, next: String ...) {
        println("SwiftData Warning -> \(main)")
        for item in next {
            println("                  -> \(item)")
        }
    }
    
    private static func getAdvice(errCode: Int32) {
        
        if errCode == SQLITE_MISUSE || errCode == SQLITE_NOMEM {
            println("                -> Possible cause: Did you call open()?")
        }
        
        if errCode == SQLITE_BUSY || errCode == SQLITE_LOCKED {
            println("                -> Possible cause: Only one 'update' can occur at a time")
        }
        
    }
    
    private static func getSQLErrorMsg(code: Int32) -> String {
        
        switch code {
            
            //SQLite error codes and descriptions as per: http://www.sqlite.org/c3ref/c_abort.html
        case SQLITE_OK:
            return "Successful result"
        case SQLITE_ERROR:
            return "SQL error or missing database"
        case SQLITE_INTERNAL:
            return "Internal logic error in SQLite"
        case SQLITE_PERM:
            return "Access permission denied"
        case SQLITE_ABORT:
            return "Callback routine requested an abort"
        case SQLITE_BUSY:
            return "The database file is locked"
        case SQLITE_LOCKED:
            return "A table in the database is locked"
        case SQLITE_NOMEM:
            return "A malloc() failed"
        case SQLITE_READONLY:
            return "Attempt to write a readonly database"
        case SQLITE_INTERRUPT:
            return "Operation terminated by sqlite3_interrupt()"
        case SQLITE_IOERR:
            return "Some kind of disk I/O error occurred"
        case SQLITE_CORRUPT:
            return "The database disk image is malformed"
        case SQLITE_NOTFOUND:
            return "Unknown opcode in sqlite3_file_control()"
        case SQLITE_FULL:
            return "Insertion failed because database is full"
        case SQLITE_CANTOPEN:
            return "Unable to open the database file"
        case SQLITE_PROTOCOL:
            return "Database lock protocol error"
        case SQLITE_EMPTY:
            return "Database is empty"
        case SQLITE_SCHEMA:
            return "The database schema changed"
        case SQLITE_TOOBIG:
            return "String or BLOB exceeds size limit"
        case SQLITE_CONSTRAINT:
            return "Abort due to constraint violation"
        case SQLITE_MISMATCH:
            return "Data type mismatch"
        case SQLITE_MISUSE:
            return "Library used incorrectly"
        case SQLITE_NOLFS:
            return "Uses OS features not supported on host"
        case SQLITE_AUTH:
            return "Authorization denied"
        case SQLITE_FORMAT:
            return "Auxiliary database format error"
        case SQLITE_RANGE:
            return "2nd parameter to sqlite3_bind out of range"
        case SQLITE_NOTADB:
            return "File opened that is not a database file"
        case SQLITE_NOTICE:
            return "Notifications from sqlite3_log()"
        case SQLITE_WARNING:
            return "Warnings from sqlite3_log()"
        case SQLITE_ROW:
            return "sqlite3_step() has another row ready"
        case SQLITE_DONE:
            return "sqlite3_step() has finished executing"
        default:
            return "Unknown SQLite error"
        }
        
    }
    
}

// fix for Swift limitations with C
private let SQLITE_NOTICE: Int32 = 27
private let SQLITE_WARNING: Int32 = 28