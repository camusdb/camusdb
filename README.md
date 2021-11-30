CamusDB
=======
CamusDB is a multi-platform lightweight NoSQL/strict schema database server written in C# and .NET 6.

[![Build Status](https://app.travis-ci.com/camusdb/camusdb.svg?branch=main)](https://app.travis-ci.com/camusdb/camusdb)

**This is an alpha project please don't use it in production.**

Features
--------
 - Single server database (multi-node is planned)
 - Supports communication via a HTTP endpoint (websockets will be supported as well)

Internal Features
-----------------
 - Parallel processing of database operations using [Task Parallel Library (TPL)](https://docs.microsoft.com/en-us/dotnet/standard/parallel-programming/task-parallel-library-tpl)
 - [Memory-mapped files](https://en.wikipedia.org/wiki/Memory-mapped_file) to avoid double buffering
 - [B+Trees](https://en.wikipedia.org/wiki/B%2B_tree) to organize rows in storage and for unique/multi indexes

Requirements
------------
 - .NET 6 (SDK 6.0.100)

