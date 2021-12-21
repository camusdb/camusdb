CamusDB
=======
CamusDB is a modern multi-platform lightweight NoSQL/strict schema database server. 

[![Build Status](https://app.travis-ci.com/camusdb/camusdb.svg?branch=main)](https://app.travis-ci.com/camusdb/camusdb)

**This is an alpha project please don't use it in production.**

Features
--------
 - Single server database (multi-node is planned)
 - Supports communication via a HTTP endpoint (websockets will be supported as well)
 - It can be embedded into a .NET project to avoid communication overhead

Internal Features
-----------------
 - [Write Ahead Log (WAL)](https://en.wikipedia.org/wiki/Write-ahead_logging) implementation to provide [Atomicity](https://en.wikipedia.org/wiki/Atomicity_(database_systems)) and [Durability](https://en.wikipedia.org/wiki/Durability_(database_systems))
 - Parallel processing of database operations using [Task Parallel Library (TPL)](https://docs.microsoft.com/en-us/dotnet/standard/parallel-programming/task-parallel-library-tpl)
 - [Memory-mapped files](https://en.wikipedia.org/wiki/Memory-mapped_file) to avoid double buffering
 - Dense and Sparse clustered indexes using [B+Trees](https://en.wikipedia.org/wiki/B%2B_tree) to organize and index rows in the storage layer and for unique/multi indexes
 - Data integrity is checked computing [XXHash](https://cyan4973.github.io/xxHash/) checksums to report memory/disk corruption

## Requirements
 - .NET 6 (SDK 6.0.100)

## License

This project is licensed under the [MIT license](LICENSE.txt).

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in CamusDB by you, shall be licensed as MIT, without any additional
terms or conditions.


