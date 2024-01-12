CamusDB
=======
CamusDB is an innovative open-source database that blends the robust features of NewSQL with the traditional structure of relational databases. It is designed to be ACID-compliant, ensuring reliability in processing transactions. One of its key features is Multi-Version Concurrency Control (MVCC), which allows for efficient handling of simultaneous data transactions, thereby enhancing performance and consistency. 

CamusDB also provides support for SQL, making it accessible and easy to use for those familiar with SQL syntax. Its multi-platform compatibility ensures it can be integrated across various operating systems and environments. Looking towards the future, CamusDB has plans to evolve into a distributed SQL database, promising scalability and improved data management for distributed systems. This forward-thinking approach positions CamusDB as a versatile and future-proof choice for database management needs.

[![Build Status](https://app.travis-ci.com/camusdb/camusdb.svg?branch=main)](https://app.travis-ci.com/camusdb/camusdb)

**This is an alpha project please don't use it in production.**

Features
--------
 - ACID-compliant 
 - Multi-Version Concurrency Control (MVCC) 
 - [SQL](https://es.wikipedia.org/wiki/SQL) language for defining data models and manipulating database information.
 - Single server database (multi-node is planned)
 - Supports communication via a HTTP endpoint (websockets will be supported as well) 

Internal Features
----------------- 
 - Parallel processing of database operations using [Task Parallel Library (TPL)](https://docs.microsoft.com/en-us/dotnet/standard/parallel-programming/task-parallel-library-tpl)
 - Dense and Sparse indexes using [B+Trees](https://en.wikipedia.org/wiki/B%2B_tree) to organize and index rows in the storage layer and for unique/multi indexes
 - [Write Ahead Log (WAL)](https://en.wikipedia.org/wiki/Write-ahead_logging) implementation to provide [Atomicity](https://en.wikipedia.org/wiki/Atomicity_(database_systems)) and [Durability](https://en.wikipedia.org/wiki/Durability_(database_systems))

## Requirements
 - .NET 6 (SDK 6.0.100)

## License

This project is licensed under the [MIT license](LICENSE.txt).

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in CamusDB by you, shall be licensed as MIT, without any additional terms or conditions.


