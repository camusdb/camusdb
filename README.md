CamusDB
=======
CamusDB is an innovative open-source database that blends the robust features of NewSQL (planned) with the traditional structure of relational databases. It is designed to be ACID-compliant, ensuring reliability in processing transactions. One of its key features is Multi-Version Concurrency Control (MVCC), which allows for efficient handling of simultaneous data transactions, thereby enhancing performance and consistency. 

CamusDB also provides support for dialect of SQL, making it accessible and easy to use for those familiar with SQL syntax. Its multi-platform compatibility ensures it can be integrated across various operating systems and environments. Looking towards the future, CamusDB has plans to evolve into a distributed SQL database, promising scalability and improved data management for distributed systems. This forward-thinking approach positions CamusDB as a versatile and future-proof choice for database management needs.

[![Build Status](https://app.travis-ci.com/camusdb/camusdb.svg?branch=main)](https://app.travis-ci.com/camusdb/camusdb)

**This is an alpha project please don't use it in production.**

![camus-cli](https://media.giphy.com/media/vqs2XqX5mAxC4Ln0FO/giphy.gif)

Features
--------
 - *ACID-compliant:* Ensures reliable transaction processing with Atomicity, Consistency, Isolation, and Durability. Guarantees complete success or failure of transactions, maintaining data integrity and reliability.
 - *Multi-Version Concurrency Control (MVCC)*: Implements MVCC for efficient concurrent operations, minimizing conflicts and locks. Enhances performance in multi-user environments by allowing multiple versions of data for different transactions.
 - *Optimized for Fast Storage:* Specifically designed for fast, low-latency storage like SSDs, providing swift data access.
 - *Snapshot and Checkpoint Features:* Supports creating snapshots of the database state at any point in time, aiding in data backup and recovery.
 - *Compression and Compaction:* Provides data compression options to save storage space and compaction to maintain database performance over time.
 - *SQL language dialect:* for defining data models and manipulating database information.

Internal Features
----------------- 
 - Uses a Log-Structured Merge-tree [(LSM tree)](https://en.wikipedia.org/wiki/Log-structured_merge-tree) for efficient write and read operations, especially suited for workloads with a high rate of writes.
 - Buffer Pool optimizes memory usage and enhances data access speed. It acts as a cache for frequently accessed data stored in the database, reducing the need for disk reads.
 - Garbage Collector feature is designed to maintain database efficiency and storage optimization. It periodically removes outdated MVCC (Multi-Version Concurrency Control) versions that are no longer needed, freeing up space and resources. This process also runs compaction, reorganizing data to optimize storage layout and improve data retrieval speeds.
 - [Write Ahead Log (WAL)](https://en.wikipedia.org/wiki/Write-ahead_logging) implementation to provide [Atomicity](https://en.wikipedia.org/wiki/Atomicity_(database_systems)) and [Durability](https://en.wikipedia.org/wiki/Durability_(database_systems)). Helps to recover from node failures.
 - Parallel processing of database operations using [Task Parallel Library (TPL)](https://docs.microsoft.com/en-us/dotnet/standard/parallel-programming/task-parallel-library-tpl)
 - Dense and Sparse indexes using [B+Trees](https://en.wikipedia.org/wiki/B%2B_tree) to organize and index rows in the storage layer and for unique/multi indexes
 
Current Limitations
-------------------
 - Most features are in active development and may be unstable or subject to changes.
 - Single server database (multi-node cluster is planned)
 - Supports communication via a HTTP endpoint (websockets/grpc2 will be supported as well) 

## Requirements
 - .NET 7 (SDK 7.0.100)

## License

This project is licensed under the [MIT license](LICENSE.txt).

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in CamusDB by you, shall be licensed as MIT, without any additional terms or conditions.
