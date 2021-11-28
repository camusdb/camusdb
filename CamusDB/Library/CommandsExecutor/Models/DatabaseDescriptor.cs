using System;
using System.Collections.Concurrent;
using CamusDB.Library.BufferPool;
using CamusDB.Library.Catalogs.Models;

namespace CamusDB.Library.CommandsExecutor.Models
{
    public class DatabaseDescriptor
    {
        public string? Name { get; set; }

        public BufferPoolHandler? TableSpace { get; set; }

        public BufferPoolHandler? SchemaSpace { get; set; }

        public BufferPoolHandler? SystemSpace { get; set; }

        public Schema Schema { get; set; } = new();

        public SystemSchema SystemSchema { get; set; } = new();

        public SemaphoreSlim DescriptorsSemaphore = new(1, 1);

        public Dictionary<string, TableDescriptor> TableDescriptors = new();
    }
}

