using System;

namespace CamusDB.Library.Catalogs.Models
{
    public class TableSchema
    {
        public int Version { get; set; }

        public string? Name { get; set; }

        public List<TableColumnSchema>? Columns { get; set; }
    }
}

