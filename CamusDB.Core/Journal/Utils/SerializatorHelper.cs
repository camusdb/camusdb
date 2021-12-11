
using System;
using CamusDB.Core.Serializer.Models;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Journal.Utils
{
    public static class SerializatorHelper
    {
        public static int GetColumnValueLength(ColumnValue value)
        {            
            return value.Type switch
            {
                ColumnType.Id or ColumnType.Integer => SerializatorTypeSizes.TypeInteger8 + SerializatorTypeSizes.TypeInteger32,
                ColumnType.String                   => SerializatorTypeSizes.TypeInteger8 + value.Value.Length,
                ColumnType.Bool                     => SerializatorTypeSizes.TypeBool,
                _                                   => throw new Exception("Unsupported column value type"),
            };
        }
    }
}

