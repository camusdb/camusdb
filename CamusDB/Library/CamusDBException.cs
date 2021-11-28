using System;

namespace CamusDB.Library
{
    public class CamusDBException : Exception
    {
        public CamusDBException(string? msg) : base(msg)
        {
        }
    }
}

