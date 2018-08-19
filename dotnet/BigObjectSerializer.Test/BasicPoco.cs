using System;
using System.Collections.Generic;
using System.Text;

namespace BigObjectSerializer.Test
{
    public class BasicPoco
    {
        public IEnumerable<int> IntValues { get; set; }
        public int IntValue { get; set; }
        public uint UintValue { get; set; }
        public short ShortValue { get; set; }
        public ushort UShortValue { get; set; }
        public long LongValue { get; set; }
        public ulong ULongValue { get; set; }
        public byte ByteValue { get; set; }
        public bool BoolValue { get; set; }
        public string StringValue { get; set; }
        public float FloatValue { get; set; }
        public double DoubleValue { get; set; }
        public IList<string> StringValues { get; set; }
        public double[] DoubleValues { get; set; }
    }
}
