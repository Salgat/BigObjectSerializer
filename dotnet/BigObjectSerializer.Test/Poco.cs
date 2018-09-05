using System;
using System.Collections.Generic;
using System.Text;

namespace BigObjectSerializer.Test
{
    public class Poco
    {
        public IEnumerable<string> StringEnumerableValues { get; set; }
        public int IntValue { get; set; }
        public IDictionary<Guid, double> GuidDoubleDictionaryValues { get; set; }
        public long LongValue;
        public IList<PocoLevel2> PocoLevel2Values { get; set; }
        public string StringValue { get; set; }
    }

    public class PocoLevel2
    {
        public string StringValue { get; set; }
        public Guid GuidValue { get; set; }
        public ISet<int> IntValues { get; set; }
        public PocoLevel3 PocoLevel3Value { get; set; }
    }

    public class PocoLevel3
    {
        public double DoubleValue { get; set; }
        public byte[] ByteValues { get; set; }
        public PocoLevel4 PocoLevel4Value { get; set; }
    }

    public struct PocoLevel4
    {
        public float FloatValue { get; set; }
    }

    public class BenchmarkPoco
    {
        public string StringValue { get; set; }
        public IDictionary<Guid, BenchmarkPoco2> DictionaryValues { get; set; }
        public IList<double> DoubleValues { get; set; }
    }

    public class BenchmarkPoco2
    {
        public int IntValue { get; set; }
        public string StringValue { get; set; }
        public Guid GuidValue { get; set; }
    }
}
