using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace BigObjectSerializer.Test
{
    public class Benchmarks
    {
        private readonly ITestOutputHelper _output;

        public Benchmarks(ITestOutputHelper output)
        {
            _output = output;
        }
        
        public static TheoryData<int, bool> BenchmarkConfigurations
        {
            get
            {
                var theorySet = new TheoryData<int, bool>();
                var sampleCounts = new List<int>()
                {
                    1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000 //1000000, 5000000, 15000000
                };
                foreach (var sampleCount in sampleCounts)
                {
                    theorySet.Add(sampleCount, false);
                    theorySet.Add(sampleCount, true);
                }
                return theorySet;
            }
        }

        [Theory(DisplayName = "Benchmarks"), MemberData(nameof(BenchmarkConfigurations))]
        public async Task FileSerializationBenchmarks(int sampleSize, bool useJson)
        {
            var random = new Random(0);
            var benchmarkPoco = new BenchmarkPoco()
            {
                StringValue = "testString",
                DictionaryValues = Enumerable.Range(0, sampleSize).ToDictionary(_ => Guid.NewGuid(), _ => new BenchmarkPoco2()
                {
                    IntValue = random.Next(0, int.MaxValue),
                    StringValue = Guid.NewGuid().ToString(),
                    GuidValue = Guid.NewGuid()
                }),
                DoubleValues = Enumerable.Range(0, sampleSize).Select(_ => random.NextDouble() * double.MaxValue).ToList()
            };
            BenchmarkPoco deserializedBenchmarkPoco;

            var timer = new Stopwatch();
            timer.Start();

            if (!useJson)
            {
                using (var stream = File.Open("test.bin", FileMode.Create))
                using (var serializer = new BigObjectSerializer(stream))
                {
                    serializer.PushObject(benchmarkPoco);
                    serializer.Flush();
                }
            }
            else
            {
                using (var fileStream = new FileStream("test.json", FileMode.Create))
                using (var writer = new StreamWriter(fileStream))
                using (var jsonWriter = new JsonTextWriter(writer))
                {
                    var ser = new JsonSerializer();
                    ser.Serialize(jsonWriter, benchmarkPoco);
                    jsonWriter.Flush();
                }
            }
            var serializationDuration = timer.ElapsedMilliseconds;

            if (!useJson)
            {
                using (var stream = File.Open("test.bin", FileMode.Open))
                using (var deserializer = new BigObjectDeserializer(stream))
                {
                    deserializedBenchmarkPoco = deserializer.PopObject<BenchmarkPoco>();
                }
            }
            else
            {
                using (var fileStream = File.Open("test.json", FileMode.Open))
                using (var reader = new StreamReader(fileStream))
                using (var jsonReader = new JsonTextReader(reader))
                {
                    var ser = new JsonSerializer();
                    ser.Deserialize<BenchmarkPoco>(jsonReader);
                }
            }
            var deserializationDuration = timer.ElapsedMilliseconds;

            _output.WriteLine($"Serialization: {TimeSpan.FromMilliseconds(serializationDuration).TotalSeconds}s, Deserialization: {TimeSpan.FromMilliseconds(deserializationDuration - serializationDuration).TotalSeconds}s");
        }

        [Fact]
        public async Task BasicStringSerialization()
        {
            byte[] serializedStream;
            var stringToPush = "Test";

            using (var stream = new MemoryStream())
            using (var serializer = new BigObjectSerializer(stream))
            {
                serializer.PushString(stringToPush);
                serializer.Flush();

                serializedStream = stream.ToArray();
            }

            using (var stream = new MemoryStream(serializedStream))
            using (var deserializer = new BigObjectDeserializer(stream))
            {
                var stringVal = deserializer.PopString();
                
                Assert.Equal(stringToPush, stringVal);
            }
        }

        [Fact]
        public async Task BasicSerialization()
        {
            byte[] serializedStream;

            int intToPush = 1;
            bool boolToPush = true;
            short shortToPush = 16;
            string stringToPush = "test";
            byte byteToPush = 0x3F;
            ushort ushortToPush = 17;
            uint uintToPush = 18;
            long longToPush = 400000000000000L;
            ulong ulongToPush = 400000000000001UL;
            float floatToPush = 1.5f;
            double doubleToPush = 15.3d;
            
            using (var stream = new MemoryStream())
            using (var serializer = new BigObjectSerializer(stream))
            {
                serializer.PushInt(intToPush);
                serializer.PushBool(boolToPush);
                serializer.PushShort(shortToPush);
                serializer.PushString(stringToPush);
                serializer.PushByte(byteToPush);
                serializer.PushUnsignedShort(ushortToPush);
                serializer.PushUnsignedInt(uintToPush);
                serializer.PushLong(longToPush);
                serializer.PushUnsignedLong(ulongToPush);
                serializer.PushFloat(floatToPush);
                serializer.PushDouble(doubleToPush);
                serializer.Flush();
                
                serializedStream = stream.ToArray();
            }
            
            using (var stream = new MemoryStream(serializedStream))
            using (var deserializer = new BigObjectDeserializer(stream))
            {
                var intVal = deserializer.PopInt();
                var boolVal = deserializer.PopBool();
                var shortVal = deserializer.PopShort();
                var stringVal = deserializer.PopString();
                var byteVal = deserializer.PopByte();
                var ushortVal = deserializer.PopUnsignedShort();
                var uintVal = deserializer.PopUnsignedInt();
                var longVal = deserializer.PopLong();
                var ulongVal = deserializer.PopUnsignedLong();
                var floatVal = deserializer.PopFloat();
                var doubleVal = deserializer.PopDouble();

                Assert.Equal(intToPush, intVal);
                Assert.Equal(boolToPush, boolVal);
                Assert.Equal(shortToPush, shortVal);
                Assert.Equal(stringToPush, stringVal);
                Assert.Equal(byteToPush, byteVal);
                Assert.Equal(ushortToPush, ushortVal);
                Assert.Equal(uintToPush, uintVal);
                Assert.Equal(longToPush, longVal);
                Assert.Equal(ulongToPush, ulongVal);
                Assert.Equal(floatToPush, floatVal);
                Assert.Equal(doubleToPush, doubleVal);
            }
        }

        [Fact]
        public async Task BasicReflectiveSerialization()
        {
            byte[] serializedStream;

            var basicPoco = new BasicPoco()
            {
                NullStringValue = null,
                IntValues = new List<int>() { 56, 57, 58 },
                IntValue = 238,
                UintValue = 543,
                ShortValue = 12,
                UShortValue = 42,
                LongValue = 400002340000000L,
                ULongValue = 600000964000000UL,
                ByteValue = 0xF3,
                BoolValue = true,
                FloatValue = 921523.129521f,
                DoubleValue = 192510921421.012351298d,
                StringValue = "testString",
                StringValues = new List<string>() { "first", "second", "third"},
                DoubleValues = new [] { 24521.523d, 12451251.9957d }
            };

            using (var stream = new MemoryStream())
            using (var serializer = new BigObjectSerializer(stream))
            {
                serializer.PushObject(basicPoco);
                serializer.Flush();

                serializedStream = stream.ToArray();
            }

            using (var stream = new MemoryStream(serializedStream))
            using (var deserializer = new BigObjectDeserializer(stream))
            {
                var deserializedBasicPoco = deserializer.PopObject<BasicPoco>();

                Assert.Equal(basicPoco.NullStringValue, deserializedBasicPoco.NullStringValue);
                Assert.True(basicPoco.IntValues.SequenceEqual(deserializedBasicPoco.IntValues));
                Assert.Equal(basicPoco.IntValue, deserializedBasicPoco.IntValue);
                Assert.Equal(basicPoco.UintValue, deserializedBasicPoco.UintValue);
                Assert.Equal(basicPoco.ShortValue, deserializedBasicPoco.ShortValue);
                Assert.Equal(basicPoco.UShortValue, deserializedBasicPoco.UShortValue);
                Assert.Equal(basicPoco.LongValue, deserializedBasicPoco.LongValue);
                Assert.Equal(basicPoco.ULongValue, deserializedBasicPoco.ULongValue);
                Assert.Equal(basicPoco.ByteValue, deserializedBasicPoco.ByteValue);
                Assert.Equal(basicPoco.FloatValue, deserializedBasicPoco.FloatValue);
                Assert.Equal(basicPoco.DoubleValue, deserializedBasicPoco.DoubleValue);
                Assert.Equal(basicPoco.StringValue, deserializedBasicPoco.StringValue);
                Assert.True(basicPoco.StringValues.SequenceEqual(deserializedBasicPoco.StringValues));
                Assert.True(basicPoco.DoubleValues.SequenceEqual(deserializedBasicPoco.DoubleValues));
            }
        }

        [Fact]
        public async Task BasicFileStreamReflectiveSerialization()
        {
            var basicPoco = new BasicPoco()
            {
                NullStringValue = null,
                IntValues = new List<int>() { 56, 57, 58 },
                IntValue = 238,
                UintValue = 543,
                ShortValue = 12,
                UShortValue = 42,
                LongValue = 400002340000000L,
                ULongValue = 600000964000000UL,
                ByteValue = 0xF3,
                BoolValue = true,
                FloatValue = 921523.129521f,
                DoubleValue = 192510921421.012351298d,
                StringValue = "testString",
                StringValues = new List<string>() { "first", "second", "third" },
                DoubleValues = new[] { 24521.523d, 12451251.9957d }
            };

            using (var stream = File.Open("test.bin", FileMode.Create))
            using (var serializer = new BigObjectSerializer(stream))
            {
                serializer.PushObject(basicPoco);
                serializer.Flush();
            }

            await Task.Delay(1000); // Give time to release control of file

            using (var stream = File.Open("test.bin", FileMode.Open))
            using (var deserializer = new BigObjectDeserializer(stream))
            {
                var deserializedBasicPoco = deserializer.PopObject<BasicPoco>();

                Assert.Equal(basicPoco.NullStringValue, deserializedBasicPoco.NullStringValue);
                Assert.True(basicPoco.IntValues.SequenceEqual(deserializedBasicPoco.IntValues));
                Assert.Equal(basicPoco.IntValue, deserializedBasicPoco.IntValue);
                Assert.Equal(basicPoco.UintValue, deserializedBasicPoco.UintValue);
                Assert.Equal(basicPoco.ShortValue, deserializedBasicPoco.ShortValue);
                Assert.Equal(basicPoco.UShortValue, deserializedBasicPoco.UShortValue);
                Assert.Equal(basicPoco.LongValue, deserializedBasicPoco.LongValue);
                Assert.Equal(basicPoco.ULongValue, deserializedBasicPoco.ULongValue);
                Assert.Equal(basicPoco.ByteValue, deserializedBasicPoco.ByteValue);
                Assert.Equal(basicPoco.FloatValue, deserializedBasicPoco.FloatValue);
                Assert.Equal(basicPoco.DoubleValue, deserializedBasicPoco.DoubleValue);
                Assert.Equal(basicPoco.StringValue, deserializedBasicPoco.StringValue);
                Assert.True(basicPoco.StringValues.SequenceEqual(deserializedBasicPoco.StringValues));
                Assert.True(basicPoco.DoubleValues.SequenceEqual(deserializedBasicPoco.DoubleValues));
            }
        }
        
        [Fact]
        public async Task ReflectiveSerialization()
        {
            byte[] serializedStream;

            var poco = new Poco()
            {
                StringEnumerableValues = new List<string>() { "first", "second", "third" },
                IntValue = 682310292,
                GuidDoubleDictionaryValues = new Dictionary<Guid, double>()
                {
                    [Guid.NewGuid()] = 125123904.129512d,
                    [Guid.NewGuid()] = 9783125.3843209d,
                    [Guid.NewGuid()] = 4532.123d,
                    [Guid.NewGuid()] = 821639.12942d
                },
                PocoLevel2Values = Enumerable.Range(0, 100).Select(i => new PocoLevel2()
                {
                    StringValue = $"{i}_{i * 100}",
                    GuidValue = Guid.NewGuid(),
                    IntValues = new HashSet<int>(Enumerable.Range(0, 50).Select(i2 => i2)),
                    PocoLevel3Value = new PocoLevel3()
                    {
                        DoubleValue = 5932.23d,
                        ByteValues = Enumerable.Range(0, 10).Select(i3 => (byte)i3).ToArray()
                    }
                }).ToList(),
                StringValue = "testValue"
            };

            using (var stream = new MemoryStream())
            using (var serializer = new BigObjectSerializer(stream))
            {
                serializer.PushObject(poco);
                serializer.Flush();

                serializedStream = stream.ToArray();
            }

            using (var stream = new MemoryStream(serializedStream))
            using (var deserializer = new BigObjectDeserializer(stream))
            {
                var deserializedBasicPoco = deserializer.PopObject<Poco>();

                Assert.True(poco.StringEnumerableValues.SequenceEqual(deserializedBasicPoco.StringEnumerableValues));
                Assert.Equal(poco.IntValue, deserializedBasicPoco.IntValue);
                Assert.Equal(poco.GuidDoubleDictionaryValues.Count, deserializedBasicPoco.GuidDoubleDictionaryValues.Count);
                Assert.True(poco.GuidDoubleDictionaryValues.All(kv => 
                    deserializedBasicPoco.GuidDoubleDictionaryValues.ContainsKey(kv.Key) && 
                    kv.Value == deserializedBasicPoco.GuidDoubleDictionaryValues[kv.Key]));
                for (var i = 0; i < poco.PocoLevel2Values.Count; ++i)
                {
                    Assert.Equal(poco.PocoLevel2Values[i].StringValue, deserializedBasicPoco.PocoLevel2Values[i].StringValue);
                    Assert.Equal(poco.PocoLevel2Values[i].GuidValue, deserializedBasicPoco.PocoLevel2Values[i].GuidValue);
                    Assert.True(poco.PocoLevel2Values[i].IntValues.OrderBy(e => e).SequenceEqual(
                        deserializedBasicPoco.PocoLevel2Values[i].IntValues.OrderBy(e => e)));
                    Assert.Equal(poco.PocoLevel2Values[i].PocoLevel3Value.DoubleValue, deserializedBasicPoco.PocoLevel2Values[i].PocoLevel3Value.DoubleValue);
                    Assert.True(poco.PocoLevel2Values[i].PocoLevel3Value.ByteValues.SequenceEqual(deserializedBasicPoco.PocoLevel2Values[i].PocoLevel3Value.ByteValues));
                }
            }
        }
    }
}
