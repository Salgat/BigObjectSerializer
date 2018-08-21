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
        
        public static TheoryData<int> BenchmarkConfigurations
        {
            get
            {
                const int maxValue = 1000000;
                var theoryData = new TheoryData<int>();
                for (var i = 1; i <= maxValue; i *= 10)
                {
                    theoryData.Add(i);
                }
                return theoryData;
            }
        }

        [Theory(DisplayName = "Benchmarks"), MemberData(nameof(BenchmarkConfigurations))]
        public async Task FileSerializationBenchmarks(int count)
        {
            var random = new Random(0);
            var benchmarkPoco = new BenchmarkPoco()
            {
                StringValue = "testString",
                DictionaryValues = Enumerable.Range(0, count).ToDictionary(_ => Guid.NewGuid(), _ => new BenchmarkPoco2()
                {
                    IntValue = random.Next(0, int.MaxValue),
                    StringValue = Guid.NewGuid().ToString(),
                    GuidValue = Guid.NewGuid()
                }),
                DoubleValues = Enumerable.Range(0, count).Select(_ => random.NextDouble() * double.MaxValue).ToList()
            };
            BenchmarkPoco deserializedBenchmarkPoco;

            var timer = new Stopwatch();
            timer.Start();
            using (var stream = File.Open("test.bin", FileMode.Create))
            using (var serializer = new BigObjectSerializer(stream))
            {
                await serializer.PushObjectAsync(benchmarkPoco);
                await serializer.FlushAsync();
            }
            var serializationDuration = timer.ElapsedMilliseconds;

            await Task.Delay(1000); // Give time to release control of file
            var delayDuration = timer.ElapsedMilliseconds;

            using (var stream = File.Open("test.bin", FileMode.Open))
            using (var deserializer = new BigObjectDeserializer(stream))
            {
                deserializedBenchmarkPoco = await deserializer.PopObjectAsync<BenchmarkPoco>();
            }
            var deserializationDuration = timer.ElapsedMilliseconds;
            
            _output.WriteLine($"Serialization: {TimeSpan.FromMilliseconds(serializationDuration).TotalSeconds}s, Deserialization: {TimeSpan.FromMilliseconds(deserializationDuration - delayDuration).TotalSeconds}s");
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
                await serializer.PushIntAsync(intToPush);
                await serializer.PushBoolAsync(boolToPush);
                await serializer.PushShortAsync(shortToPush);
                await serializer.PushStringAsync(stringToPush);
                await serializer.PushByteAsync(byteToPush);
                await serializer.PushUnsignedShortAsync(ushortToPush);
                await serializer.PushUnsignedIntAsync(uintToPush);
                await serializer.PushLongAsync(longToPush);
                await serializer.PushUnsignedLongAsync(ulongToPush);
                await serializer.PushFloatAsync(floatToPush);
                await serializer.PushDoubleAsync(doubleToPush);
                await serializer.FlushAsync();
                
                serializedStream = stream.ToArray();
            }
            
            using (var stream = new MemoryStream(serializedStream))
            using (var deserializer = new BigObjectDeserializer(stream))
            {
                var intVal = await deserializer.PopIntAsync();
                var boolVal = await deserializer.PopBoolAsync();
                var shortVal = await deserializer.PopShortAsync();
                var stringVal = await deserializer.PopStringAsync();
                var byteVal = await deserializer.PopByteAsync();
                var ushortVal = await deserializer.PopUnsignedShortAsync();
                var uintVal = await deserializer.PopUnsignedIntAsync();
                var longVal = await deserializer.PopLongAsync();
                var ulongVal = await deserializer.PopUnsignedLongAsync();
                var floatVal = await deserializer.PopFloatAsync();
                var doubleVal = await deserializer.PopDoubleAsync();

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
                await serializer.PushObjectAsync(basicPoco);
                await serializer.FlushAsync();

                serializedStream = stream.ToArray();
            }

            using (var stream = new MemoryStream(serializedStream))
            using (var deserializer = new BigObjectDeserializer(stream))
            {
                var deserializedBasicPoco = await deserializer.PopObjectAsync<BasicPoco>();

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
                await serializer.PushObjectAsync(basicPoco);
                await serializer.FlushAsync();
            }

            await Task.Delay(1000); // Give time to release control of file

            using (var stream = File.Open("test.bin", FileMode.Open))
            using (var deserializer = new BigObjectDeserializer(stream))
            {
                var deserializedBasicPoco = await deserializer.PopObjectAsync<BasicPoco>();

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
                await serializer.PushObjectAsync(poco);
                await serializer.FlushAsync();

                serializedStream = stream.ToArray();
            }

            using (var stream = new MemoryStream(serializedStream))
            using (var deserializer = new BigObjectDeserializer(stream))
            {
                var deserializedBasicPoco = await deserializer.PopObjectAsync<Poco>();

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
