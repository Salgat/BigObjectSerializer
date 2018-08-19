using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Xunit;

namespace BigObjectSerializer.Test
{
    public class Benchmarks
    {
        public static TheoryData<int> BenchmarkConfigurations
        {
            get
            {
                var theoryData = new TheoryData<int>();
                for (var i = 1; i <= 1000; i *= 10)
                {
                    theoryData.Add(i);
                }
                return theoryData;
            }
        }

        //[Theory(DisplayName = "Benchmarks"), MemberData(nameof(BenchmarkConfigurations))]
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
                throw new NotImplementedException();
                //var objectVal = await deserializer.PopObjectAsync<BasicPoco>();
            }
        }
    }
}
