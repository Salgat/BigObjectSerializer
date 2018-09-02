using BigObjectSerializer.Test;
using Newtonsoft.Json.Linq;
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace BigObjectSerializer.Console.Test
{
    class Program
    {
        static void Main(string[] args)
        {
            Task.Run(async () =>
            {
                var count = 1000000;
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
                    serializer.PushObject(benchmarkPoco);
                    serializer.Flush();
                }
                var serializationDuration = timer.ElapsedMilliseconds;
                
                var delayDuration = timer.ElapsedMilliseconds;

                using (var stream = File.Open("test.bin", FileMode.Open))
                using (var deserializer = new BigObjectDeserializer(stream))
                {
                    deserializedBenchmarkPoco = deserializer.PopObject<BenchmarkPoco>();
                }
                var deserializationDuration = timer.ElapsedMilliseconds;

                //System.Console.WriteLine(JObject.FromObject(deserializedBenchmarkPoco).ToString());
                System.Console.WriteLine($"DictionaryValues count: {benchmarkPoco.DictionaryValues.Count()}, DoubleValues count: {benchmarkPoco.DoubleValues.Count}");
                System.Console.WriteLine($"Serialization: {TimeSpan.FromMilliseconds(serializationDuration).TotalSeconds}s, Deserialization: {TimeSpan.FromMilliseconds(deserializationDuration - delayDuration).TotalSeconds}s");
            }).Wait();
            //System.Console.ReadLine();
        }
    }
}
