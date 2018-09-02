# BigObjectSerializer

This library attempts to provide a serializer that can handle very large objects efficiently. This includes objects that take up well over 1GB+ in memory. As of now the NewtonSoft JSON.NET serializer typically performs 25% faster while having a file size of roughly double compared to BigObjectSerializer. This is still an early work in progress, with the end goal of matching NewtonSoft's serialization speed. 

## Comparisons

NewtonSoft's JSON.NET serialization is used as the benchmark because it is the fastest available reflection-based serializer. Other serializers were examined, including Protobuf (which does not support very large objects and does not have a reflection-based serializer), Flatbuffers (which requires the entire serialized object to be in memory prior to writing to the stream, and for C# has a 2GB limit), MessagePack (which has a significantly slower reflection-based serializer than NewtonSoft), and BinaryFormatter (which is also significantly slower than NewtonSoft's reflection-based serializer). 

## Implementation

BigObjectSerializer is implemented as a queue (FIFO). This allows us to make assumptions about how the values are stored.

## Examples

### **Reflection based**
Serialization:
```
using (var stream = File.Open("test.bin", FileMode.Create))
using (var serializer = new BigObjectSerializer(stream))
{
    serializer.PushObject(myObject);
    serializer.Flush();
}
```

Deserialization:
```
using (var stream = File.Open("test.bin", FileMode.Open))
using (var deserializer = new BigObjectDeserializer(stream))
{
    var myObject = deserializer.PopObject<MyClass>();
}
```

### **Manual**
Serialization:
```
using (var stream = File.Open("test.bin", FileMode.Create))
using (var serializer = new BigObjectSerializer(stream))
{
    serializer.PushInt(1234);
    serializer.PushString("Test String");
    serializer.PushGuid(Guid.NewGuid());
    serializer.PushDouble(123.456);
    serializer.PushObject(new List<string>() { "one", "two", "three" });

    serializer.Flush();
}
```

Deserialization:
```
using (var stream = File.Open("test.bin", FileMode.Open))
using (var deserializer = new BigObjectDeserializer(stream))
{
    var myInt = deserializer.PopInt();
    var myString = deserializer.PopString();
    var myGuid = deserializer.PopGuid();
    var myDouble = deserializer.PopDouble();
    var myList = deserializer.PopObject<IList<string>>();
}
```

## Supported types

The following are types supported in addition to the reflection-based object serializer, which supports serialization of a class instance's properties that have getter/setters.

* int
* uint
* short
* long
* ulong
* byte
* bool
* float
* double
* string
* Guid

Additionally, the serializer supports types that implement the following interfaces. For now, these types must be used when deserializing (so use PopObject<IList< string >>() instead of PopObject<List< string >>()). The same goes for any nested properties within a class object to be serialized.

* ISet<>
* IDictionary<,>
* IList<>
* IEnumerable<>

In the future more types will be added. Additionally, all types can be pushed and popped using the "PushObject/PopObject", it's just better performance to directly call PushInt/PopInt for example.


## TODO

* Unit tests. I am sure there are many bugs and edge cases to cover (I believe one is Dictionaries with null value entries) that need to be tested.
* As of now only 255 properties per class object are supported. Probably should update from using a byte to a ushort to increase this to a 65k property limit with minimal performance impact.