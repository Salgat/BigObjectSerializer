using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BigObjectSerializer
{
    public class BigObjectDeserializer : IDisposable
    {
        private Action<byte[], int> _readFunc;
        private readonly byte[] _inputBuffer = new byte[8*1000000];
        private readonly byte[] _inputBufferOld = new byte[LargestBufferObject]; // This holds the last 8 bytes of the old buffer
        private const int LargestBufferObject = 8; // 8 bytes (long - 64 bits)
        private int _inputBufferOffset = 0;
        private bool _initialized = false;
        private static readonly object[] _emptyArray = new object[0];

        // Safely copying byte contents of float and double derived from https://github.com/google/flatbuffers/blob/master/net/FlatBuffers/ByteBuffer.cs
        private float[] _floatBuffer = new[] { 0.0f };
        private int[] _intBuffer = new[] { 0 };
        private double[] _doubleBuffer = new[] { 0.0 };
        private ulong[] _ulongBuffer = new[] { 0UL };

        // Reflection
        private readonly IDictionary<Type, IImmutableDictionary<string, PropertyInfo>> _propertiesByType = new Dictionary<Type, IImmutableDictionary<string, PropertyInfo>>();
        private readonly IImmutableDictionary<Type, Func<object>> _basicTypePopMethods; // Types that directly map to basic Pop Methods (such as PopInt)
        private static readonly IImmutableDictionary<Type, PropertyInfo> _basicTypeGenericTaskResult; // Types that directly map to basic Pop Methods (such as PopInt)
        private readonly IImmutableSet<Type> _popValueTypes; // Types that directly map to supported PopValue methods (basic types and collections)
        private readonly IDictionary<Type, ConstructorInfo> _keyValueConstructors = new Dictionary<Type, ConstructorInfo>();
        private readonly IDictionary<Type, Type[]> _genericArguments = new Dictionary<Type, Type[]>();
        private readonly IDictionary<Type, Type> _makeGenericTypeDictionary = new Dictionary<Type, Type>();
        private readonly IDictionary<Type, Type> _makeGenericTypeHashset = new Dictionary<Type, Type>();
        private readonly IDictionary<Type, IImmutableDictionary<byte, string>> _propertyToByteMapping = new Dictionary<Type, IImmutableDictionary<byte, string>>();
        private readonly IDictionary<Type, bool> _isKeyValuePair = new Dictionary<Type, bool>();

        static BigObjectDeserializer()
        {
        }
        
        public BigObjectDeserializer(Action<byte[], int> readFunc) : this()
        {
            _readFunc = readFunc;
        }

        public BigObjectDeserializer(Stream inputStream) : this()
        {
            _readFunc = (inputBuffer, readCount) =>
            {
                var byteCount = inputStream.Read(inputBuffer, 0, readCount);
                /*if (byteCount != readCount)
                {
                    throw new ArgumentException($"Expected to read count of {readCount} but read {byteCount}.");
                }*/
            };
        }

        private BigObjectDeserializer()
        {
            var popMethods = new Dictionary<Type, Func<object>>
            {
                [typeof(int)] = () => PopInt(),
                [typeof(uint)] = () => PopUnsignedInt(),
                [typeof(short)] = () => PopShort(),
                [typeof(ushort)] = () => PopUnsignedShort(),
                [typeof(long)] = () => PopLong(),
                [typeof(ulong)] = () => PopUnsignedLong(),
                [typeof(byte)] = () => PopByte(),
                [typeof(bool)] = () => PopBool(),
                [typeof(float)] = () => PopFloat(),
                [typeof(double)] = () => PopDouble(),
                [typeof(string)] = () => PopString(),
                [typeof(Guid)] = () => PopGuid(),
            };
            _basicTypePopMethods = popMethods.ToImmutableDictionary();

            var popValueTypes = popMethods.Select(kv => kv.Key).ToList();
            popValueTypes.Add(typeof(ISet<>));
            popValueTypes.Add(typeof(IDictionary<,>));
            popValueTypes.Add(typeof(IList<>));
            popValueTypes.Add(typeof(IEnumerable<>));
            _popValueTypes = popValueTypes.ToImmutableHashSet();
        }

        #region Pop

        private ulong Read(int count)
        {
            ulong result = 0;

            var inputBufferOffset = _inputBufferOffset;
            var inputBufferOffsetPlusCount = _inputBufferOffset + count;
            var useOldBuffer = false;
            if (!_initialized || inputBufferOffsetPlusCount >= _inputBuffer.Length)
            {
                if (_initialized)
                {
                    if (_inputBufferOffset != _inputBuffer.Length)
                    {
                        Buffer.BlockCopy(_inputBuffer, _inputBuffer.Length - LargestBufferObject, _inputBufferOld, 0, LargestBufferObject);
                        useOldBuffer = true;
                    }
                }
                else
                {
                    _initialized = true;
                }

                _readFunc(_inputBuffer, _inputBuffer.Length);
                _inputBufferOffset = (inputBufferOffsetPlusCount) % LargestBufferObject;
            }
            else
            {
                _inputBufferOffset = inputBufferOffsetPlusCount;
            }
            
            for (var i = 0; i < count; ++i)
            {
                var index = inputBufferOffset + i;
                if (index >= _inputBuffer.Length)
                {
                    result |= (ulong)_inputBuffer[index % 8] << i * 8;
                }
                else if (!useOldBuffer)
                {
                    result |= (ulong)_inputBuffer[index] << i * 8;
                }
                else
                {
                    result |= (ulong)_inputBufferOld[index % 8] << i * 8;
                }
            }
            
            return result;
        }

        public string PopString()
        {
            var length = PopInt(); // Strings start with an int value of the string length
            if (length == 0) return string.Empty;

            var characterBytes = PopBytes(length);
            return Encoding.UTF8.GetString(characterBytes);
        }

        private byte ReadByte()
        {
            if (!_initialized || _inputBufferOffset >= _inputBuffer.Length)
            {
                _initialized = true;
                _readFunc(_inputBuffer, _inputBuffer.Length);
                _inputBufferOffset = 0;
            }
            _inputBufferOffset += 1;
            return _inputBuffer[_inputBufferOffset - 1];
        }

        public int PopInt()
            => (int)Read(sizeof(int));

        public uint PopUnsignedInt()
            => (uint)Read(sizeof(int));

        public short PopShort()
            => (short)Read(sizeof(short));

        public ushort PopUnsignedShort()
            => (ushort)Read(sizeof(ushort));

        public long PopLong()
            => (long)Read(sizeof(long));

        public ulong PopUnsignedLong()
            => Read(sizeof(ulong));

        public bool PopBool()
            => ReadByte() == (byte)0x1;

        public float PopFloat()
        {
            _intBuffer[0] = PopInt();
            Buffer.BlockCopy(_intBuffer, 0, _floatBuffer, 0, sizeof(float));
            return _floatBuffer[0];
        }

        public double PopDouble()
        {
            _ulongBuffer[0] = PopUnsignedLong();
            Buffer.BlockCopy(_ulongBuffer, 0, _doubleBuffer, 0, sizeof(double));
            return _doubleBuffer[0];
        }

        public Guid PopGuid()
            => new Guid(PopBytes(16));

        public byte PopByte()
            => ReadByte();

        public byte[] PopBytes(int count)
        {
            var result = new byte[count];
            var bytesRead = 0;
            while (bytesRead != count)
            {
                var bytesAvailableToReadFromBuffer = _inputBuffer.Length - _inputBufferOffset;
                if (bytesAvailableToReadFromBuffer == 0)
                {
                    _readFunc(_inputBuffer, _inputBuffer.Length);
                    _inputBufferOffset = 0;
                }

                var bytesToRead = bytesAvailableToReadFromBuffer >= count - bytesRead ?
                        count - bytesRead :
                        bytesAvailableToReadFromBuffer;
                Buffer.BlockCopy(_inputBuffer, _inputBufferOffset, result, bytesRead, bytesToRead);
                bytesRead += bytesToRead;
                _inputBufferOffset += bytesToRead;
            }
            
            return result;
        }

        #endregion

        #region Reflective Pop

        public T PopObject<T>(int maxDepth = 10) where T : new()
            => (T)PopObject(typeof(T), 1, maxDepth);

        public object PopObject(Type type, int maxDepth = 10)
            => PopObject(type, 1, maxDepth);

        private object PopObject(Type type, int depth, int maxDepth)
        {
            if (depth > maxDepth) return null; // Ignore properties past max depth
            
            var typeWithoutGenerics = type.IsGenericType ? type.GetGenericTypeDefinition() : type;
            if (_popValueTypes.Contains(typeWithoutGenerics))
            {
                // Raw value to push
                return PopValue(type, depth + 1, maxDepth);
            }
            else if (IsKeyValuePair(type))
            {
                return PopKeyValuePair(type, depth + 1, maxDepth);
            }

            var mapping = GetPropertyToIntMapping(type);

            // Create and populate object
            var result = Activator.CreateInstance(type);
            var propertiesToSet = GetPropertiesToSet(type); // For now we only consider properties with getter/setter
            while (true)
            {
                var propertyByteId = PopByte();
                if (propertyByteId == 0) break; // End of object

                var propertyName = mapping[propertyByteId];
                if (!propertiesToSet.ContainsKey(propertyName))
                {
                    // We don't know how many values to pop, so we can't determine how to skip
                    throw new ArgumentException($"Property name {propertyName} not found in type but is expected.");
                }
                var matchingProperty = propertiesToSet[propertyName];

                var propertyType = matchingProperty.PropertyType;

                var propertyHasValue = PopByte() == 0x01;

                if (!propertyHasValue) continue; // Ignore null values
                
                var propertyValue = PopValue(propertyType, depth + 1, maxDepth);
                matchingProperty.SetValue(result, propertyValue);
            }

            return result;
        }

        private IImmutableDictionary<byte, string> GetPropertyToIntMapping(Type type)
        {
            if (!_propertyToByteMapping.ContainsKey(type))
            {
                var mapping = ((IDictionary<string, byte>)PopObject(typeof(IDictionary<string, byte>))).Reverse();
                _propertyToByteMapping[type] = mapping.ToImmutableDictionary();
            }
            return _propertyToByteMapping[type];
        }

        private IImmutableDictionary<string, PropertyInfo> GetPropertiesToSet(Type type)
        {
            if (_propertiesByType.ContainsKey(type))
            {
                return _propertiesByType[type];
            }
            return _propertiesByType[type] = type
                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.CanRead && p.CanWrite)
                .ToImmutableDictionary(item => item.Name, item => item);
        }

        private object PopValue(Type type, int depth, int maxDepth)
        {
            if (_basicTypePopMethods.ContainsKey(type))
            {
                // Property was basic supported type and was pushed
                return _basicTypePopMethods[type]();
            }
            else if (IsKeyValuePair(type))
            {
                return PopKeyValuePair(type, depth + 1, maxDepth);
            }
            else if (type.IsArray || typeof(IEnumerable).IsAssignableFrom(type))
            {
                // For collections, we can just store the values and let the deserializer figure out what container to put them inside
                var genericType = Utilities.GetElementType(type);
                var length = PopInt();
                var entries = new List<object>();
                for (var i = 0; i < length; ++i)
                {
                    var value = PopValue(genericType, depth + 1, maxDepth);
                    entries.Add(value);
                }
                
                if (Utilities.IsAssignableToGenericType(type, typeof(IDictionary<,>)))
                {
                    if (!_makeGenericTypeDictionary.ContainsKey(genericType))
                    {
                        var genericArguments = genericType.GetGenericArguments();
                        var genericType1 = genericArguments[0];
                        var genericType2 = genericArguments[1];
                        _makeGenericTypeDictionary[genericType] = typeof(Dictionary<,>).MakeGenericType(genericType1, genericType2);
                    }
                    var dictionaryGenericType = _makeGenericTypeDictionary[genericType];
                    return Utilities.CreateFromEnumerableConstructor(dictionaryGenericType, genericType, entries);
                }
                else if (type.IsArray)
                {
                    return Utilities.ConvertToArray(entries, genericType);
                }
                else if (Utilities.IsAssignableToGenericType(type, typeof(ISet<>)))
                {
                    if (!_makeGenericTypeHashset.ContainsKey(genericType))
                    {
                        _makeGenericTypeHashset[genericType] = typeof(HashSet<>).MakeGenericType(genericType);
                    }
                    return Utilities.CreateFromEnumerableConstructor(_makeGenericTypeHashset[genericType], genericType, entries);
                }
                else if (Utilities.IsAssignableToGenericType(type, typeof(IList<>)))
                {
                    return Utilities.ConvertToList(entries, genericType);
                }
                else if (Utilities.IsAssignableToGenericType(type, typeof(IEnumerable<>)))
                {
                    return Utilities.ConvertTo(entries, genericType);
                }
                else
                {
                    throw new NotImplementedException($"{nameof(PopObject)} does not support deserializing type of {type.FullName}");
                }
            }
            else if (type.IsClass) // TODO: Handle structs
            {
                return PopObject(type, depth + 1, maxDepth);
            }
            else
            {
                throw new NotImplementedException($"{nameof(PopObject)} does not support deserializing type of {type.FullName}");
            }
        }

        private bool IsKeyValuePair(Type type)
        {
            if (!_isKeyValuePair.ContainsKey(type))
            {
                var typeWithoutGenerics = type.IsGenericType ? type.GetGenericTypeDefinition() : type;
                _isKeyValuePair[type] = typeof(KeyValuePair<,>).IsAssignableFrom(typeWithoutGenerics);
            }
            return _isKeyValuePair[type];
        }

        private object PopKeyValuePair(Type type, int depth, int maxDepth)
        {
            // KeyValuePair is popped as the key then value
            if (!_genericArguments.ContainsKey(type))
            {
                _genericArguments[type] = type.GetGenericArguments();
            }
            
            var genericParameters = _genericArguments[type];
            var kvKey = PopObject(genericParameters[0], depth + 1, maxDepth);
            var kvValue = PopObject(genericParameters[1], depth + 1, maxDepth);
            
            if (!_keyValueConstructors.ContainsKey(type))
            {
                _keyValueConstructors[type] = type.GetConstructors().First();
            }
            return _keyValueConstructors[type].Invoke(new[] { kvKey, kvValue });
        }
        
        #endregion

        public void Dispose()
        {
        }
    }
}
