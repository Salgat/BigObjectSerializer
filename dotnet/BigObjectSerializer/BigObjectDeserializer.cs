﻿using System;
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
        private static int Counter = 0;

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
        private static readonly IImmutableDictionary<Type, MethodInfo> _basicTypePopMethods; // Types that directly map to basic Pop Methods (such as PopInt)
        private static readonly IImmutableDictionary<Type, PropertyInfo> _basicTypeGenericTaskResult; // Types that directly map to basic Pop Methods (such as PopInt)
        private static readonly IImmutableSet<Type> _popValueTypes; // Types that directly map to supported PopValue methods (basic types and collections)
        private readonly IDictionary<Type, ConstructorInfo> _keyValueConstructors = new Dictionary<Type, ConstructorInfo>();

        static BigObjectDeserializer()
        {
            var popMethods = new Dictionary<Type, MethodInfo>
            {
                [typeof(int)] = typeof(BigObjectDeserializer).GetMethod(nameof(PopInt)),
                [typeof(uint)] = typeof(BigObjectDeserializer).GetMethod(nameof(PopUnsignedInt)),
                [typeof(short)] = typeof(BigObjectDeserializer).GetMethod(nameof(PopShort)),
                [typeof(ushort)] = typeof(BigObjectDeserializer).GetMethod(nameof(PopUnsignedShort)),
                [typeof(long)] = typeof(BigObjectDeserializer).GetMethod(nameof(PopLong)),
                [typeof(ulong)] = typeof(BigObjectDeserializer).GetMethod(nameof(PopUnsignedLong)),
                [typeof(byte)] = typeof(BigObjectDeserializer).GetMethod(nameof(PopByte)),
                [typeof(bool)] = typeof(BigObjectDeserializer).GetMethod(nameof(PopBool)),
                [typeof(float)] = typeof(BigObjectDeserializer).GetMethod(nameof(PopFloat)),
                [typeof(double)] = typeof(BigObjectDeserializer).GetMethod(nameof(PopDouble)),
                [typeof(string)] = typeof(BigObjectDeserializer).GetMethod(nameof(PopString)),
                [typeof(Guid)] = typeof(BigObjectDeserializer).GetMethod(nameof(PopGuid))
            };
            _basicTypePopMethods = popMethods.ToImmutableDictionary();
            
            var genericTaskResults = new Dictionary<Type, PropertyInfo>
            {
                /*[typeof(int)] = typeof(>).MakeGenericType(typeof(int)).GetProperty(nameof(object>.Result)),
                [typeof(uint)] = typeof(>).MakeGenericType(typeof(uint)).GetProperty(nameof(object>.Result)),
                [typeof(short)] = typeof(>).MakeGenericType(typeof(short)).GetProperty(nameof(object>.Result)),
                [typeof(ushort)] = typeof(>).MakeGenericType(typeof(ushort)).GetProperty(nameof(object>.Result)),
                [typeof(long)] = typeof(>).MakeGenericType(typeof(long)).GetProperty(nameof(object>.Result)),
                [typeof(ulong)] = typeof(>).MakeGenericType(typeof(ulong)).GetProperty(nameof(object>.Result)),
                [typeof(byte)] = typeof(>).MakeGenericType(typeof(byte)).GetProperty(nameof(object>.Result)),
                [typeof(bool)] = typeof(>).MakeGenericType(typeof(bool)).GetProperty(nameof(object>.Result)),
                [typeof(float)] = typeof(>).MakeGenericType(typeof(float)).GetProperty(nameof(object>.Result)),
                [typeof(double)] = typeof(>).MakeGenericType(typeof(double)).GetProperty(nameof(object>.Result)),
                [typeof(string)] = typeof(>).MakeGenericType(typeof(string)).GetProperty(nameof(object>.Result)),
                [typeof(Guid)] = typeof(>).MakeGenericType(typeof(Guid)).GetProperty(nameof(object>.Result))*/
            };
            _basicTypeGenericTaskResult = genericTaskResults.ToImmutableDictionary();

            var popValueTypes = popMethods.Select(kv => kv.Key).ToList();
            popValueTypes.Add(typeof(ISet<>));
            popValueTypes.Add(typeof(IDictionary<,>));
            popValueTypes.Add(typeof(IList<>));
            popValueTypes.Add(typeof(IEnumerable<>));
            _popValueTypes = popValueTypes.ToImmutableHashSet();
        }
        
        public BigObjectDeserializer(Action<byte[], int> readFunc)
        {
            _readFunc = readFunc;
        }

        public BigObjectDeserializer(Stream inputStream)
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

        #region Pop

        private ulong Read(int count)
        {
            ++Counter;
            if (count > LargestBufferObject) throw new ArgumentException($"Cannot read larger than {LargestBufferObject} bytes at a time.");

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
            => new Guid(PopString());

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

            // Null check
            if (type.IsClass)
            {
                if (PopByte() == 0x0)
                {
                    return null;
                }
            }
            
            var typeWithoutGenerics = type.IsGenericType ? type.GetGenericTypeDefinition() : type;
            if (_popValueTypes.Contains(typeWithoutGenerics))
            {
                // Raw value to push
                return PopValue(type, depth + 1, maxDepth);
            }
            else if (typeof(KeyValuePair<,>).IsAssignableFrom(typeWithoutGenerics))
            {
                return PopKeyValuePair(type, depth + 1, maxDepth);
            }

            // Create and populate object
            var result = Activator.CreateInstance(type);
            var propertiesToSet = GetPropertiesToSet(type); // For now we only consider properties with getter/setter
            while (true)
            {
                var propertyName = PopString();
                if (propertyName == string.Empty) break; // End of object

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
                return _basicTypePopMethods[type].Invoke(this, _emptyArray);
            }
            else if (type.IsArray || typeof(IEnumerable).IsAssignableFrom(type))
            {
                // For collections, we can just store the values and let the deserializer figure out what container to put them inside
                var genericType = Utilities.GetElementType(type);
                var length = PopInt();
                var entries = new List<object>();
                for (var i = 0; i < length; ++i)
                {
                    var value = PopObject(genericType, depth + 1, maxDepth);
                    entries.Add(value);
                }
                
                if (Utilities.IsAssignableToGenericType(type, typeof(IDictionary<,>)))
                {
                    var genericArguments = genericType.GetGenericArguments();
                    var genericType1 = genericArguments[0];
                    var genericType2 = genericArguments[1];
                    var dictionaryGenericType = typeof(Dictionary<,>).MakeGenericType(genericType1, genericType2);
                    return Utilities.CreateFromEnumerableConstructor(dictionaryGenericType, genericType, entries);
                }
                else if (type.IsArray)
                {
                    return Utilities.ConvertToArray(entries, genericType);
                }
                else if (Utilities.IsAssignableToGenericType(type, typeof(ISet<>)))
                {
                    var hashSetGenericType = typeof(HashSet<>).MakeGenericType(genericType);
                    return Utilities.CreateFromEnumerableConstructor(hashSetGenericType, genericType, entries);
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

        private object PopKeyValuePair(Type type, int depth, int maxDepth)
        {
            // KeyValuePair is popped as the key then value
            var genericParameters = type.GetGenericArguments();
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
