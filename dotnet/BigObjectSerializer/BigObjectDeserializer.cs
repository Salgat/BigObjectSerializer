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
        private static int Counter = 0;

        private Func<byte[], int, CancellationToken, Task> _readFunc;
        private readonly byte[] _inputBuffer = new byte[8*1000000];
        private readonly byte[] _inputBufferOld = new byte[LargestBufferObject]; // This holds the last 8 bytes of the old buffer
        private const int LargestBufferObject = 8; // 8 bytes (long - 64 bits)
        private int _inputBufferOffset = 0;
        private bool _initialized = false;

        // Safely copying byte contents of float and double derived from https://github.com/google/flatbuffers/blob/master/net/FlatBuffers/ByteBuffer.cs
        private float[] _floatBuffer = new[] { 0.0f };
        private int[] _intBuffer = new[] { 0 };
        private double[] _doubleBuffer = new[] { 0.0 };
        private ulong[] _ulongBuffer = new[] { 0UL };

        // Reflection
        private static readonly IImmutableDictionary<Type, MethodInfo> _basicTypePopMethods; // Types that directly map to basic Pop Methods (such as PopIntAsync)
        private static readonly IImmutableSet<Type> _popValueTypes; // Types that directly map to supported PopValue methods (basic types and collections)

        static BigObjectDeserializer()
        {
            var popMethods = new Dictionary<Type, MethodInfo>
            {
                [typeof(int)] = typeof(BigObjectDeserializer).GetMethod(nameof(PopIntAsync)),
                [typeof(uint)] = typeof(BigObjectDeserializer).GetMethod(nameof(PopUnsignedIntAsync)),
                [typeof(short)] = typeof(BigObjectDeserializer).GetMethod(nameof(PopShortAsync)),
                [typeof(ushort)] = typeof(BigObjectDeserializer).GetMethod(nameof(PopUnsignedShortAsync)),
                [typeof(long)] = typeof(BigObjectDeserializer).GetMethod(nameof(PopLongAsync)),
                [typeof(ulong)] = typeof(BigObjectDeserializer).GetMethod(nameof(PopUnsignedLongAsync)),
                [typeof(byte)] = typeof(BigObjectDeserializer).GetMethod(nameof(PopByteAsync)),
                [typeof(bool)] = typeof(BigObjectDeserializer).GetMethod(nameof(PopBoolAsync)),
                [typeof(float)] = typeof(BigObjectDeserializer).GetMethod(nameof(PopFloatAsync)),
                [typeof(double)] = typeof(BigObjectDeserializer).GetMethod(nameof(PopDoubleAsync)),
                [typeof(string)] = typeof(BigObjectDeserializer).GetMethod(nameof(PopStringAsync)),
                [typeof(Guid)] = typeof(BigObjectDeserializer).GetMethod(nameof(PopGuidAsync))
            };
            _basicTypePopMethods = popMethods.ToImmutableDictionary();

            var popValueTypes = popMethods.Select(kv => kv.Key).ToList();
            popValueTypes.Add(typeof(ISet<>));
            popValueTypes.Add(typeof(IDictionary<,>));
            popValueTypes.Add(typeof(IList<>));
            popValueTypes.Add(typeof(IEnumerable<>));
            _popValueTypes = popValueTypes.ToImmutableHashSet();
        }
        
        public BigObjectDeserializer(Func<byte[], int, CancellationToken, Task> readFunc)
        {
            _readFunc = readFunc;
        }

        public BigObjectDeserializer(Stream inputStream)
        {
            _readFunc = async (inputBuffer, readCount, cancellationToken) =>
            {
                var byteCount = await inputStream.ReadAsync(inputBuffer, 0, readCount, cancellationToken);
                /*if (byteCount != readCount)
                {
                    throw new ArgumentException($"Expected to read count of {readCount} but read {byteCount}.");
                }*/
            };
        }

        #region Pop

        private async Task<ulong> ReadAsync(int count)
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

                await _readFunc(_inputBuffer, _inputBuffer.Length, CancellationToken.None);
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

        public async Task<string> PopStringAsync()
        {
            var length = await PopIntAsync(); // Strings start with an int value of the string length
            var characterBytes = await PopBytesAsync(length);
            
            return new string(Encoding.UTF8.GetChars(characterBytes));
        }

        private async Task<byte> ReadByteAsync()
        {
            if (!_initialized || _inputBufferOffset >= _inputBuffer.Length)
            {
                _initialized = true;
                await _readFunc(_inputBuffer, _inputBuffer.Length, CancellationToken.None);
                _inputBufferOffset = 0;
            }
            _inputBufferOffset += 1;
            return _inputBuffer[_inputBufferOffset - 1];
        }

        public Task<int> PopIntAsync()
            => ReadAsync(sizeof(int)).ContinueWith(t => (int)t.Result, TaskContinuationOptions.ExecuteSynchronously);

        public Task<uint> PopUnsignedIntAsync()
            => ReadAsync(sizeof(int)).ContinueWith(t => (uint)t.Result, TaskContinuationOptions.ExecuteSynchronously);

        public Task<short> PopShortAsync()
            => ReadAsync(sizeof(short)).ContinueWith(t => (short)t.Result, TaskContinuationOptions.ExecuteSynchronously);

        public Task<ushort> PopUnsignedShortAsync()
            => ReadAsync(sizeof(ushort)).ContinueWith(t => (ushort)t.Result, TaskContinuationOptions.ExecuteSynchronously);

        public Task<long> PopLongAsync()
            => ReadAsync(sizeof(long)).ContinueWith(t => (long)t.Result, TaskContinuationOptions.ExecuteSynchronously);

        public Task<ulong> PopUnsignedLongAsync()
            => ReadAsync(sizeof(ulong)).ContinueWith(t => (ulong)t.Result, TaskContinuationOptions.ExecuteSynchronously);

        public async Task<bool> PopBoolAsync()
            => await ReadByteAsync() == (byte)0x1;

        public async Task<float> PopFloatAsync()
        {
            _intBuffer[0] = await PopIntAsync();
            Buffer.BlockCopy(_intBuffer, 0, _floatBuffer, 0, sizeof(float));
            return _floatBuffer[0];
        }

        public async Task<double> PopDoubleAsync()
        {
            _ulongBuffer[0] = await PopUnsignedLongAsync();
            Buffer.BlockCopy(_ulongBuffer, 0, _doubleBuffer, 0, sizeof(double));
            return _doubleBuffer[0];
        }

        public Task<Guid> PopGuidAsync()
            => PopStringAsync().ContinueWith(t => new Guid(t.Result), TaskContinuationOptions.ExecuteSynchronously);

        public Task<byte> PopByteAsync()
            => ReadByteAsync();

        public async Task<byte[]> PopBytesAsync(int count)
        {
            var result = new byte[count];
            var bytesRead = 0;
            while (bytesRead != count)
            {
                var bytesAvailableToReadFromBuffer = _inputBuffer.Length - _inputBufferOffset;
                if (bytesAvailableToReadFromBuffer == 0)
                {
                    await _readFunc(_inputBuffer, _inputBuffer.Length, CancellationToken.None);
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

        public Task<T> PopObjectAsync<T>(int maxDepth = 10) where T : new()
            => PopObjectAsync(typeof(T), 1, maxDepth).ContinueWith(t => (T)t.Result, TaskContinuationOptions.ExecuteSynchronously);

        public Task<object> PopObjectAsync(Type type, int maxDepth = 10)
            => PopObjectAsync(type, 1, maxDepth);

        private async Task<object> PopObjectAsync(Type type, int depth, int maxDepth)
        {
            if (depth > maxDepth) return null; // Ignore properties past max depth

            // Null check
            if (type.IsClass)
            {
                if (await PopByteAsync() == 0x0)
                {
                    return null;
                }
            }
            
            var typeWithoutGenerics = type.IsGenericType ? type.GetGenericTypeDefinition() : type;
            if (_popValueTypes.Contains(typeWithoutGenerics))
            {
                // Raw value to push
                return await PopValueAsync(type, depth + 1, maxDepth);
            }
            else if (typeof(KeyValuePair<,>).IsAssignableFrom(typeWithoutGenerics))
            {
                return await PopKeyValuePairAsync(type, depth + 1, maxDepth);
            }

            // Create and populate object
            var result = Activator.CreateInstance(type);
            var propertiesToSet = GetPropertiesToSet(type); // For now we only consider properties with getter/setter

            if (await PopStringAsync() != "__BOS_S") throw new ArgumentException("Expected start of object.");

            while (true)
            {
                var propertyName = await PopStringAsync();
                if (propertyName == "__BOS_E") break; // End of object

                if (!propertiesToSet.ContainsKey(propertyName))
                {
                    // We don't know how many values to pop, so we can't determine how to skip
                    throw new ArgumentException($"Property name {propertyName} not found in type but is expected.");
                }
                var matchingProperty = propertiesToSet[propertyName];

                var propertyType = matchingProperty.PropertyType;

                var propertyHasValue = await PopByteAsync() == 0x01;

                if (!propertyHasValue) continue; // Ignore null values
                
                var propertyValue = await PopValueAsync(propertyType, depth + 1, maxDepth);
                matchingProperty.SetValue(result, propertyValue);
            }

            return result;
        }

        private readonly IDictionary<Type, IImmutableDictionary<string, PropertyInfo>> _propertiesByType = new Dictionary<Type, IImmutableDictionary<string, PropertyInfo>>();
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

        private async Task<object> PopValueAsync(Type type, int depth, int maxDepth)
        {
            var (success, result) = await TryPopBasicTypeAsync(type);
            if (success)
            {
                // Property was basic supported type and was pushed
                return result;
            }
            else if (type.IsArray || typeof(IEnumerable).IsAssignableFrom(type))
            {
                // For collections, we can just store the values and let the deserializer figure out what container to put them inside
                var genericType = Utilities.GetElementType(type);
                var length = await PopIntAsync();
                var entries = new List<object>();
                for (var i = 0; i < length; ++i)
                {
                    var value = await PopObjectAsync(genericType, depth + 1, maxDepth);
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
                    throw new NotImplementedException($"{nameof(PopObjectAsync)} does not support deserializing type of {type.FullName}");
                }
            }
            else if (type.IsClass) // TODO: Handle structs
            {
                return await PopObjectAsync(type, depth + 1, maxDepth);
            }
            else
            {
                throw new NotImplementedException($"{nameof(PopObjectAsync)} does not support deserializing type of {type.FullName}");
            }
        }

        private async Task<object> PopKeyValuePairAsync(Type type, int depth, int maxDepth)
        {
            // KeyValuePair is popped as the key then value
            var genericParameters = type.GetGenericArguments();
            var kvKey = await PopObjectAsync(genericParameters[0], depth + 1, maxDepth);
            var kvValue = await PopObjectAsync(genericParameters[1], depth + 1, maxDepth);

            var constructor = type.GetConstructors().First();
            return constructor.Invoke(new[] { kvKey, kvValue });
        }

        private async Task<(bool Success, object Result)> TryPopBasicTypeAsync(Type type)
        {
            if (_basicTypePopMethods.ContainsKey(type))
            {
                var task = (Task)_basicTypePopMethods[type].Invoke(this, new object[0]);
                await task;
                var resultProperty = typeof(Task<>).MakeGenericType(type).GetProperty(nameof(Task<object>.Result));
                return (true, resultProperty.GetValue(task));
            }
            return (false, null);
        }
        
        #endregion

        public void Dispose()
        {
        }
    }
}
