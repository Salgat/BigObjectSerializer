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
        private Func<byte[], int, CancellationToken, Task> _readFunc;
        private readonly bool _isLittleEndian = BitConverter.IsLittleEndian;
        private readonly byte[] _inputBuffer = new byte[8];

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
                if (byteCount != readCount)
                {
                    throw new ArgumentException($"Expected to read count of {readCount} but read {byteCount}.");
                }
            };
        }

        #region Pop

        private async Task<ulong> ReadAsync(int count)
        {
            ulong result = 0;
            await _readFunc(_inputBuffer, count, CancellationToken.None);

            if (_isLittleEndian)
            {
                for (var i = 0; i < count; ++i)
                {
                    result |= (ulong)_inputBuffer[i] << i * 8;
                }
            }
            else
            {
                for (var i = 0; i < count; ++i)
                {
                    result |= (ulong)_inputBuffer[count - 1 - i] << i * 8;
                }
            }
            return result;
        }

        public async Task<string> PopStringAsync()
        {
            var length = await PopIntAsync(); // Strings start with an int value of the string length
            var characterBytes = new byte[length];
            for (var i = 0; i < length; ++i)
            {
                characterBytes[i] = await PopByteAsync();
            }

            return new string(Encoding.UTF8.GetChars(characterBytes));
        }

        private async Task<byte> ReadByteAsync()
        {
            await _readFunc(_inputBuffer, 1, CancellationToken.None);
            return _inputBuffer[0];
        }

        public Task<int> PopIntAsync()
            => ReadAsync(sizeof(int)).ContinueWith(t => (int)t.Result);

        public Task<uint> PopUnsignedIntAsync()
            => ReadAsync(sizeof(int)).ContinueWith(t => (uint)t.Result);

        public Task<short> PopShortAsync()
            => ReadAsync(sizeof(short)).ContinueWith(t => (short)t.Result);

        public Task<ushort> PopUnsignedShortAsync()
            => ReadAsync(sizeof(ushort)).ContinueWith(t => (ushort)t.Result);

        public Task<long> PopLongAsync()
            => ReadAsync(sizeof(long)).ContinueWith(t => (long)t.Result);

        public Task<ulong> PopUnsignedLongAsync()
            => ReadAsync(sizeof(ulong)).ContinueWith(t => (ulong)t.Result);

        public Task<byte> PopByteAsync()
            => ReadByteAsync();

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
            => PopStringAsync().ContinueWith(t => new Guid(t.Result));

        #endregion

        #region Reflective Pop

        public Task<T> PopObjectAsync<T>(int maxDepth = 10) where T : new()
            => PopObjectAsync(typeof(T), 1, maxDepth).ContinueWith(t => (T)t.Result);

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

            var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance).ToList();

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
            var propertiesToSet = properties.Where(p => p.CanRead && p.CanWrite); // For now we only consider properties with getter/setter

            if (await PopStringAsync() != "__BOS_S") throw new ArgumentException("Expected start of object.");

            while (true)
            {
                var propertyName = await PopStringAsync();
                if (propertyName == "__BOS_E") break; // End of object

                var matchingProperty = propertiesToSet.FirstOrDefault(p => p.Name == propertyName);
                if (matchingProperty == default)
                {
                    // We don't know how many values to pop, so we can't determine how to skip
                    throw new ArgumentException($"Property name {propertyName} not found in type but is expected.");
                }
             
                var propertyType = matchingProperty.PropertyType;

                var propertyHasValue = await PopByteAsync() == 0x01;

                if (!propertyHasValue) continue; // Ignore null values
                
                var propertyValue = await PopValueAsync(propertyType, depth + 1, maxDepth);
                matchingProperty.SetValue(result, propertyValue);
            }

            return result;
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
