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
    public class BigObjectSerializer : IDisposable
    {
        private readonly SemaphoreSlim _bufferLock = new SemaphoreSlim(1);
        private int _activeBuffer = 0;
        private int _activeBufferPosition = 0;
        private readonly IList<byte[]> _buffers;
        private Func<ReadOnlyMemory<byte>, CancellationToken, Task> _flushCallback;
        private Task _pendingFlush = Task.CompletedTask;
        private readonly int _bufferSize;
        private const int LargestBufferObject = 8; // 8 bytes (long - 64 bits)
        private readonly bool _isLittleEndian = BitConverter.IsLittleEndian;

        // Safely copying byte contents of float and double derived from https://github.com/google/flatbuffers/blob/master/net/FlatBuffers/ByteBuffer.cs
        private float[] _floatBuffer = new[] { 0.0f };
        private int[] _intBuffer = new[] { 0 };
        private double[] _doubleBuffer = new[] { 0.0 };
        private ulong[] _ulongBuffer = new[] { 0UL };

        // Reflection
        private static readonly IImmutableDictionary<Type, MethodInfo> _basicTypePushMethods;
        
        static BigObjectSerializer()
        {
            var pushMethods = new Dictionary<Type, MethodInfo>
            {
                [typeof(int)] = typeof(BigObjectSerializer).GetMethod(nameof(PushIntAsync)),
                [typeof(uint)] = typeof(BigObjectSerializer).GetMethod(nameof(PushUnsignedIntAsync)),
                [typeof(short)] = typeof(BigObjectSerializer).GetMethod(nameof(PushShortAsync)),
                [typeof(ushort)] = typeof(BigObjectSerializer).GetMethod(nameof(PushUnsignedShortAsync)),
                [typeof(long)] = typeof(BigObjectSerializer).GetMethod(nameof(PushLongAsync)),
                [typeof(ulong)] = typeof(BigObjectSerializer).GetMethod(nameof(PushUnsignedLongAsync)),
                [typeof(byte)] = typeof(BigObjectSerializer).GetMethod(nameof(PushByteAsync)),
                [typeof(bool)] = typeof(BigObjectSerializer).GetMethod(nameof(PushBoolAsync)),
                [typeof(float)] = typeof(BigObjectSerializer).GetMethod(nameof(PushFloatAsync)),
                [typeof(double)] = typeof(BigObjectSerializer).GetMethod(nameof(PushDoubleAsync)),
                [typeof(string)] = typeof(BigObjectSerializer).GetMethod(nameof(PushStringAsync))
            };
            _basicTypePushMethods = pushMethods.ToImmutableDictionary();
        }

        public BigObjectSerializer(Func<ReadOnlyMemory<byte>, CancellationToken, Task> writeFunc, int bufferSize = 1000000)
        {
            _bufferSize = bufferSize;
            _buffers = new List<byte[]>()
            {
                new byte[bufferSize],
                new byte[bufferSize]
            };
            _flushCallback = writeFunc;
        }

        public BigObjectSerializer(Stream outputStream, int bufferSize = 1000000)
        {
            _bufferSize = bufferSize;
            _buffers = new List<byte[]>()
            {
                new byte[bufferSize],
                new byte[bufferSize]
            };
            _flushCallback = async (valuesToWrite, cancellationToken) =>
            {
                await outputStream.WriteAsync(valuesToWrite.ToArray(), 0, valuesToWrite.Length, cancellationToken);
                await outputStream.FlushAsync(cancellationToken);
            };
        }

        #region Configuration

        public BigObjectSerializer AddFlushCallback(Func<ReadOnlyMemory<byte>, CancellationToken, Task> callback)
        {
            if (_flushCallback != null)
            {
                throw new ArgumentException("Flush callback already added.");
            }
            _flushCallback = callback;
            return this;
        }

        #endregion

        #region Push

        public Task FlushAsync()
            => FlushIfRequiredAsync(true);

        private async Task FlushIfRequiredAsync(bool forceFlush = false)
        {
            if (forceFlush || _activeBufferPosition - _bufferSize < LargestBufferObject)
            {
                // No room for largest object left, flush current contents
                await _pendingFlush.ConfigureAwait(false); // Wait for pending flush to finish if it hasn't
                    
                var memorySpan = new ReadOnlyMemory<byte>(_buffers[_activeBuffer], 0, _activeBufferPosition);
                _pendingFlush = Task.Run(() => _flushCallback(memorySpan, CancellationToken.None));

                // Swap buffers to allow new buffer to fill up while flush is occurring
                _activeBuffer = _activeBuffer == 0 ? 1 : 0;
                _activeBufferPosition = 0;
            }
        }

        private void WriteValue(ulong data, int count, byte[] target, int offset)
        {
            if (count > LargestBufferObject)
            {
                throw new ArgumentException($"Values to write to buffer cannot be larger than {LargestBufferObject} in bytes.");
            }

            if (_isLittleEndian)
            {
                for (var i = 0; i < count; ++i)
                {
                    target[offset + i] = (byte)(data >> i * 8);
                }
            }
            else
            {
                for (var i = 0; i < count; ++i)
                {
                    target[offset + count - 1 - i] = (byte)(data >> i * 8);
                }
            }
        }

        private async Task WriteAndFlushIfRequireAsync(ulong data, int count)
        {
            await _bufferLock.WaitAsync().ConfigureAwait(false);
            try
            {
                WriteValue(data, count, _buffers[_activeBuffer], _activeBufferPosition);
                _activeBufferPosition += count;
                await FlushIfRequiredAsync().ConfigureAwait(false);
            }
            finally
            {
                _bufferLock.Release();
            }
        }
        
        private async Task WriteByteAndFlushIfRequireAsync(byte data)
        {
            await _bufferLock.WaitAsync().ConfigureAwait(false);
            try
            {
                _buffers[_activeBuffer][_activeBufferPosition] = data;
                ++_activeBufferPosition;
                await FlushIfRequiredAsync().ConfigureAwait(false);
            }
            finally
            {
                _bufferLock.Release();
            }
        }

        public async Task PushStringAsync(string value)
        {
            await PushIntAsync(value.Length); // First int stores length of string
            foreach (var b in Encoding.UTF8.GetBytes(value)) {
                await WriteByteAndFlushIfRequireAsync(b).ConfigureAwait(false);
            }
        }

        public Task PushIntAsync(int value)
            => WriteAndFlushIfRequireAsync((ulong)value, sizeof(int));

        public Task PushUnsignedIntAsync(uint value)
            => WriteAndFlushIfRequireAsync((ulong)value, sizeof(uint));

        public Task PushShortAsync(short value)
            => WriteAndFlushIfRequireAsync((ulong)value, sizeof(short));

        public Task PushUnsignedShortAsync(ushort value)
            => WriteAndFlushIfRequireAsync((ulong)value, sizeof(ushort));

        public Task PushLongAsync(long value)
            => WriteAndFlushIfRequireAsync((ulong)value, sizeof(long));

        public Task PushUnsignedLongAsync(ulong value)
            => WriteAndFlushIfRequireAsync((ulong)value, sizeof(ulong));

        public Task PushByteAsync(byte value)
            => WriteByteAndFlushIfRequireAsync(value);

        public Task PushBoolAsync(bool value)
            => WriteByteAndFlushIfRequireAsync(value ? (byte)0x1 : (byte)0x0);

        public async Task PushFloatAsync(float value)
        {
            _floatBuffer[0] = value;
            Buffer.BlockCopy(_floatBuffer, 0, _intBuffer, 0, sizeof(float));
            await WriteAndFlushIfRequireAsync((ulong)_intBuffer[0], sizeof(float)).ConfigureAwait(false);
        }

        public async Task PushDoubleAsync(double value)
        {
            _doubleBuffer[0] = value;
            Buffer.BlockCopy(_doubleBuffer, 0, _ulongBuffer, 0, sizeof(double));
            await WriteAndFlushIfRequireAsync((ulong)_ulongBuffer[0], sizeof(double)).ConfigureAwait(false);
        }

        #region Reflective Push

        public Task PushObjectAsync<T>(T value, int maxDepth = 10) where T : new()
            => PushObjectAsync(value, typeof(T), 1, maxDepth);

        private async Task PushObjectAsync(object value, Type type, int depth, int maxDepth)
        {
            if (depth > maxDepth) return; // Ignore properties past max depth

            var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance).ToList();

            if (properties.Count() == 0)
            {
                // Raw value to push
                await PushValueAsync(value, type, depth + 1, maxDepth);
                return;
            }

            foreach (var property in properties.Where(p => p.CanRead && p.CanWrite)) // For now we only consider properties with getter/setter
            {
                var propertyType = property.PropertyType;
                var name = property.Name;
                var propertyValue = property.GetValue(value);
                
                if (propertyValue == null) continue; // Null values can be ignored since the deserializer will also leave the property null NOTE: This leaves a bug where default values are not overwritten with a null value

                await PushStringAsync(name); // Property name is pushed first to help deserialization handle extra or out of order properties changed after serialization
                await PushValueAsync(propertyValue, propertyType, depth + 1, maxDepth);
            }
        }

        private async Task PushValueAsync(object value, Type type, int depth, int maxDepth)
        {
            if (await PushBasicTypeAsync(value, type))
            {
                // Property was basic supported type and was pushed
                return;
            }
            else if (type.IsArray || typeof(IEnumerable).IsAssignableFrom(type))
            {
                // For collections, we can just store the values and let the deserializer figure out what container to put them inside
                var genericType = GetElementType(type);
                var enumerable = ((IEnumerable)value).OfType<object>();

                await PushIntAsync(enumerable.Count()); // Store the length of the enumerable
                foreach (var entry in enumerable)
                {
                    await PushObjectAsync(entry, genericType, depth + 1, maxDepth);
                }
            }
            else if (type.IsClass) // TODO: Handle structs
            {
                await PushObjectAsync(value, type, depth + 1, maxDepth);
            }
            else
            {
                throw new NotImplementedException($"{nameof(PushObjectAsync)} does not support serializing type of {type.FullName}");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="value"></param>
        /// <returns>True if value was a basic type pushed.</returns>
        private async Task<bool> PushBasicTypeAsync(object value, Type type)
        {
            // TODO: Use reflection method invoke for this
            if (_basicTypePushMethods.ContainsKey(type))
            {
                await (Task)_basicTypePushMethods[type].Invoke(this, new[] { value });
                return true;
            }
            return false;
        }
        
        private static Type GetElementType(Type type)
        {
            // Source: https://stackoverflow.com/questions/906499/getting-type-t-from-ienumerablet
            // Type is Array
            // short-circuit if you expect lots of arrays 
            if (type.IsArray)
                return type.GetElementType();

            // type is IEnumerable<T>;
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                return type.GetGenericArguments()[0];

            // type implements/extends IEnumerable<T>;
            var enumType = type.GetInterfaces()
                                    .Where(t => t.IsGenericType &&
                                           t.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                                    .Select(t => t.GenericTypeArguments[0]).FirstOrDefault();
            return enumType ?? type;
        }

        #endregion

        #endregion

        public void Dispose()
        {
        }
    }
}
