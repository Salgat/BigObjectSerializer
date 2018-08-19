using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
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

        #endregion

        public void Dispose()
        {
        }
    }
}
