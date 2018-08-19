using System;
using System.Collections.Generic;
using System.IO;
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

        #endregion

        public void Dispose()
        {
        }
    }
}
