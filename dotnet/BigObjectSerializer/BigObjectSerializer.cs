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
        private readonly Stream _stream;

        // Safely copying byte contents of float and double derived from https://github.com/google/flatbuffers/blob/master/net/FlatBuffers/ByteBuffer.cs
        private float[] _floatBuffer = new[] { 0.0f };
        private int[] _intBuffer = new[] { 0 };
        private double[] _doubleBuffer = new[] { 0.0 };
        private ulong[] _ulongBuffer = new[] { 0UL };

        // Reflection
        private readonly IDictionary<Type, IImmutableList<PropertyInfo>> _propertiesByType = new Dictionary<Type, IImmutableList<PropertyInfo>>();
        private static readonly IImmutableDictionary<Type, Action<BigObjectSerializer, object>> _basicTypePushMethods;
        private static readonly IImmutableSet<Type> _pushValueTypes; // Types that directly map to supported PopValue methods (basic types and collections)

        static BigObjectSerializer()
        {
            var pushMethods = new Dictionary<Type, Action<BigObjectSerializer, object>>
            {
                [typeof(int)] = (instance, val) => instance.PushInt((int)val),
                [typeof(uint)] = (instance, val) => instance.PushUnsignedInt((uint)val),
                [typeof(short)] = (instance, val) => instance.PushShort((short)val),
                [typeof(ushort)] = (instance, val) => instance.PushUnsignedShort((ushort)val),
                [typeof(long)] = (instance, val) => instance.PushLong((long)val),
                [typeof(ulong)] = (instance, val) => instance.PushUnsignedLong((ulong)val),
                [typeof(byte)] = (instance, val) => instance.PushByte((byte)val),
                [typeof(bool)] = (instance, val) => instance.PushBool((bool)val),
                [typeof(float)] = (instance, val) => instance.PushFloat((float)val),
                [typeof(double)] = (instance, val) => instance.PushDouble((double)val),
                [typeof(string)] = (instance, val) => instance.PushString((string)val),
                [typeof(Guid)] = (instance, val) => instance.PushGuid((Guid)val),
            };
            _basicTypePushMethods = pushMethods.ToImmutableDictionary();

            var pushValueTypes = pushMethods.Select(kv => kv.Key).ToList();
            pushValueTypes.Add(typeof(ISet<>));
            pushValueTypes.Add(typeof(IDictionary<,>));
            pushValueTypes.Add(typeof(IList<>));
            pushValueTypes.Add(typeof(IEnumerable<>));
            _pushValueTypes = pushValueTypes.ToImmutableHashSet();
        }
        
        public BigObjectSerializer(Stream outputStream)
        {
            _stream = outputStream;
        }
        
        #region Push

        public void Flush()
            => _stream.Flush();

        public Task FlushAsync()
            => _stream.FlushAsync();

        private void WriteValue(ulong data, int count)
        {
            for (var i = 0; i < count; ++i)
            {
                _stream.WriteByte((byte)(data >> i * 8));
            }
        }
        
        private void WriteByte(byte data)
        {
            _stream.WriteByte(data);
        }

        private void WriteBytes(byte[] data)
        {
            var dataPosition = 0;
            _stream.Write(data, 0, data.Length);
        }

        public void PushString(string value)
        {
            PushInt(value.Length); // First int stores length of string
            if (value.Length == 0) return;
            WriteBytes(Encoding.UTF8.GetBytes(value));
        }

        public void PushInt(int value)
            => WriteValue((ulong)value, sizeof(int));

        public void PushUnsignedInt(uint value)
            => WriteValue((ulong)value, sizeof(uint));

        public void PushShort(short value)
            => WriteValue((ulong)value, sizeof(short));

        public void PushUnsignedShort(ushort value)
            => WriteValue((ulong)value, sizeof(ushort));

        public void PushLong(long value)
            => WriteValue((ulong)value, sizeof(long));

        public void PushUnsignedLong(ulong value)
            => WriteValue((ulong)value, sizeof(ulong));

        public void PushByte(byte value)
            => WriteByte(value);

        public void PushBool(bool value)
            => WriteByte(value ? (byte)0x1 : (byte)0x0);

        public void PushFloat(float value)
        {
            _floatBuffer[0] = value;
            Buffer.BlockCopy(_floatBuffer, 0, _intBuffer, 0, sizeof(float));
            WriteValue((ulong)_intBuffer[0], sizeof(float));
        }

        public void PushDouble(double value)
        {
            _doubleBuffer[0] = value;
            Buffer.BlockCopy(_doubleBuffer, 0, _ulongBuffer, 0, sizeof(double));
            WriteValue((ulong)_ulongBuffer[0], sizeof(double));
        }

        public void PushGuid(Guid value)
            => WriteBytes(value.ToByteArray());

        #endregion

        #region Reflective Push

        public void PushObject<T>(T value, int maxDepth = 10) where T : new()
            => PushObject(value, typeof(T), 1, maxDepth);

        public void PushObject(object value, Type type, int maxDepth = 10)
            => PushObject(value, type, 1, maxDepth);

        private void PushObject(object value, Type type, int depth, int maxDepth)
        {
            if (depth > maxDepth) return; // Ignore properties past max depth

            // Null check
            if (type.IsClass)
            {
                if (value is null)
                {
                    PushByte(0x0);
                }
                else
                {
                    PushByte(0x1);
                }
            }
            
            var typeWithoutGenerics = type.IsGenericType ? type.GetGenericTypeDefinition() : type;
            if (_pushValueTypes.Contains(typeWithoutGenerics)) // Might be better to instead check if it's one of the types directly serializable
            {
                // Raw value to push
                PushValue(value, type, depth + 1, maxDepth);
                return;
            }
            else if (typeof(KeyValuePair<,>).IsAssignableFrom(typeWithoutGenerics))
            {
                PushKeyValuePair(value, type, depth + 1, maxDepth);
                return;
            }
            
            foreach (var property in GetPropertiesToGet(type)) // For now we only consider properties with getter/setter
            {
                var propertyType = property.PropertyType;
                var name = property.Name;
                var propertyValue = property.GetValue(value);

                PushString(name); // Property name is pushed first to help deserialization handle extra or out of order properties changed after serialization

                if (propertyValue == null)
                {
                    // Null values are marked with byte value 0x00 and skipped
                    PushByte(0x0);
                    continue;
                }
                else
                {
                    PushByte(0x1);
                }
                PushValue(propertyValue, propertyType, depth + 1, maxDepth);
            }
            PushString(string.Empty); // Mark end of object (since no property can have an empty string label)
        }

        private IImmutableList<PropertyInfo> GetPropertiesToGet(Type type)
        {
            if (_propertiesByType.ContainsKey(type))
            {
                return _propertiesByType[type];
            }
            return _propertiesByType[type] = type
                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.CanRead && p.CanWrite)
                .ToImmutableList();
        }

        private void PushValue(object value, Type type, int depth, int maxDepth)
        {
            if (_basicTypePushMethods.ContainsKey(type))
            {
                // Property was basic supported type and was pushed
                _basicTypePushMethods[type].Invoke(this, value);
                return;
            }
            else if (type.IsArray || typeof(IEnumerable).IsAssignableFrom(type))
            {
                // For collections, we can just store the values and let the deserializer figure out what container to put them inside
                var genericType = Utilities.GetElementType(type);
                var enumerable = ((IEnumerable)value).OfType<object>();

                PushInt(enumerable.Count()); // Store the length of the enumerable
                foreach (var entry in enumerable)
                {
                    PushObject(entry, genericType, depth + 1, maxDepth);
                }
            }
            else if (type.IsClass) // TODO: Handle structs
            {
                PushObject(value, type, depth + 1, maxDepth);
            }
            else
            {
                throw new NotImplementedException($"{nameof(PushObject)} does not support serializing type of {type.FullName}");
            }
        }

        private void PushKeyValuePair(object value, Type type, int depth, int maxDepth)
        {
            // KeyValuePair is pushed as the key then value
            var properties = type.GetProperties();
            var kvKey = properties.First(p => p.Name == nameof(KeyValuePair<object, object>.Key));
            var kvValue = properties.First(p => p.Name == nameof(KeyValuePair<object, object>.Value));

            PushObject(kvKey.GetValue(value), kvKey.PropertyType, depth + 1, maxDepth);
            PushObject(kvValue.GetValue(value), kvValue.PropertyType, depth + 1, maxDepth);
        }
        
        #endregion

        public void Dispose()
        {
        }
    }

    // TODO: Add object type serializer (for PushObject, instead of calling the internal serializer, calls the callback instead)
}
