using FastMember;
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
        private readonly IDictionary<Type, PropertyInfo[]> _propertiesByType = new Dictionary<Type, PropertyInfo[]>();
        private static readonly IImmutableDictionary<Type, Action<BigObjectSerializer, object>> _basicTypePushMethods;
        // Describes the mapping of string to int for each property name. Is serialized in the first instance of a type that doesn't already have a serialized mapping.
        private readonly IDictionary<Type, IImmutableDictionary<string, byte>> _propertyToIntMapping = new Dictionary<Type, IImmutableDictionary<string, byte>>(); // NOTE: Byte only allows up to 255 properties. Benchmark with short also
        private readonly IDictionary<Type, TypeAccessor> _getters = new Dictionary<Type, TypeAccessor>();
        private readonly IDictionary<Type, (Type keyType, Type valueType)> _keyValueTypes = new Dictionary<Type, (Type keyType, Type valueType)>();
        private readonly IDictionary<Type, bool> _isKeyValuePair = new Dictionary<Type, bool>();

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
            => PushValue(value, typeof(T), 1, maxDepth);

        public void PushObject(object value, Type type, int maxDepth = 10)
            => PushValue(value, type, 1, maxDepth);

        private void PushObject(object value, Type type, int depth, int maxDepth)
        {
            if (depth > maxDepth) return; // Ignore properties past max depth
            
            if (GetPropertyNameMapping(type, out var propertyNameMappings))
            {
                PushValue(propertyNameMappings, typeof(IDictionary<string, byte>), depth + 1, maxDepth);
            }

            var getters = GetTypeAccessor(type);
            var properties = GetPropertiesToGet(type);
            for (var i = 0; i < properties.Length; ++i) // For now we only consider properties with getter/setter
            {
                var propertyType = properties[i].PropertyType;
                var name = properties[i].Name;
                var propertyValue = getters[value, name];
                
                PushByte(propertyNameMappings[name]);

                if (propertyValue is null)
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
            PushByte(0); // Mark end of object (since no property can have an empty string label)
        }

        private TypeAccessor GetTypeAccessor(Type type)
        {
            if (!_getters.TryGetValue(type, out var getter))
            {
                getter = _getters[type] = TypeAccessor.Create(type);
            }
            return getter;
        }
        
        private PropertyInfo[] GetPropertiesToGet(Type type)
        {
            if (!_propertiesByType.TryGetValue(type, out var properties))
            {
                properties = _propertiesByType[type] =
                    _propertiesByType[type] = type
                    .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                    .Where(p => p.CanRead && p.CanWrite)
                    .ToArray();
            }
            return properties;
        }

        private void PushValue(object value, Type type, int depth, int maxDepth)
        {
            // Null check
            if (value is null)
            {
                return;
            }

            if (_basicTypePushMethods.TryGetValue(type, out var pushMethod))
            {
                // Property was basic supported type and was pushed
                pushMethod(this, value);
                return;
            }
            else if (IsKeyValuePair(type))
            {
                PushKeyValuePair(value, type, depth + 1, maxDepth);
                return;
            }
            else if (type.IsArray || typeof(IEnumerable).IsAssignableFrom(type))
            {
                // For collections, we can just store the values and let the deserializer figure out what container to put them inside
                var genericType = Utilities.GetElementType(type);
                var enumerable = ((IEnumerable)value).OfType<object>().ToArray();

                PushInt(enumerable.Length); // Store the length of the enumerable
                for (var i = 0; i < enumerable.Length; ++i)
                {
                    PushValue(enumerable[i], genericType, depth + 1, maxDepth);
                }
            }
            else // TODO: Handle structs
            {
                PushObject(value, type, depth + 1, maxDepth);
            }
            /*else
            {
                throw new NotImplementedException($"{nameof(PushObject)} does not support serializing type of {type.FullName}");
            }*/
        }

        private void PushKeyValuePair(object value, Type type, int depth, int maxDepth)
        {
            // KeyValuePair is pushed as the key then value
            if (!_keyValueTypes.TryGetValue(type, out var kvType))
            {
                var properties = type.GetProperties();
                var kvKeyProperty = properties.First(p => p.Name == nameof(KeyValuePair<object, object>.Key));
                var kvValueProperty = properties.First(p => p.Name == nameof(KeyValuePair<object, object>.Value));
                kvType = _keyValueTypes[type] = (kvKeyProperty.PropertyType, kvValueProperty.PropertyType);
            }
            var (keyType, valueType) = kvType;

            var getters = GetTypeAccessor(type);
            var kvKey = getters[value, nameof(KeyValuePair<object, object>.Key)];
            var kvValue = getters[value, nameof(KeyValuePair<object, object>.Value)];

            // TODO: Handle null key or value
            PushValue(kvKey, keyType, depth + 1, maxDepth);
            PushValue(kvValue, valueType, depth + 1, maxDepth);
        }

        private bool IsKeyValuePair(Type type)
        {
            if (!_isKeyValuePair.TryGetValue(type, out var isKeyValueType))
            {
                var typeWithoutGenerics = type.IsGenericType ? type.GetGenericTypeDefinition() : type;
                isKeyValueType = _isKeyValuePair[type] = typeof(KeyValuePair<,>).IsAssignableFrom(typeWithoutGenerics);
            }
            return isKeyValueType;
        }

        #endregion

        /// <summary>
        /// 
        /// </summary>
        /// <param name="type"></param>
        /// <param name="mapping"></param>
        /// <returns>Returns true if the first time the type to map has appeared.</returns>
        private bool GetPropertyNameMapping(Type type, out IImmutableDictionary<string, byte> mapping)
        {
            if (!_propertyToIntMapping.TryGetValue(type, out var mappingResult))
            {
                var newMapping = new Dictionary<string, byte>();
                byte counter = 1; // 0 == end of object
                var properties = GetPropertiesToGet(type);
                for (var i = 0; i < properties.Length; ++i)
                {
                    newMapping[properties[i].Name] = counter++;
                }
                _propertyToIntMapping[type] = newMapping.ToImmutableDictionary();

                mapping = _propertyToIntMapping[type];
                return true;
            }

            mapping = mappingResult;
            return false;
        }

        public void Dispose()
        {
        }
    }

    // TODO: Add object type serializer (for PushObject, instead of calling the internal serializer, calls the callback instead)
}
