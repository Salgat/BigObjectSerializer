using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace BigObjectSerializer
{
    internal static class Utilities
    {
        private static readonly ConcurrentDictionary<(Type, Type), bool> _isAssignableToGenericType = new ConcurrentDictionary<(Type, Type), bool>();
        private static readonly ConcurrentDictionary<(Type, Type), ConstructorInfo> _createFromEnumerableConstructor = new ConcurrentDictionary<(Type, Type), ConstructorInfo>();
        private static readonly ConcurrentDictionary<Type, Type> _getElementType = new ConcurrentDictionary<Type, Type>();

        public static bool IsAssignableToGenericType(Type givenType, Type genericType)
        {
            var key = (givenType, genericType);
            if (_isAssignableToGenericType.ContainsKey(key)) return _isAssignableToGenericType[key];

            // Source: https://stackoverflow.com/questions/74616/how-to-detect-if-type-is-another-generic-type/1075059#1075059
            var interfaceTypes = givenType.GetInterfaces();

            foreach (var it in interfaceTypes)
            {
                if (it.IsGenericType && it.GetGenericTypeDefinition() == genericType)
                    return true;
            }

            if (givenType.IsGenericType && givenType.GetGenericTypeDefinition() == genericType)
                return true;

            Type baseType = givenType.BaseType;
            if (baseType == null) return false;
            
            return _isAssignableToGenericType[key] = IsAssignableToGenericType(baseType, genericType);
        }

        public static object CreateFromEnumerableConstructor(Type genericContainerType, Type genericParameter, IEnumerable entries)
        {
            var castEntries = ConvertTo(entries, genericParameter);

            var key = (genericContainerType, genericParameter);
            if (_createFromEnumerableConstructor.ContainsKey(key))
            {
                return _createFromEnumerableConstructor[key].Invoke(new[] { castEntries });
            }
            else
            {
                var enumerableConstructor = genericContainerType.GetConstructors().First(c =>
                {
                    var paramaters = c.GetParameters();
                    if (paramaters.Length == 0) return false;

                    var parameterType = paramaters.FirstOrDefault().ParameterType;
                    if (parameterType.IsGenericType) parameterType = parameterType.GetGenericTypeDefinition();
                    return paramaters.Length == 1 && !typeof(IDictionary<,>).IsAssignableFrom(parameterType) && typeof(IEnumerable).IsAssignableFrom(parameterType);
                });
                _createFromEnumerableConstructor[key] = enumerableConstructor;

                return enumerableConstructor.Invoke(new[] { castEntries });
            }
        }

        public static object GetDefault(Type type)
        {
            // Source: https://stackoverflow.com/questions/325426/programmatic-equivalent-of-defaulttype
            if (type.IsValueType)
            {
                return Activator.CreateInstance(type);
            }
            return null;
        }

        public static Type GetElementType(Type type)
        {
            if (_getElementType.ContainsKey(type)) return _getElementType[type];

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
            return _getElementType[type] = enumType ?? type;
        }

        #region Convert IEnumerable

        // Source: https://stackoverflow.com/questions/17758888/how-to-cast-generic-list-of-one-type-to-generic-list-of-an-unknown-type
        private static readonly MethodInfo _convertToMethod = typeof(Utilities).GetMethod(nameof(ConvertTo), new[] { typeof(IEnumerable) });
        private static readonly ConcurrentDictionary<Type, MethodInfo> _convertToMakeGeneric = new ConcurrentDictionary<Type, MethodInfo>();

        private static readonly MethodInfo _convertToListMethod = typeof(Utilities).GetMethod(nameof(ConvertToList), new[] { typeof(IEnumerable) });
        private static readonly ConcurrentDictionary<Type, MethodInfo> _convertToListMakeGeneric = new ConcurrentDictionary<Type, MethodInfo>();

        private static readonly MethodInfo _convertToArrayMethod = typeof(Utilities).GetMethod(nameof(ConvertToArray), new[] { typeof(IEnumerable) });
        private static readonly ConcurrentDictionary<Type, MethodInfo> _convertToArrayMakeGeneric = new ConcurrentDictionary<Type, MethodInfo>();

        public static IEnumerable<T> ConvertTo<T>(this IEnumerable items)
        {
            // see method above
            return items.Cast<T>();
        }

        public static IEnumerable ConvertTo(this IEnumerable items, Type targetType)
        {
            if (_convertToMakeGeneric.ContainsKey(targetType))
            {
                return (IEnumerable)_convertToMakeGeneric[targetType].Invoke(null, new[] { items });
            }
            else
            {
                var generic = _convertToMakeGeneric[targetType] = _convertToMethod.MakeGenericMethod(targetType);
                return (IEnumerable)generic.Invoke(null, new[] { items });
            }
        }

        public static IList<T> ConvertToList<T>(this IEnumerable items)
        {
            // see method above
            return items.Cast<T>().ToList();
        }

        public static IList ConvertToList(this IEnumerable items, Type targetType)
        {
            if (_convertToListMakeGeneric.ContainsKey(targetType))
            {
                return (IList)_convertToListMakeGeneric[targetType].Invoke(null, new[] { items });
            }
            else
            {
                var generic = _convertToListMakeGeneric[targetType] = _convertToListMethod.MakeGenericMethod(targetType);
                return (IList)generic.Invoke(null, new[] { items });
            }
        }
        
        public static T[] ConvertToArray<T>(this IEnumerable items)
        {
            // see method above
            return items.Cast<T>().ToArray<T>();
        }

        public static object ConvertToArray(this IEnumerable items, Type targetType)
        {
            if (_convertToArrayMakeGeneric.ContainsKey(targetType))
            {
                return _convertToArrayMakeGeneric[targetType].Invoke(null, new[] { items });
            }
            else
            {
                var generic = _convertToArrayMakeGeneric[targetType] = _convertToArrayMethod.MakeGenericMethod(targetType);
                return generic.Invoke(null, new[] { items });
            }
        }

        #endregion
    }
}
