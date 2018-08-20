using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace BigObjectSerializer
{
    internal static class Utilities
    {
        public static bool IsAssignableToGenericType(Type givenType, Type genericType)
        {
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

            return IsAssignableToGenericType(baseType, genericType);
        }

        public static object CreateFromEnumerableConstructor(Type genericContainerType, Type genericParameter, IEnumerable entries)
        {
            var castEntries = ConvertTo(entries, genericParameter);
            var enumerableConstructor = genericContainerType.GetConstructors().First(c =>
            {
                var paramaters = c.GetParameters();
                if (paramaters.Length == 0) return false;

                var parameterType = paramaters.FirstOrDefault().ParameterType;
                if (parameterType.IsGenericType) parameterType = parameterType.GetGenericTypeDefinition();
                return paramaters.Length == 1 && !typeof(IDictionary<,>).IsAssignableFrom(parameterType) && typeof(IEnumerable).IsAssignableFrom(parameterType);
            });
            return enumerableConstructor.Invoke(new[] { castEntries });
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

        #region Convert IEnumerable

        // Source: https://stackoverflow.com/questions/17758888/how-to-cast-generic-list-of-one-type-to-generic-list-of-an-unknown-type
        private static readonly MethodInfo _convertToMethod = typeof(Utilities).GetMethod(nameof(ConvertTo), new[] { typeof(IEnumerable) });
        private static readonly MethodInfo _convertToListMethod = typeof(Utilities).GetMethod(nameof(ConvertToList), new[] { typeof(IEnumerable) });
        private static readonly MethodInfo _convertToArrayMethod = typeof(Utilities).GetMethod(nameof(ConvertToArray), new[] { typeof(IEnumerable) });

        public static IEnumerable<T> ConvertTo<T>(this IEnumerable items)
        {
            // see method above
            return items.Cast<T>();
        }

        public static IEnumerable ConvertTo(this IEnumerable items, Type targetType)
        {
            var generic = _convertToMethod.MakeGenericMethod(targetType);
            return (IEnumerable)generic.Invoke(null, new[] { items });
        }

        public static IList<T> ConvertToList<T>(this IEnumerable items)
        {
            // see method above
            return items.Cast<T>().ToList();
        }

        public static IList ConvertToList(this IEnumerable items, Type targetType)
        {
            var generic = _convertToListMethod.MakeGenericMethod(targetType);
            return (IList)generic.Invoke(null, new[] { items });
        }
        
        public static T[] ConvertToArray<T>(this IEnumerable items)
        {
            // see method above
            return items.Cast<T>().ToArray<T>();
        }

        public static object ConvertToArray(this IEnumerable items, Type targetType)
        {
            var generic = _convertToArrayMethod.MakeGenericMethod(targetType);
            return generic.Invoke(null, new[] { items });
        }

        #endregion
    }
}
