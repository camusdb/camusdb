using System;
using System.Text;
using CamusDB.Generators.Utils;
using Microsoft.CodeAnalysis;

namespace CamusDB.Generators.Journal
{
    public class JournalPayloadLength
    {
        private static void GetDictionaryKeyLength(StringBuilder sb, (ITypeSymbol type, string fullName, string name) typeDef)
        {
            string type = typeDef.type.Name.ToString();

            switch (type)
            {
                case "String":
                    sb.AppendLine("\t\t\t\tlength += SerializatorTypeSizes.TypeInteger16 + keyValuePair.Key.Length;");
                    break;

                case "Int32":
                    sb.AppendLine("\t\t\t\tlength += SerializatorTypeSizes.TypeInteger32;");
                    break;

                default:
                    throw new Exception("Unsupported Dictionary<TKey,TValue> key type: " + type);
            }
        }

        private static void GetDictionaryValueLength(StringBuilder sb, (ITypeSymbol type, string fullName, string name) typeDef)
        {
            string type = typeDef.type.Name.ToString();

            switch (typeDef.fullName)
            {
                case "System.String":
                    sb.AppendLine("\t\t\t\tlength += SerializatorTypeSizes.TypeInteger16 + keyValuePair.Value.Length;");
                    break;

                case "System.Int32":
                    sb.AppendLine("\t\t\t\tlength += SerializatorTypeSizes.TypeInteger32;");
                    break;

                case "CamusDB.Core.CommandsExecutor.Models.ColumnValue":
                    sb.AppendLine("\t\t\t\tlength += SerializatorHelper.GetColumnValueLength(keyValuePair.Value);");
                    break;

                default:
                    throw new Exception("Unsupported Dictionary<TKey,TValue> value type: " + typeDef.fullName);
            }
        }

        private static void GetDictionaryLength(StringBuilder sb, IPropertySymbol symbol)
        {
            (ITypeSymbol type, string fullName, string name) typeOne = JournalHelper.GetGenericArgumentType(symbol, 0);
            (ITypeSymbol type, string fullName, string name) typeTwo = JournalHelper.GetGenericArgumentType(symbol, 1);

            string dict = JournalHelper.Uncamelize(symbol.Name);

            sb.AppendLine("\t\t\tlength += SerializatorTypeSizes.TypeInteger16;\n");

            sb.AppendLine("\t\t\tforeach (KeyValuePair<" + typeOne.fullName + "," + typeTwo.fullName + "> keyValuePair in " + dict + ")");
            sb.AppendLine("\t\t\t{");

            GetDictionaryKeyLength(sb, typeOne);
            GetDictionaryValueLength(sb, typeTwo);

            sb.AppendLine("\t\t\t}\n");
        }

        private static void GetArrayParameterLength(StringBuilder sb, IPropertySymbol symbol, ITypeSymbol type)
        {
            string fullName = type.ContainingNamespace + "." + type.Name;

            sb.AppendLine("\t\t\tlength += SerializatorTypeSizes.TypeUnsignedInteger32;");

            switch (fullName)
            {
                case "System.Byte":
                    sb.Append("\t\t\tlength += SerializatorTypeSizes.TypeInteger8 * ");
                    sb.Append(JournalHelper.Uncamelize(symbol.Name));
                    sb.AppendLine(".Length;\n");
                    break;

                default:
                    throw new Exception("Unsupported array GetParameterLength type: " + fullName);
            }
        }

        public static void GetParameterLength(StringBuilder sb, IPropertySymbol symbol)
        {
            if (!JournalHelper.IsJournalField(symbol))
                return;            

            if (symbol.Type.Kind == SymbolKind.ArrayType)
            {
                var element = ((IArrayTypeSymbol)symbol.Type).ElementType;
                GetArrayParameterLength(sb, symbol, element);
                return;
            }

            string fullName = symbol.Type.ContainingNamespace + "." + symbol.Type.Name;

            switch (fullName)
            {
                case "System.String":
                    sb.Append("\t\t\tlength += SerializatorTypeSizes.TypeInteger16 + ");
                    sb.Append(JournalHelper.Uncamelize(symbol.Name));
                    sb.AppendLine(".Length;\n");
                    break;

                case "System.UInt32":
                    sb.AppendLine("\t\t\tlength += SerializatorTypeSizes.TypeUnsignedInteger32;");
                    break;

                case "CamusDB.Core.Util.Trees.BTreeTuple":
                    sb.AppendLine("\t\t\tlength += SerializatorTypeSizes.TypeInteger32 + SerializatorTypeSizes.TypeInteger32;");
                    break;

                case "System.Collections.Generic.Dictionary":
                    GetDictionaryLength(sb, symbol);
                    break;

                default:
                    throw new Exception("Unsupported GetParameterLength type: " + fullName + " - " + symbol.Type.Kind);
            }
        }
    }
}

