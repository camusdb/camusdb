
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Text;
using CamusDB.Generators.Utils;
using Microsoft.CodeAnalysis;

namespace CamusDB.Generators.Journal
{
    internal class JournalPayloadSerialize
    {
        private static void WriteDictionaryKey(StringBuilder sb, (ITypeSymbol type, string fullName, string name) typeDef)
        {
            string type = typeDef.type.Name.ToString();

            switch (type)
            {
                case "String":
                    sb.Append($"\t\t\t\tSerializator.WriteInt16(journal, keyValuePair.Key.Length, ref pointer);\n");
                    sb.AppendLine($"\t\t\t\tSerializator.WriteString(journal, keyValuePair.Key, ref pointer);\n");
                    break;

                /*case "Int32":
                    sb.AppendLine("\t\t\t\tlength += SerializatorTypeSizes.TypeInteger32;");
                    break;*/

                default:
                    throw new Exception("Unsupported Dictionary<TKey,TValue> key type: " + type);
            }
        }

        private static void WriteDictionaryValue(StringBuilder sb, (ITypeSymbol type, string fullName, string name) typeDef)
        {
            string type = typeDef.type.Name.ToString();

            switch (typeDef.fullName)
            {
                case "System.String":
                    sb.AppendLine("\t\t\t\tlength += keyValuePair.Value.Length;");
                    break;

                case "System.Int32":
                    sb.AppendLine("\t\t\t\tlength += SerializatorTypeSizes.TypeInteger32;");
                    break;

                case "CamusDB.Core.CommandsExecutor.Models.ColumnValue":
                    sb.AppendLine("\t\t\t\tSerializatorHelper.WriteColumnValue(journal, keyValuePair.Value, ref pointer);");
                    break;

                default:
                    throw new Exception("Unsupported Dictionary<TKey,TValue> value type: " + typeDef.fullName);
            }
        }

        private static void WriteDictionary(StringBuilder sb, IPropertySymbol symbol)
        {
            (ITypeSymbol type, string fullName, string name) typeOne = JournalHelper.GetGenericArgumentType(symbol, 0);
            (ITypeSymbol type, string fullName, string name) typeTwo = JournalHelper.GetGenericArgumentType(symbol, 1);

            string dict = JournalHelper.Uncamelize(symbol.Name);

            sb.AppendLine($"\t\t\tSerializator.WriteInt16(journal, {dict}.Count, ref pointer);\n");

            sb.AppendLine("\t\t\tforeach (KeyValuePair<" + typeOne.fullName + "," + typeTwo.fullName + "> keyValuePair in " + dict + ")");
            sb.AppendLine("\t\t\t{");

            WriteDictionaryKey(sb, typeOne);
            WriteDictionaryValue(sb, typeTwo);

            sb.AppendLine("\t\t\t}\n");
        }

        private static void ReadDictionaryKey(StringBuilder sb, (ITypeSymbol type, string fullName, string name) typeDef)
        {
            string type = typeDef.type.Name.ToString();

            switch (type)
            {
                case "String":
                    sb.AppendLine($"\t\t\t\tshort _keyLength = await SerializatorHelper.ReadInt16(journal);");
                    sb.AppendLine($"\t\t\t\tstring _key = await SerializatorHelper.ReadString(journal, _keyLength);\n");
                    break;

                /*case "Int32":
                    sb.AppendLine("\t\t\t\tlength += SerializatorTypeSizes.TypeInteger32;");
                    break;*/

                default:
                    throw new Exception("Unsupported Dictionary<TKey,TValue> key type: " + type);
            }
        }

        private static void ReadDictionaryValue(StringBuilder sb, (ITypeSymbol type, string fullName, string name) typeDef)
        {
            string type = typeDef.type.Name.ToString();

            switch (typeDef.fullName)
            {
                case "System.String":
                    sb.AppendLine("\t\t\t\tlength += keyValuePair.Value.Length;");
                    break;

                case "System.Int32":
                    sb.AppendLine("\t\t\t\tlength += SerializatorTypeSizes.TypeInteger32;");
                    break;

                case "CamusDB.Core.CommandsExecutor.Models.ColumnValue":
                    sb.AppendLine("\t\t\t\tColumnValue _value = await SerializatorHelper.ReadColumnValue(journal);");
                    break;

                default:
                    throw new Exception("Unsupported Dictionary<TKey,TValue> value type: " + typeDef.fullName);
            }
        }

        private static void ReadDictionary(StringBuilder sb, IPropertySymbol symbol)
        {
            (ITypeSymbol type, string fullName, string name) typeOne = JournalHelper.GetGenericArgumentType(symbol, 0);
            (ITypeSymbol type, string fullName, string name) typeTwo = JournalHelper.GetGenericArgumentType(symbol, 1);

            string dict = JournalHelper.Uncamelize(symbol.Name);

            sb.AppendLine($"\t\t\tshort {dict}Count = await SerializatorHelper.ReadInt16(journal);\n");
            sb.AppendLine($"\t\t\tDictionary<{typeOne.fullName},{typeTwo.fullName}> {dict} = new();\n");

            sb.AppendLine($"\t\t\tfor (int i = 0; i < {dict}Count; i++)");
            sb.AppendLine("\t\t\t{");

            ReadDictionaryKey(sb, typeOne);
            ReadDictionaryValue(sb, typeTwo);

            sb.AppendLine($"\t\t\t\t{dict}.Add(_key, _value);");

            sb.AppendLine("\t\t\t}\n");
        }

        public static void WriteParameter(StringBuilder sb, IPropertySymbol symbol)
        {
            if (!JournalHelper.IsJournalField(symbol))
                return;
            
            string varName = JournalHelper.Uncamelize(symbol.Name);
            string fullName = symbol.Type.ContainingNamespace + "." + symbol.Type.Name;

            switch (fullName)
            {
                case "System.String":
                    sb.Append($"\t\t\tSerializator.WriteInt16(journal, {varName}.Length, ref pointer);\n");
                    sb.AppendLine($"\t\t\tSerializator.WriteString(journal, {varName}, ref pointer);\n");
                    break;

                case "System.UInt32":
                    sb.AppendLine($"\t\t\tSerializator.WriteUInt32(journal, {varName}, ref pointer);\n");                    
                    break;

                case "CamusDB.Core.Util.Trees.BTreeTuple":
                    sb.AppendLine($"\t\t\tSerializator.WriteInt32(journal, {varName}.SlotOne, ref pointer);\n");
                    sb.AppendLine($"\t\t\tSerializator.WriteInt32(journal, {varName}.SlotTwo, ref pointer);\n");
                    break;

                case "System.Collections.Generic.Dictionary":
                    WriteDictionary(sb, symbol);
                    break;

                default:                    
                    throw new Exception("Unsupported WriteParameter type " + fullName);
            }
        }

        public static void ReadParameter(StringBuilder sb, IPropertySymbol symbol)
        {
            if (!JournalHelper.IsJournalField(symbol))
                return;
            
            string varName = JournalHelper.Uncamelize(symbol.Name);
            string fullName = symbol.Type.ContainingNamespace + "." + symbol.Type.Name;

            switch (fullName)
            {
                case "System.String":
                    sb.AppendLine($"\t\t\tshort {varName}Length = await SerializatorHelper.ReadInt16(journal);");
                    sb.AppendLine($"\t\t\tstring {varName} = await SerializatorHelper.ReadString(journal, {varName}Length);\n");
                    break;

                case "System.UInt32":
                    sb.AppendLine($"\t\t\tuint {varName} = await SerializatorHelper.ReadUInt32(journal);\n");
                    break;

                case "CamusDB.Core.Util.Trees.BTreeTuple":
                    sb.AppendLine($"\t\t\tCamusDB.Core.Util.Trees.BTreeTuple {varName} = new(\n");
                    sb.AppendLine($"\t\t\t\tawait SerializatorHelper.ReadInt32(journal),\n");
                    sb.AppendLine($"\t\t\t\tawait SerializatorHelper.ReadInt32(journal)\n");
                    sb.AppendLine(");");
                    break;

                case "System.Collections.Generic.Dictionary":
                    ReadDictionary(sb, symbol);
                    break;

                default:
                    throw new Exception("Unsupported ReadParameter type " + fullName);
            }
        }
    }
}

