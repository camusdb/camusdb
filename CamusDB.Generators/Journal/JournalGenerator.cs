
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Diagnostics;
using CamusDB.Generators.Utils;
using Microsoft.CodeAnalysis;
using System.Collections.Generic;
using Microsoft.CodeAnalysis.Text;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace CamusDB.Generators.Journal
{
    [Generator]
    internal class JournalGenerator : ISourceGenerator
    {
        public void Initialize(GeneratorInitializationContext context)
        {
            context.RegisterForSyntaxNotifications(() => new SyntaxReceiver());
        }

        private void GeneratePayloadLength(ITypeSymbol symbol, StringBuilder sb, List<string> parameters)
        {
            sb.Append("\t\tprivate static int GetPayloadLength(");
            sb.Append(string.Join(", ", parameters));
            sb.AppendLine(")");
            sb.AppendLine("\t\t{");
            sb.AppendLine("\t\t\tint length = 0;");

            foreach (var property in JournalHelper.GetProperties(symbol))
                JournalPayloadLength.GetParameterLength(sb, property);

            sb.AppendLine("\t\t\treturn length;");
            sb.AppendLine("\t\t}\n");
        }

        private void GeneratePayload(ITypeSymbol symbol, StringBuilder sb, List<string> parameters)
        {
            sb.Append("\t\tprivate static void WritePayload(byte[] journal, ");
            sb.Append(string.Join(", ", parameters));
            sb.AppendLine(", ref int pointer)");
            sb.AppendLine("\t\t{");

            foreach (var property in JournalHelper.GetProperties(symbol))
                JournalPayloadSerialize.WriteParameter(sb, property);

            sb.AppendLine("\t\t}\n");
        }

        private void GenerateSerialize(StringBuilder sb, ITypeSymbol symbol, List<string> callParameters)
        {
            sb.AppendLine($"\t\tpublic static byte[] Serialize(uint sequence, {symbol.Name} data)");
            sb.AppendLine("\t\t{");

            sb.Append("\t\t\tint length = GetPayloadLength(");
            sb.Append(string.Join(", ", callParameters));
            sb.AppendLine(");\n");

            sb.AppendLine("\t\t\tbyte[] journal = new byte[");
            sb.AppendLine("\t\t\t\tSerializatorTypeSizes.TypeInteger32 + // LSN (4 bytes)");
            sb.AppendLine("\t\t\t\tSerializatorTypeSizes.TypeInteger16 + // journal type (2 bytes)");
            sb.AppendLine("\t\t\t\tlength // payload");
            sb.AppendLine("\t\t\t];\n");

            sb.AppendLine("\t\t\tint pointer = 0;\n");

            sb.AppendLine("\t\t\tSerializator.WriteUInt32(journal, sequence, ref pointer);");
            sb.AppendLine("\t\t\tSerializator.WriteInt16(journal, (short)JournalLogTypes.Insert, ref pointer);\n");

            sb.Append("\t\t\tWritePayload(journal, ");
            sb.Append(string.Join(", ", callParameters));
            sb.AppendLine(", ref pointer);\n");

            sb.AppendLine("\t\t\treturn journal;");

            sb.AppendLine("\t\t}\n");
        }

        private void GenerateDeserialize(StringBuilder sb, ITypeSymbol symbol, List<string> callParameters)
        {
            sb.AppendLine($"\t\tpublic static async Task<{symbol.Name}> Deserialize(FileStream journal)");
            sb.AppendLine("\t\t{");

            foreach (var property in JournalHelper.GetProperties(symbol))
                JournalPayloadSerialize.ReadParameter(sb, property);

            sb.Append("\t\t\treturn new(");
            sb.Append(string.Join(", ", callParameters));
            sb.AppendLine(");");

            sb.AppendLine("\t\t}");
        }

        public void Execute(GeneratorExecutionContext context)
        {
            // Get our SyntaxReceiver back
            if (!(context.SyntaxReceiver is SyntaxReceiver receiver))
                throw new ArgumentException("Received invalid receiver in Execute step");

            foreach (var node in receiver.CandidateSyntaxes)
            {
                var model = context.Compilation.GetSemanticModel(node.SyntaxTree);
                var symbol = model.GetDeclaredSymbol(node, context.CancellationToken) as ITypeSymbol;

                var sb = new StringBuilder();

                sb.AppendLine("using System.Diagnostics;");
                sb.AppendLine("using CamusDB.Core.Serializer;");
                sb.AppendLine("using CamusDB.Core.Journal.Utils;");
                sb.AppendLine("using CamusDB.Core.Journal.Models;");
                sb.AppendLine("using CamusDB.Core.Serializer.Models;");
                sb.AppendLine("using CamusDB.Core.CommandsExecutor.Models;");
                sb.Append("\n");

                sb.AppendLine("namespace " + symbol.ContainingNamespace);
                sb.AppendLine("{");
                sb.AppendLine("\tpublic sealed class " + symbol.Name + "Serializator");
                sb.AppendLine("\t{");

                List<string> parameters = new();

                foreach (var property in JournalHelper.GetProperties(symbol))
                    JournalPayloadParameters.GetMethodParameters(property, parameters);

                List<string> callParameters = new();

                foreach (var property in JournalHelper.GetProperties(symbol))
                    JournalPayloadParameters.GetCallParameters(property, callParameters);

                List<string> dataCallParameters = new();

                foreach (var property in JournalHelper.GetProperties(symbol))
                    JournalPayloadParameters.GetDataCallParameters(property, dataCallParameters);

                GeneratePayloadLength(symbol, sb, parameters);
                GeneratePayload(symbol, sb, parameters);
                GenerateSerialize(sb, symbol, dataCallParameters);
                GenerateDeserialize(sb, symbol, callParameters);

                sb.AppendLine("\t}");
                sb.AppendLine("}");

                context.AddSource((symbol.ContainingNamespace + "." + symbol.Name + "Serializator.cs"), SourceText.From(sb.ToString(), Encoding.UTF8));

                File.WriteAllText("/tmp/" + (symbol.ContainingNamespace + "." + symbol.Name + "Serializator.cs"), (symbol.ContainingNamespace + "." + symbol.Name + "Serializator.cs").ToLowerInvariant() + "\n" + sb.ToString() + "\n");
            }
        }
    }
}