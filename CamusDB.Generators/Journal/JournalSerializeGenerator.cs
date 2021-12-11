
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using CamusDB.Generators.Utils;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace CamusDB.Generators.Journal
{
    [Generator]
    public class JournalSerializeGenerator : ISourceGenerator
    {
        internal class SyntaxReceiver : ISyntaxReceiver
        {
            private readonly List<ClassDeclarationSyntax> candidateSyntaxes = new List<ClassDeclarationSyntax>();

            public IReadOnlyList<ClassDeclarationSyntax> CandidateSyntaxes => candidateSyntaxes;

            public void OnVisitSyntaxNode(SyntaxNode syntaxNode)
            {
                if (!(syntaxNode is ClassDeclarationSyntax classSyntax))
                    return;

                foreach (var attributeList in classSyntax.AttributeLists)
                {
                    foreach (var attribute in attributeList.Attributes)
                    {
                        string attributeName = attribute.Name.ToString();
                        if (attributeName == "JournalSerializable")
                            candidateSyntaxes.Add(classSyntax);
                    }
                }
            }
        }
               
        private void GetMethodParameters(IPropertySymbol symbol, List<string> parameters)
        {
            if (!JournalHelper.IsJournalField(symbol))
                return;

            string type = symbol.Type.Name.ToString();

            switch (type)
            {
                case "String":
                    parameters.Add("string " + JournalHelper.Uncamelize(symbol.Name));
                    break;

                case "Int32":
                    parameters.Add("int " + JournalHelper.Uncamelize(symbol.Name));
                    break;

                case "Boolean":
                    parameters.Add("bool " + JournalHelper.Uncamelize(symbol.Name));
                    break;

                case "List":
                    {
                        (ITypeSymbol type, string fullName, string name) typeOne = JournalHelper.GetGenericArgumentType(symbol, 0);
                        parameters.Add("List<" + typeOne.fullName + "> " + JournalHelper.Uncamelize(symbol.Name));
                    }
                    break;

                case "Dictionary":
                    {
                        (ITypeSymbol type, string fullName, string name) typeOne = JournalHelper.GetGenericArgumentType(symbol, 0);
                        (ITypeSymbol type, string fullName, string name) typeTwo = JournalHelper.GetGenericArgumentType(symbol, 1);
                        parameters.Add("Dictionary<" + typeOne.fullName + "," + typeTwo.fullName + "> " + JournalHelper.Uncamelize(symbol.Name));
                    }
                    break;

                default:
                    string fullName = symbol.Type.ContainingNamespace + "." + symbol.Type.Name;
                    break;
            }
        }
        
        private void GetCallParameters(IPropertySymbol symbol, List<string> parameters)
        {
            if (!JournalHelper.IsJournalField(symbol))
                return;

            parameters.Add(JournalHelper.Uncamelize(symbol.Name));
        }

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

        private void GenerateSerialize(StringBuilder sb, List<string> parameters, List<string> callParameters)
        {
            sb.Append("\t\tpublic static byte[] Serialize(uint sequence, ");
            sb.Append(string.Join(", ", parameters));
            sb.AppendLine(")");

            sb.AppendLine("\t\t{");
            //sb.AppendLine("\t\t\treturn Array.Empty<byte>();");

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
            sb.AppendLine("\t\t\tSerializator.WriteInt16(journal, (short)JournalLogTypes.InsertTicket, ref pointer);");

            sb.AppendLine("\t\t\treturn journal;");

            sb.AppendLine("\t\t}");
            sb.AppendLine("\t}");
            sb.AppendLine("}");
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

                //throw new Exception(node.ToFullString());
                var sb = new StringBuilder();

                sb.AppendLine("using System.Diagnostics;");
                sb.AppendLine("using CamusDB.Core.Serializer;");                
                sb.AppendLine("using CamusDB.Core.Journal.Utils;");
                sb.AppendLine("using CamusDB.Core.Journal.Models;");
                sb.AppendLine("using CamusDB.Core.Serializer.Models;");
                sb.Append("\n");

                sb.AppendLine("namespace CamusDB.Core.Journal");
                sb.AppendLine("{");
                sb.AppendLine("\tpublic sealed class " + symbol.Name + "Serializator");
                sb.AppendLine("\t{");

                List<string> parameters = new();

                foreach (var property in JournalHelper.GetProperties(symbol))
                    GetMethodParameters(property, parameters);

                List<string> callParameters = new();

                foreach (var property in JournalHelper.GetProperties(symbol))
                    GetCallParameters(property, callParameters);

                GeneratePayloadLength(symbol, sb, parameters);
                GenerateSerialize(sb, parameters, callParameters);

                //  public static void XX() { System.Console.WriteLine(\"hello from the other side\"); } }

                context.AddSource((symbol.ContainingNamespace + "." + symbol.Name + "Serializator.cs"), SourceText.From(sb.ToString(), Encoding.UTF8));

                File.WriteAllText("/tmp/" + (symbol.ContainingNamespace + "." + symbol.Name + "Serializator.cs"), (symbol.ContainingNamespace + "." + symbol.Name + "Serializator.cs").ToLowerInvariant() + "\n" + sb.ToString() + "\n");
            }
        }
    }
}