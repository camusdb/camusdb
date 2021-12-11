
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Microsoft.CodeAnalysis;
using System.Collections.Generic;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace CamusDB.Generators.Journal
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
}

