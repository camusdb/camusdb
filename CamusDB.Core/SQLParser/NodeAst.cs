
namespace CamusDB.Core.SQLParser;

public sealed class NodeAst
{
	public NodeType nodeType;

    public NodeAst leftAst;

    public NodeAst rightAst;

    public NodeAst(NodeType nodeType, NodeAst leftAst, NodeAst rightAst)
	{
		this.nodeType = nodeType;
		this.leftAst = leftAst;
		this.rightAst = rightAst;
	}
}
