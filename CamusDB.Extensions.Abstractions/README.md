# CamusDB.Extensions.Abstractions

The contract for [CamusDB](https://github.com/camusdb/camusdb) C# extensions. Reference this package
from a class library to write SQL scalar functions, row triggers and stored procedures that a CamusDB
server loads at startup.

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>netstandard2.0</TargetFramework>
    <LangVersion>latest</LangVersion>
    <Nullable>enable</Nullable>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="CamusDB.Extensions.Abstractions" Version="0.16.*" />
  </ItemGroup>
</Project>
```

```csharp
using System.Collections.Generic;
using System.Threading.Tasks;
using CamusDB.Extensions;

public static class TextFunctions
{
    [CamusFunction("slugify")]
    public static string Slugify(string value) => value.Trim().ToLowerInvariant().Replace(' ', '-');
}

public sealed class AuditTrigger : ICamusTrigger
{
    public Task FireAsync(ITriggerContext context) =>
        context.Sql.ExecuteAsync(
            "INSERT INTO audit_log (op, tbl) VALUES (@op, @tbl)",
            new Dictionary<string, object?> { ["op"] = context.Operation.ToString(), ["tbl"] = context.TableName });
}
```

Deploy the built DLL on every node of the server and list it in the configuration:

```yaml
extension_assemblies:
  - /opt/camusdb/ext/MyCompany.Ext.dll
```

```sql
SELECT slugify(title) FROM posts;
CREATE TRIGGER audit ON orders AFTER INSERT EXTERNAL NAME 'MyCompany.Ext.AuditTrigger';
```

**Versions.** This package is versioned with the CamusDB server: reference the version of the servers
you deploy to. A server refuses an extension compiled against a newer contract than the one it
carries, and loads one compiled against the same or an older one. While CamusDB is 0.x, "newer" means
a newer major.minor (`0.17.*` is newer than a 0.16 server); from 1.0 on, a newer major.

**Trust.** An extension runs inside the server process with the server's rights. The server's SAFE
check is a guardrail for reviewed code, not a sandbox.

The full guide (value mapping, NULL rules, trigger and procedure semantics, the SAFE check, limits
and error codes) is
[docs/csharp-extensions.md](https://github.com/camusdb/camusdb/blob/main/docs/csharp-extensions.md).
