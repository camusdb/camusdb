# Config

Reads the server configuration file and exposes it as a typed `ConfigDefinition` object.

`ConfigReader` parses YAML using YamlDotNet (underscore naming convention). `ConfigResolver`
merges CLI overrides and applies the resolved config to `CamusDBConfig` at startup in
`Program.cs` (precedence: CLI > env > YAML > default).

See `docs/configuration.md` for the full reference and CLI ↔ YAML mapping.
