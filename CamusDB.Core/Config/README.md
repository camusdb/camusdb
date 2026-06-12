# Config

Reads the server configuration file and exposes it as a typed `ConfigDefinition` object.

`ConfigReader` parses YAML using YamlDotNet (underscore naming convention). `CamusStartup` applies the resulting `ConfigDefinition` to `CamusDBConfig` globals at startup — overriding defaults such as `DataDirectory`.

`ConfigDefinition` fields:

| Field | Default | Description |
|-------|---------|-------------|
| `DataDir` | `"Data"` | Directory where database files are stored |
