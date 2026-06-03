# Config

Reads the server configuration file and exposes it as a typed `ConfigDefinition` object.

`ConfigReader` parses YAML using YamlDotNet (underscore naming convention). `CamusStartup` applies the resulting `ConfigDefinition` to `CamusDBConfig` globals at startup — overriding defaults such as `DataDirectory` and `BufferPoolSize`.

`ConfigDefinition` fields:

| Field | Default | Description |
|-------|---------|-------------|
| `DataDir` | `"Data"` | Directory where database files are stored |
| `BufferPoolSize` | `-1` (auto) | Maximum pages per KV bucket; auto-scales to CPU count when negative |
