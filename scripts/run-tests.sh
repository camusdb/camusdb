cd CamusDB.Tests
dotnet build
mkdir -p bin/Debug/net10.0/Data/
dotnet test --no-build -v normal --blame-hang --blame-hang-timeout 60s --blame-crash -- NUnit.MaxParallelWorkers=8
