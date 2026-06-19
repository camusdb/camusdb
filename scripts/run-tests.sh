
cd CamusDB.Tests
dotnet build
mkdir -p bin/Debug/net10.0/Data/
dotnet test -v normal --blame-hang --blame-hang-timeout 60s --blame-crash
