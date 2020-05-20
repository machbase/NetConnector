# .NET Connector for MACHBASE
It is based on [MySQL .NET Connector](https://github.com/mysql/mysql-connector-net). It is under GNU GPLv2 license.

* It's solution is for Visual Studio 2015, but you can convert those source codes for older Visual Studio.
* .NET Core project looks empty, but you can build by linking (or hard-copying) from .NET Framework project's source codes.

# Supported version
* \>= .NET Framework 4.0
* \>= .NET Core 2.0

## If your Machbase Server use Machbase 5.7.x...
Check your Communication Module Version with `machCheckEnv -i`
and Change Version.cs in `NetConnector/MachConnector/Core/`
