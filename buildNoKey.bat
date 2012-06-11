call "%VS100COMNTOOLS%vsvars32.bat"

md build
del build/*.* /Q
copy .\Lib\NServiceBus\.net 4.0\binaries\NServiceBus.* .\Build
rem copy .\Lib\RabbitMQ\*.* .\Build

msbuild .\src\NServiceBusRmq.sln /p:Configuration=Release

rem copy .\src\NServiceBus.Unicast.Transport.RabbitMQ\bin\Release\NServiceBus.Unicast.Transport.RabbitMQ.* .\Build

call mergeAssemblies.bat

pause