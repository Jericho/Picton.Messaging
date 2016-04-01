rmdir /s /q "TestResults"
rmdir /s /q "CodeCoverageData"
rmdir /s /q "CodeCoverageReport"

mkdir "CodeCoverageData"
mkdir "CodeCoverageReport"


packages\OpenCover.4.6.519\tools\OpenCover.Console.exe^
 -register:user^
 -target:"C:\Program Files (x86)\Microsoft Visual Studio 12.0\Common7\IDE\CommonExtensions\Microsoft\TestWindow\vstest.console.exe"^
 -targetargs:"""Picton.Azure.UnitTests\bin\Debug\Picton.Azure.UnitTests.dll"""^
 -filter:"+[Picton.Azure]* -[Picton.Azure]Picton.Azure.Properties.*"^
 -excludebyattribute:*.ExcludeFromCodeCoverage*^
 -hideskipped:All^
 -output:.\CodecoverageData\Picton.Azure_coverage.xml


packages\ReportGenerator.2.4.4.0\tools\ReportGenerator.exe^
 -reports:.\CodeCoverageData\*.xml^
 -targetdir:.\CodeCoverageReport^
 -reporttypes:Html^
 -filters:-Picton.Azure.UnitTests*


start CodeCoverageReport\index.htm