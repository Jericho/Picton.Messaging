rmdir /s /q "TestResults"
rmdir /s /q "CodeCoverageData"
rmdir /s /q "CodeCoverageReport"

mkdir "CodeCoverageData"
mkdir "CodeCoverageReport"


packages\OpenCover.4.6.519\tools\OpenCover.Console.exe^
 -returntargetcode^
 -register:user^
 -target:"C:\Program Files (x86)\Microsoft Visual Studio 14.0\Common7\IDE\CommonExtensions\Microsoft\TestWindow\vstest.console.exe"^
 -targetargs:"""Picton.UnitTests\bin\Debug\Picton.UnitTests.dll"""^
 -filter:"+[Picton]* -[Picton]Picton.Properties.*"^
 -excludebyattribute:*.ExcludeFromCodeCoverage*^
 -hideskipped:All^
 -output:.\CodecoverageData\Picton_coverage.xml


packages\ReportGenerator.2.4.5.0\tools\ReportGenerator.exe^
 -reports:.\CodeCoverageData\*.xml^
 -targetdir:.\CodeCoverageReport^
 -reporttypes:Html^
 -filters:-Picton.UnitTests*


start CodeCoverageReport\index.htm