Update artifact version in all pom files using maven command line.
mvn versions:set -DnewVersion=<vesion number>

Manually modify the version properties.

Remove the *.versionBackup file for each pom.xml
find . -name *.versionsBackup|xargs rm
