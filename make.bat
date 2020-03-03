set HADOOP_HOME=C:\BigData\hadoop-2.9.1
javac -cp %HADOOP_HOME%\share\hadoop\common\hadoop-common-2.9.1.jar;%HADOOP_HOME%\share\hadoop\mapreduce\hadoop-mapreduce-client-core-2.9.1.jar;%HADOOP_HOME%\share\hadoop\common\lib\commons-cli-1.2.jar WordCount.java
jar cf wc.jar WordCount*.class
pause