## 这个项目是使用maven命令构建的：
mvn archetype:generate -B \
-DarchetypeGroupId=org.apache.flink \
-DarchetypeArtifactId=flink-quickstart-java \
-DarchetypeVersion=1.10.2 \
-DgroupId=com.dinglicom \
-DartifactId=flink-train-java \
-Dversion=1.0-SNAPSHOT \
-Dpackage=com.dinglicom
#### 构建成功之后，在resources目录下默认会生成一个log4j.properties的日志配置文件
