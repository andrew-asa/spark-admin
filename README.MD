# spark 学习
# 依赖 & 编译
+ 依赖基础类库 com.asa.third,com.asa.base
   + mavan 仓库地址

   ```
   <repositories>
           <repository>
               <id>andrew.asa-maven-repository</id>
               <url>https://raw.githubusercontent.com/andrew-asa/maven-repository/master/repository</url>
           </repository>
   </repositories>
   ```
   + mavn依赖

   ```
   <dependency>
        <groupId>com.asa</groupId>
            <artifactId>com.asa.third</artifactId>
            <version>1.0-SNAPSHOT</version>
        </dependency>
        <dependency>
            <groupId>com.asa</groupId>
            <artifactId>com.asa.base</artifactId>
        <version>1.0-SNAPSHOT</version>
   </dependency>
   ```
+ 最好的学习就是看单元测试
+ [参考笔记](https://github.com/andrew-asa/blog/tree/master/技术相关/spark)
+ [开发时间表](./src/main/resources/note/DevTimeTable.md)
+ [TODO](./src/main/resources/note/Todo.md)
+ 涉及到改动代码的pr记得在.git/hooks里面添加运行所有单元测试，所有单元测试通过才能提交
```
#! /bin/bash

# save the file as <git_directory>/.git/hooks/pre-commit

echo "Running Maven clean test for errors"
# retrieving current working directory
CWD=`pwd`
MAIN_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# go to main project dir
cd $MAIN_DIR/../../
# running maven clean test
/usr/local/bin/mvn clean test
if [ $? -ne 0 ]; then
  "Error while testing the code"
  # go back to current working dir
  cd $CWD
  exit 1
fi
# go back to current working dir
cd $CWD
```
