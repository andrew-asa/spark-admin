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

+ [参考笔记](https://github.com/andrew-asa/blog/tree/master/技术相关/spark)
+ [开发时间表](./src/main/resources/note/DevTimeTable.md)
+ [TODO](./src/main/resources/note/Todo.md)
