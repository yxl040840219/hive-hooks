Contains hive custom hooks

build: mvn clean install

1) QueueHandlerHiveDriverRunHook : 用来判断hive用户提交Job 运行的队列

2) HiveServer2Auth: 用来判断连接hiveServer2 的用户名密码

3) GrantHook: 用来判断执行授权的用户

Steps to configure:
-------------------

a) 配置 hive-site.xml

```
   <property>
    <name>hive.exec.driver.run.hooks</name>
    <value>com.customer.queue.QueueHandlerHiveDriverRunHook</value>
  </property>
```

```
<property>  
  <name>hive.server2.authentication</name>  
  <value>CUSTOM</value>  
</property>  
  
<property>  
  <name>hive.server2.custom.authentication.class</name>  
  <value>com.customer.auth.HiveServer2Auth</value>  
</property> 

<property>  
  <name>hive.server2.custom.authentication.file</name>  
  <value>local file path</value>  
</property> 

 
```

```
<property>  
  <name>hive.grant.admin.username</name>  
  <value>hadoop</value>  
</property> 

<property> 
    <name>hive.semantic.analyzer.hook</name> 
    <value>com.customer.grant.GrantHook</value>  
</property>

```

b) 拷贝 jar hive-custom-*jar 到 hive lib

c) 重启 hiveserver2.


