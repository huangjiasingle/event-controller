## event-controller

主要是同步kubernetes所有的事件到指定的etcd数据库,其主要原因是因为kubernetes的事件默认1小时自动删除.持久化event可以做很多的分析.