给定一组（key-as-string, value-as-integer）对，本章给出5个解决方案

* MapReduce的Top 10解决方案：假定所有输入键都是唯一的，即给定集合{(K, V)}，所有K是
唯一的。

* Spark Top 10解决方案：假定Key唯一，不使用Spark的排序函数例如top()或takeOrdered()

* Spark Top 10解决方案：假定Key不唯一，不使用Spark的排序函数例如top()或takeOrdered()

* Spark Top 10解决方案：假定Key不唯一，使用Spark强大的排序函数takeOrdered()

* MapReduce Top 10解决方案：假定Key不唯一