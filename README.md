[toc]

# bools-0.3.3

常用功能集合，助力更高效的编写代码<br>

## 安装

```shell
pip3 install -U bools
```



## 文档

### datetime「时间日期处理」

#### Datetime

> 默认带有时区信息（Asia/Shanghai），支持和timedelta等互相操作。保留原生datetime.datetime所有方法和属性<br>
>
> 有一系列比原生datetime更简洁、高效的与时间字符串交互方式

##### fromtimestamp

*特性：支持任意位数的时间戳*

```python
>>> from bools.datetime import Datetime
>>> Datetime.fromtimestamp(1660000000000)
Datetime(2022, 8, 9, 7, 6, 40, tzinfo=tzfile('/usr/share/zoneinfo/Asia/Shanghai'))
```

##### from_str

*特性：无须指定format，且性能高于strptime（约2倍）*

```python
>>> Datetime.from_str('2021-1-1 12:32:24')
Datetime(2021, 1, 1, 12, 32, 24, tzinfo=tzfile('/usr/share/zoneinfo/Asia/Shanghai'))
>>> Datetime.from_str('2021-1-1T12:32')
Datetime(2021, 1, 1, 12, 32, tzinfo=tzfile('/usr/share/zoneinfo/Asia/Shanghai'))
```

##### to_str、str

*特性：便捷的输出时间字符串的任意部分*

```python
>>> Datetime.now().str
'2021-06-18 16:20:23'
>>> Datetime.now().to_str(3,6)
'16:20:30'
```

##### from_datetime

*将原生datetime转化为Datetime对象*

```python
>>> Datetime.from_datetime(datetime.now())
Datetime(2021, 6, 18, 16, 23, 11, 569620, tzinfo=tzfile('/usr/share/zoneinfo/Asia/Shanghai'))
```

#### Timedelta

> 同datetime.timedelta，均可与Datetime互操作

```python
>>> Datetime.now()+Timedelta(days=1)
Datetime(2021, 6, 19, 16, 25, 33, 188782, tzinfo=tzfile('/usr/share/zoneinfo/Asia/Shanghai'))
>>> Datetime.now()+timedelta(hours=1)
Datetime(2021, 6, 18, 17, 25, 43, 40131, tzinfo=tzfile('/usr/share/zoneinfo/Asia/Shanghai'))
```

#### set_default_tz

> 通过tz_id修改Datetime默认时区

```python
>>> from bools.datetime import set_default_tz
>>> set_default_tz('Asia/Shanghai')
```

#### set_default_format

> 设置Datetime.to_str()输出的时间字符串格式，格式同strptime format

```python
>>> from bools.datetime import set_default_format
>>> set_default_format('%Y-%m-%d %H:%M:%S')
```

### log「日志输出」

#### Logger

> 配置简单，输出带颜色区分的日志（通过ascii esc实现）<br>
>
> debug：灰色，info：绿色，warning：黄色，error：红色

```python
>>> from bools.log import Logger
>>> # Logger.init("DEBUG") 可通过init调整输出日志级别，默认输出>INFO
>>> Logger.info('hello world')
[2021-06-18 16:34:36,561][INFO] : hello world
>>> Logger.error('hello world')
[2021-06-18 16:35:02,127][ERROR] : hello world
```

<img src="http://lbj.wiki/static/images/4450203c-d010-11eb-9928-00163e30ead3.png" alt="image-20210618163623997" style="zoom:50%;" />

### functools「工具函数」

#### parallel

*多进程处理函数。特性：**支持传递lambda函数和无参函数***

```python
>>> from bools.functools import parallel
>>> parallel(lambda x:x**2)(range(4))
[0, 1, 4, 9]
>>> parallel(lambda :2, count=2)(range(4))
[2, 2, 2, 2]
```

#### catch

*异常处理装饰器，可传入闭包控制异常后执行函数或在异常后返回值*

```python
>>> from bools.functools import catch
>>> result = catch(except_return=1, log='计算报错')(lambda :1/0)()
[2021-06-18 16:39:33,449][ERROR] : 计算报错
Traceback (most recent call last):
  File "/Users/bowaer/PycharmProjects/bools/bools/functools/functools.py", line 10, in wrapper
    return func(*args, **kwargs)
  File "<stdin>", line 1, in <lambda>
ZeroDivisionError: division by zero
>>> result
1
```

```python
>>> @catch(except_return=2)
... def func(n):
...  return 1/n
... 
>>> func(2)
0.5
>>> func(0)
[2021-06-18 16:41:31,912][ERROR] : 
Traceback (most recent call last):
  File "/Users/bowaer/PycharmProjects/bools/bools/functools/functools.py", line 10, in wrapper
    return func(*args, **kwargs)
  File "<stdin>", line 3, in func
ZeroDivisionError: division by zero
2
```

#### timeit

*函数时间统计装饰器*

```python
>>> from bools.functools import timeit
>>> costs = timeit(count=10, return_costs=True)(time.sleep)(0.1)
[2021-06-18 17:29:37,064][INFO] : 平均执行时间: 0.103s
>>> costs
[0.10266709327697754, 0.105194091796875, 0.10502386093139648, 0.10092806816101074, 0.10246896743774414, 0.10508394241333008, 0.10145998001098633, 0.10406613349914551, 0.10434389114379883, 0.10046100616455078]
```

```python
>>> from bools.functools import timeit
>>> @timeit
... def test():
...  for i in range(1000000):
...   'hello'+'world'
... 
>>> test()
[2021-06-18 18:03:16,937][INFO] : 平均执行时间: 0.019s
```



## 版本历史

### 0.3.3

> 增加并行处理函数parallel，支持传递lambda函数和无参函数

### 0.3.2

> 装饰器函数支持兼容无参数使用，即@timeit == @timeit()
>
> catch增加exception参数支持自定义异常捕获类型

### 0.3.1

> functools增加timeit统计函数执行时间

### 0.3.0

> 增加functools模块及catch异常处理

### 0.2.2

> 修复Datetime和timedelta互操作返回原生datetime对象的问题

### 0.2.0

> 增加log模块

### 0.1.0

> 增加datetime模块