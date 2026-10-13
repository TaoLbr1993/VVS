# README

###数据集

数据位置，数据位置已经移动到了`\home\dataset\hybrid`下，一共有四个数据集`WIT`，`deep`，`YouTube-aduio`和`YouTube-rgb`

### 代码

排版：四个文件夹分别负责处理四个数据集`LION`(已弃用)，`WIT`，`deep`，`YouTube`(包含`YouTube-aduio`和`YouTube-rgb`)

`process.py`: 处理原始数据，生成`.bin`文件——存放向量数据，生成`meta.txt`——存放结构化数据

`split.py`: 将原始向量数据分成多个`.fivecs`文件，与相应的多个`meta_i.txt`文件

`merge.py`: 将多个`.fivecs`文件与相应的`meta_i.txt`文件合并为一个大的`.fivecs`文件与相应的`meta_j.txt`

`generate_query.py`: 生成`query.txt`文件，包含100个query，示例可见`\home\dataset\hybrid\YouTube-audio\query.txt`

`groundtruth.py`: 生成对应的groundtruth文件


### rgb
rgb的整体逻辑是先把原始数据划分为四个.bin文件，实际用的时候只用了第四个文件，然后将这个文件里的数据进行划分，划分到不同的silo里（用了一些no-IID的方法），然后应该是四个大文件都分别进行一次，然后划分到每个silo中，之后再合并到一起的，但是如果是只用了第四个大文件的话合并其实没有必要。生成查询向量的时候是每个silo取五个向量，然后label是随机的label得到的

### audio
meta的数据量是93.6w，等与rgb比对，看看是全局的数据还是最后250个的数据
应该是用一个大的.bin文件然后从里面随机出query向量，然后再根据当前的silo数目，利用对应silo的数据来计算正确值。感觉还是蛮合理的，相当于是在有5个silo的情况下看看其正确率，再看看其他数量的，这符合实际情况，不要被全局的正确率给误导了。当然假设query向量是全随机的，所以以后调整的话就调整silo数目就行了