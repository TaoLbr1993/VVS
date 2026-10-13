# FedVS: Towards Federated Vector Similarity Search with Filters

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

**This repository aims to provide an prototype system for secure and efficient federated vector similarity search with filters**

# Environment

OS: Ubuntu 20.04 LTS  
GCC/G++: >= 8.4.0  
CMake: >= 3.19.1  
Docker: >=19.03  
Docker Compose: >=1.29.2   
[gRPC](https://grpc.io/): >= 1.66.0  
[Milvus](https://milvus.io/): >= 2.5.2  
[Boost C++ library](https://www.boost.org/): >= 1.85.0  
RAM: >= 8GB

# Third Party Requirement and Installment

## Milvus

**Milvus** is a blazing-fast open-source vector database designed to power AI-driven applications at scale and has been deployed into various industrial scenario. Built for vector similarity search on unstructured data, it enables machines to understand and retrieve contextually relevant information through neural network embeddings.

Milvus is composed of a server which is integrated into a docker container, thus Milvus server should be firstly deployed.

* **Download Milvus Docker Compose file**

```bash
wget https://github.com/milvus-io/milvus/releases/download/v2.3.10/milvus-standalone-docker-compose.yml -O docker-compose.yml
```

* **Start Milvus Services**

```bash
docker-compose up -d
```

* **Verify Installation**

```bash
docker-compose ps
```

Milvus offers comprehensive multi-language SDKs to facilitate vector operations. In our FedVS implementation, we leverage the [C++ SDK for Milvus](https://github.com/milvus-io/milvus-sdk-cpp) (hosted under the algorithms module). This SDK package features a well-structured design with:

- Clean implementation of Milvus core functionalities
- Intuitive CMake configurations
- Cross-platform compatibility

The modular architecture enables developers to seamlessly integrate vector search capabilities through straightforward API calls. For instance, the C++ binding abstracts complex operations while maintaining native performance, simplifying API interactions like collection management and vector similarity search.

```
FedVSE/
│
├── algorithms/
    ├── cpp
        ├── milvus
        └── cmake
        
```

## gRPC

gRPC is a modern, open source, high-performance remote procedure call (RPC) framework that can run anywhere. gRPC enables client and server applications to communicate transparently, and simplifies the building of connected systems.

Before compiling this project, you need to install the [gRPC](https://github.com/grpc/grpc) first by following the [guideline](https://grpc.io/docs/languages/cpp/quickstart/) as follows.

* **Setup**

Choose a directory to hold locally installed packages. This page assumes that the environment variable `MY_INSTALL_DIR` holds this directory path. For example:

```bash
export MY_INSTALL_DIR=$HOME/.local
mkdir -p $MY_INSTALL_DIR
```

* **Install cmake**

You need version 3.13 or later of cmake. Install it by following these instructions:

```bash
sudo apt install -y cmake
```

* **Install other required tools**

Install the basic tools required to build gRPC:

```bash
sudo apt install -y build-essential autoconf libtool pkg-config
```

* **Clone the grpc repo**

Clone the **grpc** repo and its submodules:

```bash
git clone --recurse-submodules -b v1.66.0 --depth 1 --shallow-submodules https://github.com/grpc/grpc
```

* **Build and install gRPC and Protocol Buffers**

The following commands build and locally install **gRPC** and **Protocol Buffers**:

```bash
cd grpc
mkdir -p cmake/build
pushd cmake/build
cmake -DgRPC_INSTALL=ON \
      -DgRPC_BUILD_TESTS=OFF \
      -DCMAKE_INSTALL_PREFIX=$MY_INSTALL_DIR \
      ../..
make -j 4
make install
popd
```

## Boost C++ Libraries

Boost provides peer-reviewed and widely useful C++ libraries that work well with the Standard Library. Before compiling this project, you also need to install the [Boost](https://www.boost.org/) first by following the [guideline](https://www.boost.org/doc/libs/1_85_0/more/getting_started/unix-variants.html) as follows.

* **Download the Boost**

In Ubuntu, you can use the following commands to download Boost 1.85.0

```bash
wget https://archives.boost.io/release/1.85.0/source/boost_1_85_0.tar.gz
tar -xzvf /boost_1_85_0.tar.gz
```

* **Setup**

Choose a directory to hold locally installed packages. This page assumes that the environment variable `MY_INSTALL_DIR` holds this directory path. For example:

```bash
export MY_INSTALL_DIR=$HOME/.local
mkdir -p $MY_INSTALL_DIR
```

* **Build and install Boost**

The following commands build and locally install **Boost**:

```bash
cd boost_1_85_0
./bootstrap.sh --prefix=$MY_INSTALL_DIR
./b2 install
```

## Intel SGX

You also need to install the SDK and PSW for Intel SGX. The installation follows the guideline [Intel_SGX_SW_Installation_Guide_for_Linux.pdf](https://download.01.org/intel-sgx/latest/linux-latest/docs).

### I. Install Dependency

By executing the following commands, you can instal the dependencies first:

```
sudo apt-get install build-essential ocaml automake autoconf libtool wget python3 python-is-python3 libssl-dev dkms
sudo update-alternatives --install /usr/bin/python python /usr/bin/python3 1
```

### II. Install Intel SGX SDK

By executing the following commands, you can install Intel SGX SDK:

```
echo 'deb [arch=amd64] https://download.01.org/intel-sgx/sgx_repo/ubuntu focal main' | sudo tee /etc/apt/sources.list.d/intel-sgx.list
wget -qO - https://download.01.org/intel-sgx/sgx_repo/ubuntu/intel-sgx-deb.key | sudo apt-key add
sudo apt-get update
sudo apt-get install libsgx-epid libsgx-quote-ex libsgx-dcap-ql
sudo apt-get install libsgx-urts libsgx-launch libsgx-enclave-common
sudo apt-get install libsgx-urts-dbgsym libsgx-enclave-common-dbgsym libsgx-dcap-ql-dbgsym libsgx-dcap-default-qpl-dbgsym
```

### III. Install Intel SGX PSW

By executing the following commands, you can install Intel SGX PSW:

```
wget https://download.01.org/intel-sgx/latest/linux-latest/distro/ubuntu20.04-server/sgx_linux_x64_sdk_2.25.100.3.bin
chmod +x sgx_linux_x64_sdk_2.25.100.3.bin
./sgx_linux_x64_sdk_2.25.100.3.bin 
sudo apt-get install libsgx-enclave-common-dev libsgx-dcap-ql-dev libsgx-dcap-default-qpl-dev tee-appraisal-tool
```

In the third step, select ``NO`` and then input ``/opt/intel``.

### IV. Compile the source with Intel SGX (**Simulation Mode**)

By replacing the line of ``cmake ......`` with the following line in ``scripts/build.sh``, you can enable the SGX-based PSI protocol:

```
cmake -DUSE_SGX=ON ..
source /opt/intel/sgxsdk/environment
```

Now, you can re-compile and run the program.

### V. Compile the source with Intel SGX (**Hardware Mode**)

By replacing the line of ``cmake ..`` with the following line in ``scripts/build.sh``, you can enable the SGX-based PSI protocol:

```
cmake -DUSE_SGX=ON ..
```

Now, you can re-compile and run the program. Notice that, you need to ensure that your server supports the hardware of Intel SGX. Otherwise, you can only use the **simulation mode**.

## Compile and run our algorithms

### Compile the algorithms

Execute the following commands to compile **data provider(Silo.cpp)** ,**central server(Broker.cpp)** and **query user(User.cpp)**:

```bash
cd scripts
chmod +x *.sh
./build.sh
```

### Enable the data silo of the algorithms

Execute the following command in one terminal to enable a **data provider**:

```bash
./runSilo.sh
```

The detailed commands in the `runSilo.sh` is as follows:

```bash
#!/bin/bash

ORIGINAL_DIR=$(pwd)
cd ../cpp/build
silo_id=0
number=$((0*5 + ${silo_id}))
data_path="/home/dataset/hybrid/YouTube-audio/YouTube_${number}.fivecs"
scalardata_path="/home/dataset/hybrid/YouTube-audio/meta_${number}.txt"
cluster_path="/home/dataset/hybrid/YouTube-audio/YouTube_${number}.cluster/cluster"
collection_name="LOCAL_DATA_${number}"
milvus_port="50055"
cluster_option="OFF"
milvus_option="OFF"
alpha=0.05
cluster_num=10
ipaddr=localhost:50050
 
./silo --ip=$ipaddr --id=$number --data-path=$data_path --scalardata-path=$scalardata_path --cluster-path=$cluster_path --index-type=HNSW --collection-name=$collection_name --milvus-port=$milvus_port --cluster-option=$cluster_option --milvus-option=$milvus_option --alpha=$alpha --cluster-num=$cluster_num

cd "$ORIGINAL_DIR" 
```
> * ip: data provider's ip address
> * id: data provider's ID
> * data-path: file path of vector data
> * scalardata-path: file path of attributes
> * cluster-path: file path that stores clusters
> * index-type: vector index's type
> * collection-name: target collection name in Milvus
> * milvus-port: running port of Milvus server
> * cluster-option: whether to build cluster
> * milvus-option: whether to insert vectors into Milvus
> * alpha: hyper parameter $\alpha$
> * cluster-num: hyper parameter number of clusters

### Enable the central server of the algorithms

Central server side must be run on an server **equipped with Intel SGX**. Similar to the script for data silos, the query user can be enabled with the following command:

```
./runBroker.sh
```

The detailed commands in the `runBroker.sh` is as follows:

```bash
#!/bin/bash

ORIGINAL_DIR=$(pwd)
cd ../cpp/build

if [ -f /opt/intel/sgxsdk/environment ]; then
    source /opt/intel/sgxsdk/environment
else
    echo "Error: /opt/intel/sgxsdk/environment does not exist."
    exit 1
fi

ipaddr="localhost:50056"
ip_path="../../scripts/ip.txt"

./broker --broker-ip=$ipaddr --silo-ip=$ip_path

if [ $? -ne 0 ]; then  
    echo "Data broker FAIL"  
    exit 1  
fi 

cd "$ORIGINAL_DIR"
```

> * broker-ip: ip address of central server
> * silo-ip: ip address of each data provider

### Enable the query user of the algorithms

The query user can be launched with the following command:

```bash
./run.sh
```

The detailed commands of query user is as follows:

```bash
#!/bin/bash

ORIGINAL_DIR=$(pwd)
cd ../cpp/build
broker_ip="localhost:50056"
ip_path="../../scripts/ip.txt"
query_k=128
query_path="/home/dataset/hybrid/YouTube-audio/query.txt"
truth_path="/home/dataset/hybrid/YouTube-audio/gt_128_5.ivecs"
output_path="output.txt"

./user  --broker-ip=$broker_ip --silo-ip=$ip_path --query-k=$query_k --query-path=$query_path --output-path=$output_path --truth-path=$truth_path
if [ $? -ne 0 ]; then  
    echo "Query user FAIL"  
    exit 1  
fi 

cd "$ORIGINAL_DIR"
```

> * broker-ip: ip address of central server
> * silo-ip: ip address of each data provider
> * query-k: query parameter $k$
> * query-path: file path of query vectors and attribute filter
> * output-path: file path that stores output
> * truth-path: file path that stores ground truth of queries

# License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.