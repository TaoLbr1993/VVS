#include <grpcpp/grpcpp.h>
#include <grpc/grpc.h>
#include <vector>
#include <string>
#include <mutex>
#include <thread>
#include <cmath>
#include <boost/program_options.hpp>
#include <boost/asio.hpp>  
#include <boost/asio/thread_pool.hpp>
#include <signal.h>
namespace bpo = boost::program_options;

#include "UserBroker.grpc.pb.h"
#include "BrokerSilo.grpc.pb.h"
#include "SiloFilter.grpc.pb.h"
#include "SiloFilter.pb.h"
#include "utils/DataType.hpp"
#include "utils/MetricType.hpp"
#include "utils/BenchLogger.hpp"
#include "utils/File_IO.h"
#include "middleware/BaseSiloConnector.hpp"
#include "middleware/MilvusSiloConnector.hpp"
#include "global.hpp"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using google::protobuf::Empty;
// 这里面是定义了一些远程调用的接口
using userbroker::UserBroker;
using userbroker::knnQuery;
using userbroker::rpcComm;
// 这里面是定义了一些远程调用的接口
using brokersilo::BrokerSilo;
using brokersilo::Vector;
using silofilter::SiloFilter;

class FilterConnector {
    private:
        std::unique_ptr<silofilter::SiloFilter::Stub> stub_;
    public:
        FilterConnector(const std::shared_ptr<Channel> &channel):
            stub_(silofilter::SiloFilter::NewStub(channel)) {
        }

        double getComm() {
            ClientContext context;
            Empty request;
            silofilter::rpcComm response;
            Status status = stub_->requestComm(&context, request, &response);
            if (!status.ok()) {
                std::cerr << "RPC failed: " << status.error_message() << std::endl;
                std::cout << "Failed to get Comm from filter." << std::endl;
                std::exit(EXIT_FAILURE);
            }
            return 1.0 * response.rpcdatanum();
        }
};

// 这个和broker的是两个不同的类，定义了不同的需要的函数
class SiloConnector {
    private:
        std::unique_ptr<BrokerSilo::Stub> stub_;
        size_t siloId; //silo id
        std::string ipAddr; //silo ip address
        BenchLogger m_logger;
        std::vector<brokersilo::Vector> finalAns;
    
    public:
        SiloConnector(const std::shared_ptr<Channel> &channel, const size_t &id, const std::string &addr):
            stub_(BrokerSilo::NewStub(channel)), siloId(id), ipAddr(addr)  {
            m_logger.Init();
        }

        void requestAns() {
            ClientContext context;
            Empty request;
            finalAns.clear();
            std::unique_ptr<ClientReader<brokersilo::Vector> > reader(
                stub_->getAns(&context, request));
            double grpc_comm = 0;
            brokersilo::Vector nowp;
            while(reader->Read(&nowp)) {
                finalAns.emplace_back(nowp);
                grpc_comm += nowp.ByteSizeLong();
            }
            m_logger.LogAddComm(request.ByteSizeLong());
            if(recordAnswerComm) m_logger.LogAddComm(grpc_comm);
            return;
        }

        float getComm() {
            double comm = m_logger.GetQueryComm();
            m_logger.Init();
            return comm;
        }

        std::vector<VidType> getAnsId() {
            std::vector<VidType> idRes;
            for(auto &item : finalAns) {
                idRes.emplace_back(item.id());
            }
            return idRes;
        }
};

class BrokerConnector {
    private:
        std::unique_ptr<UserBroker::Stub> stub_;
        size_t siloNum;
        size_t filterNum;
        std::vector<std::shared_ptr<SiloConnector>> siloPtr; 
        std::vector<std::shared_ptr<FilterConnector>> filterPtr;
        std::vector<std::string> ipAddrSet; //SiloConnector address
        std::vector<std::string> filterIpAddrSet;
        std::string ipAddr; //broker address
        BenchLogger m_logger;
        BenchLogger m_testsLogger;
        std::unordered_set<int> one_num_seq_deep {0,5,10,16,22,26,27,28,29,30,33,35,37,39,40,43,45,46,47,50,51,56,58,59,60,61,67,68,69,72,75,76,77,78,79,80,82,87,88,92,94,95,98,99,7,13,52,31,74,1};
        std::unordered_set<int> one_num_seq_ytA{24,25,46,55,57,58,62,63,65,66,71,0,2,3,4,6,11,12,15,16,17,19,20,22,27,31,32,34,36,38,39,40,44,45,47,48,50,53,54,60,64,67,69,72,73,74,77,80,81,82,86,89,90,92,94,95,97,99};
        std::unordered_set<int> one_num_seq_ytR{0,2,6,7,8,9,10,14,15,16,19,20,21,23,25,28,30,32,33,35,36,37,38,39,41,42,43,44,46,47,45,48,50,51,52,55,56,57,58,59,60,61,62,63,64,65,66,67,69,70,71,73,74,75,79,82,83,84,85,86,87,88,90,93,94,95,96,97,98,99};
        std::unordered_set<int> one_num_seq_wit{5};
        std::unordered_set<int> one_num_seq;
    public:
        BrokerConnector(const std::shared_ptr<Channel> &channel, const std::string &ipAddr, const std::string &ipAddrFile, const std::string &filterIpAddrFile):
            stub_(UserBroker::NewStub(channel)), ipAddr(ipAddr) {
            one_num_seq = std::move(one_num_seq_ytR);
            std::cout<<"the size of one num seq is "<< one_num_seq.size() <<std::endl;
            ReadSiloIPaddr(ipAddrFile, ipAddrSet);
            ReadSiloIPaddr(filterIpAddrFile, filterIpAddrSet);
            if (ipAddrSet.empty()) {
                throw std::invalid_argument("No data silo's IP addresses");  
                std::exit(EXIT_FAILURE);
            }
            if (filterIpAddrSet.empty()) {
                throw std::invalid_argument("No data filter's IP addresses");  
                std::exit(EXIT_FAILURE);
            }
            siloNum = ipAddrSet.size();
            filterNum = filterIpAddrSet.size();
            siloPtr.resize(siloNum);
            filterPtr.resize(filterNum);
            grpc::ChannelArguments args;  
            args.SetInt(GRPC_ARG_MAX_SEND_MESSAGE_LENGTH, INT_MAX);  
            args.SetInt(GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH, INT_MAX); //set maximum comm cost
            for(size_t i = 0;i < siloNum;i++) { //connect to each silo
                std::string url = ipAddrSet[i];
                std::cout << "User is connecting Silo #(" << std::to_string(i) << ") on IP address " << url << std::endl;
                siloPtr[i] = std::make_shared<SiloConnector>(grpc::CreateCustomChannel(url.c_str(),
                                grpc::InsecureChannelCredentials(), args), i, url);
            }
            for (size_t i = 0;i < filterNum;i++) {
                std::string url = filterIpAddrSet[i];
                std::cout << "User is connecting Filter #(" << std::to_string(i) << ") on IP address " << url << std::endl;
                filterPtr[i] = std::make_unique<FilterConnector>(grpc::CreateCustomChannel(url,
                                grpc::InsecureChannelCredentials(), grpc::ChannelArguments()));
            }
            m_logger.Init();
            m_testsLogger.Init();
        }

        void sendKnnQuery(VectorDataType queryV, size_t queryK, std::string condition) {
            ClientContext context;
            knnQuery request;
            Empty response;
            int dim = queryV.data.size();
            for(int i = 0;i < dim;i++) {
                request.add_data(queryV[i]);
            }
            request.set_k(queryK);
            request.set_condition(condition);
            request.set_qid(queryV.vid);
            Status status = stub_->requestKNN(&context, request, &response);

            if (!status.ok()) {
                std::cerr << "RPC failed: " << status.error_message() << std::endl;
                std::cout << "Failed to get Diffie-Hellman random value A from server." << std::endl;
                std::exit(EXIT_FAILURE);
            }

            return;
        }

        double getComm() {
            ClientContext context;
            Empty request;
            rpcComm response;
            Status status = stub_->requestComm(&context, request, &response);
            if (!status.ok()) {
                std::cerr << "RPC failed: " << status.error_message() << std::endl;
                std::cout << "Failed to get Comm from broker." << std::endl;
                std::exit(EXIT_FAILURE);
            }
            // 这里应该是没有算上当前这个查询的comm
            return 1.0 * response.rpcdatanum();
        }

        static void getVector(BrokerConnector *impl, const size_t siloId) {
            impl->siloPtr[siloId]->requestAns();
        }

        void parallelGetVector() {
            boost::asio::thread_pool pool(std::thread::hardware_concurrency());
            for (size_t i = 0;i < siloNum; i++) {
                boost::asio::post(pool, std::bind(getVector, this, i));
            }
            pool.join();
        }
        
        void kNNTest(const std::string &queryFile, const std::string &outputFile, const std::string &messageFile,const std::string &truthFile, const int queryK) {
            std::vector<VectorDataType> queryVList;
            std::vector<size_t> queryKList;
            std::vector<std::string> queryCList;
            // std::ofstream file(messageFile);
            // if (!file.is_open()) {
            //     std::cerr << "Failed to open file for dumping vector queries: " << messageFile << std::endl;
            //     std::exit(EXIT_FAILURE);
            // }
            // 就是本地读文件，但是queryK不是在这里设置的
            ReadHybridVectorQuery(queryFile, queryVList, queryKList, queryCList);
            std::fill(queryKList.begin(), queryKList.end(), queryK);
            size_t queryNum = queryVList.size();
            std::vector<std::vector<VidType>> ansList;
            double sendKnnQueryTime = 0;
            double getVectorTime = 0;
            for (size_t i = 0; i < queryNum; ++i) { // test the first query
                m_logger.SetStartTimer();
                m_testsLogger.SetStartTimer();
                sendKnnQuery(queryVList[i], queryKList[i], queryCList[i]);
                // std::cout << "sendKnnQuery takes " << m_testsLogger.GetDurationTime() << "ms" << std::endl;
                // file << "sendKnnQuery takes " << m_testsLogger.GetDurationTime() << "ms" << std::endl;
                parallelGetVector();
                // std::cout << "parallelGetVector takes " << m_testsLogger.GetDurationTime() << "ms" << std::endl;
                // file << "parallelGetVector takes " << m_testsLogger.GetDurationTime() << "ms" << std::endl;
                std::vector<VidType> ansi;
                for(int j = 0;j < siloNum;j++) {
                    std::vector<VidType> idSet = siloPtr[j]->getAnsId();
                    ansi.insert(ansi.end(), idSet.begin(), idSet.end());
                }
                std::cout << i << "th result's size : " << (int)ansi.size() << std::endl;
                // file << i << "th result's size : " << (int)ansi.size() << std::endl;
                // assert((int)result.size() == queryKList[i]);
                ansList.emplace_back(ansi);
                m_logger.SetEndTimer();
                m_testsLogger.SetEndTimer();
                double queryComm = getComm();
                std::cout << "queryBrokerComm#" << i << " : " << queryComm << std::endl;
                for (size_t j = 0;j < filterNum;j++) {
                    double filterComm = filterPtr[j]->getComm();
                    queryComm += filterComm;
                    std::cout << "queryFilterComm#" << i << " : " << filterComm << std::endl;
                }
                
                for (size_t j = 0; j < siloNum; j++) {
                    float siloComm = siloPtr[j]->getComm();
                    queryComm += siloComm;
                    std::cout << "queryAnsComm#" << i << " : " << siloComm << std::endl;
                }
                m_logger.LogAddComm(queryComm);
                double queryTime = m_logger.GetDurationTime();
                m_logger.LogOneQuery();
                if (one_num_seq.find(i) == one_num_seq.end()) {
                    m_testsLogger.LogAddComm(queryComm);
                    m_testsLogger.LogOneQuery();
                }

                #ifdef SHOW_EVERYONE
                std::cout << std::fixed << std::setprecision(6) 
                    << "Query #(" << queryVList[i].vid << "): runtime = " << queryTime/1000.0 << " [s], communication = " << queryComm/1024.0 << " [KB]" << std::endl;
                // file << std::fixed << std::setprecision(6) 
                    // << "Query #(" << queryVList[i].vid << "): runtime = " << queryTime/1000.0 << " [s], communication = " << queryComm/1024.0 << " [KB]" << std::endl;
                #endif
            }
            DumpGroundTruth(outputFile, ansList);
            std::string message = m_logger.Print();
            m_testsLogger.Print();
            // file << message;
            // file.close();
            EvaluateAnswer(outputFile, truthFile, messageFile, one_num_seq);
            // std::cout << "AVG sendKnnQueryTime: " << sendKnnQueryTime / queryNum << " [ms], AVG getVectorTime: " << getVectorTime / queryNum << " [ms]" << std::endl;
            // file << "AVG sendKnnQueryTime: " << sendKnnQueryTime / queryNum << " [ms], AVG getVectorTime: " << getVectorTime / queryNum << " [ms]" << std::endl;
            // std::cout << "optimized top-k time: " << 1.0 * optTopKTime / queryNum << "ms" << std::endl;
            // std::cout << "refine k time: " << 1.0 * optRefineTime / (queryNum * siloNum) << "ms" << std::endl;
        }
};

std::shared_ptr<BrokerConnector> brokerPtr = nullptr;

int main(int argc, char** argv) {
    std::string queryFile, outputFile, messageFile,truthFile, brokerIpAddr, ipAddrFile, filterIpAddr;
    int queryK;
    try { 
        bpo::options_description option_description("Required options");
        option_description.add_options()
            ("help", "produce help message")
            ("broker-ip", bpo::value<std::string>(), "ip address of central server")
            ("silo-ip", bpo::value<std::string>(), "ip address of each data provider")
            ("filter-ip", bpo::value<std::string>(), "ip address of filter")
            ("query-k", bpo::value<int>(), "query parameter $k$")
            ("query-path", bpo::value<std::string>(), "file path of query vectors and attribute filter")
            ("output-path", bpo::value<std::string>(), "file path that stores output")
            ("message-path", bpo::value<std::string>(), "file path that stores message")
            ("truth-path", bpo::value<std::string>(), "file path that stores ground truth of queries");

        bpo::variables_map variable_map;
        bpo::store(bpo::parse_command_line(argc, argv, option_description), variable_map);
        bpo::notify(variable_map);    

        if (variable_map.count("help")) {
            std::cout << option_description << std::endl;
            return 0;
        }

        bool options_all_set = true;

        if (variable_map.count("broker-ip")) {
            brokerIpAddr = variable_map["broker-ip"].as<std::string>();
        } else {
            options_all_set = false;
        }

        if (variable_map.count("silo-ip")) {
            ipAddrFile = variable_map["silo-ip"].as<std::string>();
            std::cout << "Data silo's IP configuration file path was set to " << ipAddrFile << "\n";
        } else {
            std::cout << "Data silo's IP configuration file path was not set" << "\n";
            options_all_set = false;
        }

        if (variable_map.count("filter-ip")) {
            filterIpAddr = variable_map["filter-ip"].as<std::string>();
            std::cout << "Filter's IP address was set to " << filterIpAddr << "\n";
        } else {
            std::cout << "Filter's IP address was not set" << "\n";
        }

        if (variable_map.count("query-k")) {
            queryK = variable_map["query-k"].as<int>();
            std::cout << "The K of KNN was set to " << queryK << "\n";
        } else {
            std::cout << "The K of KNN was not set" << "\n";
        }

        if (variable_map.count("query-path")) {
            queryFile = variable_map["query-path"].as<std::string>();
            std::cout << "User's received query file path was set to " << queryFile << "\n";
        } else {
            std::cout << "User's received query file path was not set" << "\n";
            options_all_set = false;
        }

        if (variable_map.count("output-path")) {
            outputFile = variable_map["output-path"].as<std::string>();
            std::cout << "User's answer output file path was set to " << outputFile << "\n";
        } else {
            std::cout << "User's answer output file path was not set" << "\n";
        }

        if (variable_map.count("message-path")) {
            messageFile = variable_map["message-path"].as<std::string>();
            std::cout << "User's message file path was set to " << messageFile << "\n";
        } else {
            std::cout << "User's message file path was not set" << "\n";
        }

        if (variable_map.count("truth-path")) {
            truthFile = variable_map["truth-path"].as<std::string>();
            std::cout << "User's ground truth file path was set to " << truthFile << "\n";
        } else {
            std::cout << "User's ground truth file path was not set" << "\n";
        }

        if (false == options_all_set) {
            throw std::invalid_argument("Some options were not properly set");
            std::cout.flush();
            std::exit(EXIT_FAILURE);
        }
    } catch (std::exception& e) {  
        std::cerr << "Error: " << e.what() << "\n";  
        std::exit(EXIT_FAILURE);
    }

    grpc::ChannelArguments args;  
    args.SetInt(GRPC_ARG_MAX_SEND_MESSAGE_LENGTH, INT_MAX);  
    args.SetInt(GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH, INT_MAX);
    std::string url = brokerIpAddr;
    brokerPtr = std::make_shared<BrokerConnector>(grpc::CreateCustomChannel(brokerIpAddr.c_str(),
                                grpc::InsecureChannelCredentials(), args), brokerIpAddr, ipAddrFile, filterIpAddr);
    brokerPtr->kNNTest(queryFile, outputFile, messageFile,truthFile, queryK);
}

