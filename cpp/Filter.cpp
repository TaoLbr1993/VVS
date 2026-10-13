#include "SiloFilter.grpc.pb.h"
#include "SiloFilter.pb.h"
#include "middleware/BaseSiloConnector.hpp"
#include "middleware/MilvusSiloConnector.hpp"
#include "cluster/clusterBuilder.hpp"
#include "global.hpp"
#include <grpcpp/grpcpp.h>
#include <grpc/grpc.h>
#include <boost/program_options.hpp>
#include <grpcpp/support/status.h>

namespace bpo = boost::program_options;

#include <iostream>
#include <memory>
#include <string>
#include <sqlite3.h>
#include "utils/BenchLogger.hpp"

using silofilter::rpcComm;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::Status;
using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using silofilter::SiloFilter;
using google::protobuf::Empty;
class SiloFilterImpl final : public SiloFilter::Service {
    private:
        std::string condition;
        sqlite3* sqlitePtr;
        BenchLogger m_logger;
        // 这个也可能是分多个table，然后不同函数的table不一样的，有待考量
        std::string tableName;
        // 应该查询的Stmt也可以提前构造出来，然后现在只能是一个query一个query查找，不能是多个query并行
        std::vector<std::pair<std::string, std::string>> equalCondList;
        std::vector<std::tuple<std::string, int, int>> unequalCondList;
        int queryId = 0;

        // 不是很懂，直接复制粘贴的
        static int callback(void *NotUsed, int argc, char **argv, char **azColName){
            int i;
            for(i=0; i<argc; i++){
                printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
            }
            printf("\n");
            return 0;
        }
    public:
        explicit SiloFilterImpl(const std::string& sqlDBName, const std::string& dataName, const std::string& scalarName, const std::string& tableName) : tableName(tableName) {
            m_logger.Init();
            // 提前open完毕可能之后的插入/查询的时间开销会小一些
            int rc = sqlite3_open(sqlDBName.c_str(), &sqlitePtr);
            if (rc != SQLITE_OK) {
                std::cerr << "Failed to open SQLite database: " << sqlite3_errmsg(sqlitePtr) << std::endl;
                std::exit(EXIT_FAILURE);
            }
            sqlite3_exec(sqlitePtr, "PRAGMA journal_mode=WAL;", nullptr, nullptr, nullptr);
            sqlite3_exec(sqlitePtr, "PRAGMA synchronous=NORMAL;", nullptr, nullptr, nullptr);
            sqlite3_busy_timeout(sqlitePtr, 5000);
            std::cout << "Connect SQLite database successfully." << std::endl;
            createAndInsertToSQLite(dataName, scalarName, tableName);
        }

        void createAndInsertToSQLite(const std::string& dataName, const std::string& scalarName, const std::string& tableName) {
            std::vector<VectorDataType> dataList;
            // 这里应该是有一些冗余，只需要读vid就行了，等后期再改
            ReadVectorData(dataName, dataList);
            std::vector<std::shared_ptr<attribute>> scalarList;
            ReadScalarData(scalarName, scalarList);
            std::string createTableQuery = "CREATE TABLE IF NOT EXISTS " + tableName + " (id INTEGER PRIMARY KEY NOT NULL";
            for (size_t i = 0; i < scalarList.size(); i++) {
                createTableQuery += ",\n" + scalarList[i]->getName() + " ";
                if (scalarList[i]->getType() == INT) {
                    createTableQuery += "INTEGER ";
                } else if (scalarList[i]->getType() == FLOAT) {
                    createTableQuery += "REAL ";
                } else {
                    createTableQuery += "VARCHAR(50) ";
                }
                createTableQuery += "NOT NULL";
            }
            createTableQuery += ");";
            // std::cout << createTableQuery << std::endl;
            char *zErrMsg = nullptr;
            int rc = sqlite3_exec(sqlitePtr, createTableQuery.c_str(), callback, nullptr, &zErrMsg);
            if( rc != SQLITE_OK ){
               fprintf(stderr, "SQL error: %s\n", zErrMsg);
               sqlite3_free(zErrMsg);
               std::exit(EXIT_FAILURE);
            }else{
               fprintf(stdout, "Table created successfully\n");
            }
            rc = sqlite3_exec(sqlitePtr, "BEGIN TRANSACTION;", nullptr, nullptr, nullptr);
            if(rc != SQLITE_OK) {
                std::cerr << "Failed to begin transaction: " << sqlite3_errmsg(sqlitePtr) << std::endl;
                std::exit(EXIT_FAILURE);
            }
            sqlite3_stmt* stmt = nullptr;
            std::string insertQuery = "INSERT INTO " + tableName + " (id";
            for (size_t i = 0; i < scalarList.size(); i++) {
                insertQuery += ", " + scalarList[i]->getName();
            }
            insertQuery += ") VALUES (?";
            for (size_t i = 0; i < scalarList.size(); i++) {
                insertQuery += ", ?";
            }
            insertQuery += ");";
            // std::cout << insertQuery << std::endl;
            sqlite3_prepare_v2(sqlitePtr,
                insertQuery.c_str(),
                -1, &stmt, nullptr);

            // 这里重复插入的后果有待考量
            for (size_t i = 0; i < dataList.size(); i++) {
                sqlite3_bind_int64(stmt, 1, dataList[i].vid);
                for (size_t j = 0; j < scalarList.size(); j++) {
                    if (scalarList[j]->getType() == INT) {
                        sqlite3_bind_int(stmt, j + 2, dynamic_cast<IntType*>(scalarList[j].get())->getValue(i));
                    } else if (scalarList[j]->getType() == FLOAT) {
                        sqlite3_bind_double(stmt, j + 2, dynamic_cast<FloatType*>(scalarList[j].get())->getValue(i));
                    } else {
                        // 这个东西写的时候必须要加SQLITE_TRANSIENT，有待研究
                        sqlite3_bind_text(stmt, j + 2, dynamic_cast<StringType*>(scalarList[j].get())->getValue(i).c_str(), -1, SQLITE_TRANSIENT);
                    }
                }
                // 这个一步一步的执行，如果是SELECT的话是一次取一个结果
                rc = sqlite3_step(stmt);
                if(rc != SQLITE_DONE) {
                    std::cerr << "Failed to insert data into SQLite database: " << sqlite3_errmsg(sqlitePtr) << std::endl;
                    std::exit(EXIT_FAILURE);
                }
                // 这个是重置状态，就是这个stmt可以再执行一次
                sqlite3_reset(stmt);
                // 这个是清空bind
                sqlite3_clear_bindings(stmt);
            }
            sqlite3_finalize(stmt);
            rc = sqlite3_exec(sqlitePtr, "COMMIT;", nullptr, nullptr, nullptr);
            if (rc != SQLITE_OK) {
                std::cerr << "Failed to insert data into SQLite database: " << sqlite3_errmsg(sqlitePtr) << std::endl;
                std::exit(EXIT_FAILURE);
            }else{
               fprintf(stdout, "Data inserted successfully\n");
            }
        }

        float getProportion() {
            BenchLogger tem_logger;
            tem_logger.Init();
            tem_logger.SetStartTimer();
            // 构建 SQL
            std::string searchQuery = "SELECT COUNT(*) FROM " + tableName;
            searchQuery += " WHERE ";
            if (!equalCondList.empty()) {
                for (size_t i = 0; i < equalCondList.size(); i++) {
                    searchQuery += equalCondList[i].first + "=?";
                    if (i + 1 < equalCondList.size()) searchQuery += " AND ";
                }
            }
            
            if (!unequalCondList.empty()) {
                if (!equalCondList.empty()) {
                    searchQuery += " AND ";
                }
                for (size_t i = 0; i < unequalCondList.size(); i++) {
                    const auto& [field, lower, upper] = unequalCondList[i];
                    searchQuery += "(" + field + " BETWEEN ? AND ?)";
                    if (i + 1 < unequalCondList.size()) searchQuery += " AND ";
                }
            }
            searchQuery += ";";
            std::cout << "proportion query: " << searchQuery << std::endl;

            // 准备与绑定
            sqlite3_stmt* stmt = nullptr;
            int rc = sqlite3_prepare_v2(sqlitePtr, searchQuery.c_str(), -1, &stmt, nullptr);
            if (rc != SQLITE_OK) {
                std::cerr << "prepare failed: " << sqlite3_errmsg(sqlitePtr) << std::endl;
                std::exit(EXIT_FAILURE);
            }
            for (size_t i = 0; i < equalCondList.size(); i++) {
                const std::string& v = equalCondList[i].second;
                sqlite3_bind_text(stmt, static_cast<int>(i + 1), v.c_str(), -1, SQLITE_TRANSIENT);
            }

            for (size_t i = 0; i < unequalCondList.size(); i++) {
                const auto& [field, lower, upper] = unequalCondList[i];
                sqlite3_bind_int(stmt, static_cast<int>(equalCondList.size() + i * 2 + 1), lower);
                sqlite3_bind_int(stmt, static_cast<int>(equalCondList.size() + i * 2 + 2), upper);
            }

            // 执行 COUNT(*)（只会有一行）
            int matchedCount = 0;
            rc = sqlite3_step(stmt);
            if (rc == SQLITE_ROW) {
                matchedCount = sqlite3_column_int(stmt, 0);
            } else {
                // COUNT(*) 必然返回一行，任何其他情况都是错误
                std::cerr << "COUNT query failed (rc=" << rc << "): " << sqlite3_errmsg(sqlitePtr) << std::endl;
                sqlite3_finalize(stmt);
                std::exit(EXIT_FAILURE);
            }
            sqlite3_finalize(stmt);
            tem_logger.SetEndTimer();
            if (queryId == 4) {
                std::cout << "the #" << queryId << " query COUNT(*) time: " << tem_logger.GetDurationTime() << " ms"<<std::endl;
            }

            tem_logger.SetStartTimer();
            // 表总数
            std::string totalSql = "SELECT COUNT(*) FROM " + tableName + ";";
            rc = sqlite3_prepare_v2(sqlitePtr, totalSql.c_str(), -1, &stmt, nullptr);
            if (rc != SQLITE_OK) {
                std::cerr << "prepare failed: " << sqlite3_errmsg(sqlitePtr) << std::endl;
                // 先直接暴力退出，先不写更健壮的错误处理了
                std::exit(EXIT_FAILURE);
            }
            int totalCount = 0;
            rc = sqlite3_step(stmt);
            if (rc == SQLITE_ROW) {
                totalCount = sqlite3_column_int(stmt, 0);
            } else {
                // COUNT(*) 必然返回一行，任何其他情况都是错误
                std::cerr << "COUNT total failed (rc=" << rc << "): " << sqlite3_errmsg(sqlitePtr) << std::endl;
                sqlite3_finalize(stmt);
                std::exit(EXIT_FAILURE);
            }
            sqlite3_finalize(stmt);

            tem_logger.SetEndTimer();
            if (queryId == 4) {
                std::cout << "the #" << queryId << " query total COUNT(*) time:} " << tem_logger.GetDurationTime() << " ms"<<std::endl;
            }
            // 计算比例（避免除零）
            double proportion = (totalCount > 0) ? (1.0 * matchedCount / totalCount) : 0.0;
            std::cout << "proportion: " << proportion << std::endl;
            return proportion;
        }

        void searchFromSQLite(std::vector<int>& filteredIdList, const std::vector<int>& idList) {
            m_logger.SetStartTimer();
            std::string searchQuery = "SELECT id FROM " + tableName + " WHERE ";
            for (size_t i = 0; i < equalCondList.size(); i++) {
                searchQuery += equalCondList[i].first + "=?";
                if (i != equalCondList.size() - 1) {
                    searchQuery += " and ";
                }
            }
            if (!unequalCondList.empty()) {
                if (!equalCondList.empty()) {
                    searchQuery += " and ";
                }
                for (size_t i = 0; i < unequalCondList.size(); i++) {
                    const auto& [field, lower, upper] = unequalCondList[i];
                    searchQuery += "(" + field + " BETWEEN ? AND ?)";
                    if (i + 1 < unequalCondList.size()) searchQuery += " AND ";
                }

            }
            searchQuery += " and id in (";
            for (size_t i = 0; i < idList.size(); i++) {
                searchQuery += "?";
                if (i != idList.size() - 1) {
                    searchQuery += ", ";
                }
            }
            searchQuery += ");";
            // std::cout << searchQuery << std::endl;
            sqlite3_stmt* stmt = nullptr;
            sqlite3_prepare_v2(sqlitePtr, searchQuery.c_str(), -1, &stmt, nullptr);
            for (size_t i = 0; i < equalCondList.size(); i++) {
                sqlite3_bind_text(stmt, i + 1, equalCondList[i].second.c_str(), -1, nullptr);
            }
            for (size_t i = 0; i < unequalCondList.size(); i++) {
                const auto& [field, lower, upper] = unequalCondList[i];
                sqlite3_bind_int(stmt, static_cast<int>(equalCondList.size() + i * 2 + 1), lower);
                sqlite3_bind_int(stmt, static_cast<int>(equalCondList.size() + i * 2 + 2), upper);
            }
            for (size_t i = 0; i < idList.size(); i++) {
                sqlite3_bind_int(stmt, i + equalCondList.size() + unequalCondList.size()*2 + 1, idList[i]);
            }
            int rc;
            while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
                filteredIdList.push_back(sqlite3_column_int(stmt, 0));
            }
            if(rc != SQLITE_DONE) {
                std::cerr << "step failed: " << sqlite3_errmsg(sqlitePtr) << std::endl;
                std::exit(EXIT_FAILURE);
            }

            sqlite3_finalize(stmt);
            m_logger.SetEndTimer();
            m_logger.LogOneQuery();
            m_logger.Print();
        }

        Status getProportion(ServerContext* context, const Empty* request, silofilter::proportionResp* response) override {
            response->set_proportion(getProportion());
            m_logger.LogAddComm(response->ByteSizeLong());
            return Status::OK;
        }

        Status sendCondition(ServerContext* context, const silofilter::conditonReq* request, Empty* response) override {
            std::cout << "receive condition: " << request->condition() << std::endl;
            condition = request->condition();
            parseQuery();
            m_logger.LogAddComm(request->ByteSizeLong());
            return Status::OK;
        }

        Status sendVector(ServerContext* context, const silofilter::vectorReq* request, silofilter::vectorResp* response) override {
            std::vector<int> idList;
            // m_logger.SetStartTimer();
            for(int i = 0; i < request->idlist_size(); i++) {
                idList.push_back(request->idlist(i));
            }
            m_logger.LogAddComm(request->ByteSizeLong());
            
            std::vector<int> filteredIdList;
            searchFromSQLite(filteredIdList, idList);
            for (auto& id : filteredIdList) {
                response->add_idlist(id);
                // std::cout << "id : " << id << std::endl;
            }
            // m_logger.SetEndTimer();
            // std::cout << "receive " << idList.size() << " vectors takes " << m_logger.GetDurationTime() << "ms" << std::endl;
            m_logger.LogAddComm(response->ByteSizeLong());
            return Status::OK;
        }

        Status testRoundTime(ServerContext* context, const silofilter::vectorReq* request, Empty* response) override {
            std::vector<int> idList;
            for(int i = 0; i < request->idlist_size(); i++) {
                idList.push_back(request->idlist(i));
            }
            return Status::OK;
        }

        Status requestComm(ServerContext* context, const Empty* request, rpcComm* response) override {
            queryId++;
            response->set_rpcdatanum(m_logger.GetQueryComm());
            m_logger.Init();
            return Status::OK;
        }

        std::pair<std::string, std::string> parseEqualExp(std::string token) {
            int ptr = 0;
            std::string name = "";
            while(std::isalpha(token[ptr])) {
                name.push_back(token[ptr]);
                ptr++;
            }
            ptr++;
            ptr++; //="
            std::string value = "";
            while(token[ptr] != '\"') {
                value.push_back(token[ptr]);
                ptr++;
            }
            return std::make_pair(name, value);
        }

        std::tuple<std::string, int, int> parseUnEqualExp(std::string &token) {
            int ptr = 0;
            std::string digit = "";
            int l, r;
            while(std::isdigit(token[ptr])){
                digit.push_back(token[ptr]);
                ptr++;
            } 
            l = std::atoi(digit.data());
            ptr++;
            ptr++; // <=
            std::string name = "";
            while(std::isalpha(token[ptr])) {
                name.push_back(token[ptr]);
                ptr++;
            }  
            ptr++;
            ptr++; // <=
            digit = "";
            while(ptr < token.size() && std::isdigit(token[ptr])) {
                digit.push_back(token[ptr]);
                ptr++;
            }
            r = std::atoi(digit.data());
            return std::make_tuple(name, l, r);
        }
    
        void parseQuery() {
            equalCondList.clear();
            unequalCondList.clear();
            std::istringstream iss(condition);
            std::vector<std::string> tokenSet;
            std::string token;
            while(std::getline(iss, token, ' ')) {
                tokenSet.emplace_back(token);
            }
            condition = "";
            if(std::isdigit(tokenSet[0][0])) { // unequal exp
                // 这个等之后看到具体例子了再改
                std::tuple<std::string, int, int> cond = parseUnEqualExp(tokenSet[0]);
                unequalCondList.emplace_back(cond);
            } else {
                std::pair<std::string, std::string> cond = parseEqualExp(tokenSet[0]);
                equalCondList.emplace_back(cond);
            }
            if((int)tokenSet.size() > 1) {
                assert(tokenSet[1] == "and");
                if(std::isdigit(tokenSet[2][0])) {
                    // 这个之后需要改
                    std::tuple<std::string, int, int> cond = parseUnEqualExp(tokenSet[2]);
                    unequalCondList.emplace_back(cond);
                } else {
                    std::pair<std::string, std::string> cond = parseEqualExp(tokenSet[2]);
                    equalCondList.emplace_back(cond);
                }
            }
        }
};

void RunService(const std::string& ipAddr, const std::string& sqlDbName, const std::string& dataName, const std::string& scalarName, const std::string& tableName) {
    std::string serverAddress = ipAddr;
    SiloFilterImpl service(sqlDbName, dataName, scalarName, tableName);
    ServerBuilder builder;
    builder.AddListeningPort(serverAddress, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    std::cout << "Silo Filter listening on " << serverAddress << std::endl;
    server->Wait();
    // 强制把输出缓冲区中的内容输出
    std::cout.flush();
}

int main(int argc, char** argv) {
    std::string ipAddr, sqlDbName, dataName, scalarName, tableName;
    int siloId;
    bool options_all_set = true;
    try { 
        bpo::options_description option_description("Required options");
        option_description.add_options()
            ("help", "produce help message")
            ("ip", bpo::value<std::string>(), "ip address of filter")
            ("sql-db-name", bpo::value<std::string>(), "name of sqlite database")
            ("data-name", bpo::value<std::string>(), "name of data")
            ("scalar-name", bpo::value<std::string>(), "name of scalar")
            ("table-name", bpo::value<std::string>(), "name of table");

        bpo::variables_map variable_map;
        bpo::store(bpo::parse_command_line(argc, argv, option_description), variable_map);
        bpo::notify(variable_map);    

        if (variable_map.count("help")) {
            std::cout << option_description << std::endl;
            return 0;
        }

        if (variable_map.count("ip")) {
            ipAddr = variable_map["ip"].as<std::string>();
            std::cout << "Filter's IP address was set to " << ipAddr << "\n";
        } else {
            std::cout << "Filter's IP address was not set" << "\n";
            options_all_set = false;
        }

        if (variable_map.count("sql-db-name")) {
            sqlDbName = variable_map["sql-db-name"].as<std::string>();
            std::cout << "Name of sqlite database was set to " << sqlDbName << "\n";
        } else {
            std::cout << "Name of sqlite database was not set" << "\n";
            options_all_set = false;
        }

        if (variable_map.count("data-name")) {
            dataName = variable_map["data-name"].as<std::string>();
            std::cout << "Name of data was set to " << dataName << "\n";
        } else {
            std::cout << "Name of data was not set" << "\n";
            options_all_set = false;
        }

        if (variable_map.count("scalar-name")) {
            scalarName = variable_map["scalar-name"].as<std::string>();
            std::cout << "Name of scalar was set to " << scalarName << "\n";
        } else {
            std::cout << "Name of scalar was not set" << "\n";
            options_all_set = false;
        }

        if (variable_map.count("table-name")) {
            tableName = variable_map["table-name"].as<std::string>();
            std::cout << "Name of table was set to " << tableName << "\n";
        } else {
            std::cout << "Name of table was not set" << "\n";
            options_all_set = false;
        }

        if (false == options_all_set) {
            throw std::invalid_argument("Some options were not properly set");
            std::cout.flush();
        }
    } catch (std::exception& e) {  
        std::cerr << "Error: " << e.what() << "\n";  
        std::exit(EXIT_FAILURE);
    }
    RunService(ipAddr, sqlDbName, dataName, scalarName, tableName);
    return 0;
}
