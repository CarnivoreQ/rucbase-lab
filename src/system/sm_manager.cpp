/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sm_manager.h"

#include <sys/stat.h>
#include <unistd.h>

#include <fstream>

#include "index/ix.h"
#include "record/rm.h"
#include "record_printer.h"

/**
 * @description: 判断是否为一个文件夹
 * @return {bool} 返回是否为一个文件夹
 * @param {string&} db_name 数据库文件名称，与文件夹同名
 */
bool SmManager::is_dir(const std::string& db_name) {
    struct stat st;
    return stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

/**
 * @description: 创建数据库，所有的数据库相关文件都放在数据库同名文件夹下
 * @param {string&} db_name 数据库名称
 */
void SmManager::create_db(const std::string& db_name) {
    //Todo:
    //0.已存在则Error
    //1.为数据库创建并进入子目录
    //2.在子目录初始化数据库并将其写入文件
    //3.为数据库创建日志文件
    //4.完成操作回到根目录

    //0.检查db是否已经存在，如果存在，throw一个错误
    if (is_dir(db_name)) {
        throw DatabaseExistsError(db_name);
    }
    //1.为数据库创建一个子目录
    std::string cmd = "mkdir " + db_name;
    if (system(cmd.c_str()) < 0) {  // 创建一个名为db_name的目录
        throw UnixError();
    }
    if (chdir(db_name.c_str()) < 0) {  // 进入名为db_name的目录
        throw UnixError();
    }
    //2.1初始化数据库元数据
    DbMeta *new_db = new DbMeta();
    new_db->name_ = db_name;

    //2.2使用ofstream在当前目录打开一个名为DB_META_NAME的文件用于储存数据库，如果不存在DB_META_NAME，则创建该文件
    std::ofstream ofs(DB_META_NAME);

    //2.3将new_db中的信息，使用DBMeta定义的operator<<操作符，写入DB_META_NAME文件中
    ofs << *new_db; 
    //2.4写入完成后，将new_db删除
    delete new_db;

    //3.创建日志文件
    disk_manager_->create_file(LOG_FILE_NAME);

    //4.回到根目录
    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @description: 删除数据库，同时需要清空相关文件以及数据库同名文件夹
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::drop_db(const std::string& db_name) {
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }
    std::string cmd = "rm -r " + db_name;
    if (system(cmd.c_str()) < 0) {
        throw UnixError();
    }
}

/**
 * @description: 打开数据库，找到数据库对应的文件夹，并加载数据库元数据和相关文件
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::open_db(const std::string& db_name) {
    //Lab3 Task1 Todo
    //0.判断路径是否存在，如果不存在，Throw一个Error
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }
    //1.进入db_name目录
    if (chdir(db_name.c_str()) < 0) {
        throw UnixError();
    }
    //2.1加载DB元数据，使用ifstream在当前目录打开一个名为DB_META_NAME的文件用于储存数据库，如果不存在DB_META_NAME，则创建该文件
    std::ifstream ifs(DB_META_NAME);
    //2.2使用重载操作符>>将DB_META_NAME中的内容读出到db_中
    ifs >> db_;

    //3.加载数据库中所有内容(数据、索引)
    for (auto& entry : db_.tabs_) {
        std::string tab_name = entry.first;
        //3.1加载每张表的记录(rm_manager)
        fhs_[tab_name] = rm_manager_->open_file(tab_name);
        //3.2加载索引(ix_manager)
        for (auto& index : db_.tabs_[tab_name].indexes) {
            ihs_.emplace(ix_manager_->get_index_name(tab_name, index.cols),
                         ix_manager_->open_index(tab_name, index.cols));
        }
    }
}

/**
 * @description: 把数据库相关的元数据刷入磁盘中
 */
void SmManager::flush_meta() {
    // 默认清空文件
    std::ofstream ofs(DB_META_NAME);
    ofs << db_;
}

/**
 * @description: 关闭数据库并把数据落盘
 */
void SmManager::close_db() {
    //Lab3 Task1 Todo
    //1.使用ofstream在当前目录打开一个名为DB_META_NAME的文件用于储存数据库，如果不存在DB_META_NAME，则创建该文件。
    flush_meta();
    //2.1关闭记录(rm_manager)
    for (auto& entry : fhs_) {
        const RmFileHandle* file_handle = entry.second.get();
        rm_manager_->close_file(file_handle);
    }
    //2.2关闭索引(ix_manager)
    for (auto& entry : ihs_) {
        const IxIndexHandle* index_handle = entry.second.get();
        ix_manager_->close_index(index_handle);
    }

    //3.删除已打开信息
    db_.name_.clear();
    db_.tabs_.clear();
    fhs_.clear();
    ihs_.clear();

    //4.回到根目录
    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @description: 显示所有的表,通过测试需要将其结果写入到output.txt,详情看题目文档
 * @param {Context*} context 
 */
void SmManager::show_tables(Context* context) {
    std::fstream outfile;
    outfile.open("output.txt", std::ios::out | std::ios::app);
    outfile << "| Tables |\n";
    RecordPrinter printer(1);
    printer.print_separator(context);
    printer.print_record({"Tables"}, context);
    printer.print_separator(context);
    for (auto &entry : db_.tabs_) {
        auto &tab = entry.second;
        printer.print_record({tab.name}, context);
        outfile << "| " << tab.name << " |\n";
    }
    printer.print_separator(context);
    outfile.close();
}

/**
 * @description: 显示表的元数据
 * @param {string&} tab_name 表名称
 * @param {Context*} context 
 */
void SmManager::desc_table(const std::string& tab_name, Context* context) {
    TabMeta &tab = db_.get_table(tab_name);

    std::vector<std::string> captions = {"Field", "Type", "Index"};
    RecordPrinter printer(captions.size());
    // Print header
    printer.print_separator(context);
    printer.print_record(captions, context);
    printer.print_separator(context);
    // Print fields
    for (auto &col : tab.cols) {
        std::vector<std::string> field_info = {col.name, coltype2str(col.type), col.index ? "YES" : "NO"};
        printer.print_record(field_info, context);
    }
    // Print footer
    printer.print_separator(context);
}

/**
 * @description: 创建表
 * @param {string&} tab_name 表的名称
 * @param {vector<ColDef>&} col_defs 表的字段
 * @param {Context*} context 
 */
void SmManager::create_table(const std::string& tab_name, const std::vector<ColDef>& col_defs, Context* context) {
    //Lab2/3 Task1 Todo
    //0.如果表已存在，则throw一个Error
    if (db_.is_table(tab_name)) {
        throw TableExistsError(tab_name);
    }
    //1.初始化表元数据
    int curr_offset = 0;
    TabMeta tab;
    tab.name = tab_name;
    for (auto &col_def : col_defs) {
        ColMeta col = {.tab_name = tab_name,
                       .name = col_def.name,
                       .type = col_def.type,
                       .len = col_def.len,
                       .offset = curr_offset,
                       .index = false};
        curr_offset += col_def.len;
        tab.cols.push_back(col);
    }
    //2.创建并打开记录
    int record_size = curr_offset;  // record_size就是colmeta所占的大小（表的元数据也是以记录的形式进行存储的）
    rm_manager_->create_file(tab_name, record_size);
    db_.tabs_[tab_name] = tab;
    // fhs_[tab_name] = rm_manager_->open_file(tab_name);
    fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));

    //flush_meta();
}

/**
 * @description: 删除表
 * @param {string&} tab_name 表的名称
 * @param {Context*} context
 */
void SmManager::drop_table(const std::string& tab_name, Context* context) {
    //Lab3 Task1 Todo
    if (db_.is_table(tab_name)) {
        //0.1在db_meta找到表的索引
        const RmFileHandle* file_handle = fhs_[tab_name].get();
        //1.关闭并删除记录文件
        rm_manager_->close_file(file_handle);
        rm_manager_->destroy_file(tab_name);  // 数据文件
        //2.关闭并删除索引，清除索引记录
        for (auto& index : db_.tabs_[tab_name].indexes) {
            if (ix_manager_->exists(tab_name, index.cols)) {
                //2.1关闭索引文件
                std::string idx_name = ix_manager_->get_index_name(tab_name, index.cols);
                const IxIndexHandle* ih = ihs_[idx_name].get();
                ix_manager_->close_index(ih);
                //2.2删除索引文件
                ix_manager_->destroy_index(tab_name, index.cols);
                //2.3清除索引记录
                ihs_.erase(idx_name);
            }
        }
        //3.清除表信息
        db_.tabs_.erase(tab_name);
        fhs_.erase(tab_name);
    } 
    else {//0.2找不到表时，throw一个Error
        throw TableNotFoundError(tab_name);
    }
}

/**
 * @description: 创建索引
 * @param {string&} tab_name 表的名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::create_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    //Lab3 Task1 Todo
    //1.检查索引是否已经存在，如果已经存在则throw一个Error
    if (ix_manager_->exists(tab_name, col_names)) {
        throw IndexExistsError(tab_name, col_names);
    }
    //2.1获取要索引的列的元数据，注意，同时get表名和列名
    std::vector<ColMeta> index_cols;
    for (auto& col_name : col_names) {
        index_cols.push_back(*(db_.get_table(tab_name).get_col(col_name)));
    }
    //2.2调用IxManager的方法建立索引
    ix_manager_->create_index(tab_name, index_cols);
    //2.3打开索引并放入ihs中
    std::string ix_name = ix_manager_->get_index_name(tab_name, col_names);
    ihs_.emplace(ix_name, ix_manager_->open_index(tab_name, col_names));
    //3.更新表上建立的索引(indexes)
    IndexMeta idx_meta;
    idx_meta.tab_name = tab_name;
    idx_meta.col_tot_len = 0;
    for (auto col_meta : index_cols) {
        idx_meta.col_tot_len += col_meta.len;
    }
    idx_meta.col_num = index_cols.size();
    idx_meta.cols = index_cols;

    db_.tabs_[tab_name].indexes.push_back(idx_meta);
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    //Lab3 Task1 Todo
    //0.索引不存在时，Throw一个Error(由于ColMeta中的index处于unused状态，这部分略过（逃)
    /*TabMeta &tab = db_.tabs_[tab_name];
    auto col = tab.get_col(col_name);
    if (!col->index) {
        throw IndexNotFoundError(tab_name, col_name);
    }*/
    //1.关闭索引文件
    std::string idx_name = ix_manager_->get_index_name(tab_name, col_names);
    const IxIndexHandle* ih = ihs_[idx_name].get();
    ix_manager_->close_index(ih);
    //2.删除索引文件
    ix_manager_->destroy_index(tab_name, col_names);
    //3.从ihs中清除索引
    ihs_.erase(ix_manager_->get_index_name(tab_name, col_names));
    //4.更新表上建立的索引
    auto idx_meta = db_.get_table(tab_name).get_index_meta(col_names);
    db_.get_table(tab_name).indexes.erase(idx_meta);
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<ColMeta>&} 索引包含的字段元数据
 * @param {Context*} context
 */
/*void SmManager::drop_index(const std::string& tab_name, const std::vector<ColMeta>& index_cols, Context* context) {
    std::string idx_name = ix_manager_->get_index_name(tab_name, index_cols);
    const IxIndexHandle* ih = ihs_[idx_name].get();
    ix_manager_->close_index(ih);
    // 删除索引文件
    ix_manager_->destroy_index(tab_name, index_cols);
    // 从ihs中删除
    ihs_.erase(ix_manager_->get_index_name(tab_name, index_cols));
    // 更新indexes
    auto idx_meta = db_.get_table(tab_name).//indexes;
    db_.get_table(tab_name).indexes.erase(idx_meta);
}*/