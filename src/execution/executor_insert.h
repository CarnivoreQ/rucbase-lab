/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

/*insert算子通过读取传入的数据，构建记录缓冲，然后将数据插入到表的数据文件中，
  并在需要的情况下更新相应的索引。保证了数据的插入操作是原子的，同时也考虑了索引的更新*/
#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"
//定义了一个名为InsertExecutor的类，继承自AbstractExecutor基类，表示插入算子的执行器
class InsertExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;                   // 保存表的元数据，包括表名、列信息
    std::vector<Value> values_;     // 保存需要插入的数据值，对应表的各列
    RmFileHandle *fh_;              // 表的数据文件句柄，用于插入数据
    std::string tab_name_;          // 表名称
    Rid rid_;                       // 插入的位置，由于系统默认插入时不指定位置，因此当前rid_在插入后才赋值
    SmManager *sm_manager_;         // 指向数据库管理器的指针，用于获取表的信息、进行文件操作等

   public:
   // 构造函数初始化了插入算子的成员变量，包括数据库管理器指针、表的元数据、需要插入的数据、表的名称
    InsertExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Value> values, Context *context) {
        sm_manager_ = sm_manager;
        tab_ = sm_manager_->db_.get_table(tab_name);
        values_ = values;
        tab_name_ = tab_name;
        if (values.size() != tab_.cols.size()) {
            throw InvalidValueCountError();
        }
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        context_ = context;
    };
    // Next函数用于执行插入操作
    std::unique_ptr<RmRecord> Next() override {
        // Make record buffer
        RmRecord rec(fh_->get_file_hdr().record_size);// 创建一个 RmRecord 对象 rec 作为记录缓冲，用于存放插入的数据
        // 遍历表的每一列，将待插入数据拷贝到记录缓冲的相应位置
        for (size_t i = 0; i < values_.size(); i++) {
            auto &col = tab_.cols[i];
            auto &val = values_[i];
            // 异常处理
            if (col.type != val.type) {
                throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
            }
            val.init_raw(col.len);
            memcpy(rec.data + col.offset, val.raw->data, col.len);
        }
        // 将记录缓冲中的数据插入到表的数据文件中，并获取插入的位置 rid_
        rid_ = fh_->insert_record(rec.data, context_);
        
        // 遍历表的每个索引，获取索引句柄，并将插入的数据插入到相应的索引中
        for(size_t i = 0; i < tab_.indexes.size(); ++i) {
            auto& index = tab_.indexes[i];
            auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
            char* key = new char[index.col_tot_len];
            int offset = 0;
            for(size_t i = 0; i < index.col_num; ++i) {
                memcpy(key + offset, rec.data + index.cols[i].offset, index.cols[i].len);
                offset += index.cols[i].len;
            }
            ih->insert_entry(key, rid_, context_->txn_);
        }
        return nullptr;// 插入算子在执行过程中没有返回值，Next 函数返回一个空指针
    }
    Rid &rid() override { return rid_; }
};

