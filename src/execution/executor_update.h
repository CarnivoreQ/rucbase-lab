/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

/*UpdateExecutor 类的实现通过遍历指定的记录位置，
根据赋值语句列表更新对应的字段值，实现了更新操作。
在更新记录时，首先从索引中删除旧的记录，然后在数据文件中更新记录，最后将新的记录插入到索引中*/

#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"
// 定义了一个类，继承自 AbstractExecutor 基类，表示更新操作的执行器
class UpdateExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;                              // 表的元数据
    std::vector<Condition> conds_;             // 更新的条件
    RmFileHandle *fh_;                         // 表的数据文件句柄
    std::vector<Rid> rids_;                    // 需要更新的记录的位置
    std::string tab_name_;                     // 表名称
    std::vector<SetClause> set_clauses_;       // 更新操作的赋值语句列表
    SmManager *sm_manager_;                    // SmManager 的指针

    std::vector<ColMeta> cols_;                // 表的列元数据

   public:
   // 构造函数接受 SmManager、表名、更新操作的赋值语句、更新的条件、要更新的记录位置和上下文
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;

        cols_ = tab_.cols;
    }
    // 用于更新操作
    std::unique_ptr<RmRecord> Next() override {
        // Update each rid of record file and index file
        for (auto &rid : rids_) {// 遍历要更新的记录位置 rids
            auto rec = fh_->get_record(rid, context_);
            for (auto &set_clause : set_clauses_) {// 对于每个记录位置，获取记录，并根据赋值语句列表 set_clauses_ 更新对应的字段值
                auto lhs_col = tab_.get_col(set_clause.lhs.col_name);
                memcpy(rec->data + lhs_col->offset, set_clause.rhs.raw->data, lhs_col->len);
            }
            // 对于每个更新的记录，首先从索引中删除旧的记录，然后在数据文件中更新记录，最后将新的记录插入到索引中
            // Remove old entry from index
            for (size_t i = 0; i < tab_.indexes.size(); ++i) {
                auto &index = tab_.indexes[i];
                auto ih =
                    sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                char *key = new char[index.col_tot_len];
                int offset = 0;
                for (size_t j = 0; j < index.col_num; ++j) {
                    memcpy(key + offset, rec->data + index.cols[j].offset, index.cols[j].len);
                    offset += index.cols[j].len;
                }
                ih->delete_entry(key, context_->txn_);
            }
            // Update record in record file
            fh_->update_record(rid, rec->data, context_);
            // Insert new index into index
            for (size_t i = 0; i < tab_.indexes.size(); ++i) {
                auto &index = tab_.indexes[i];
                auto ih =
                    sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                char *key = new char[index.col_tot_len];
                int offset = 0;
                for (size_t j = 0; j < index.col_num; ++j) {
                    memcpy(key + offset, rec->data + index.cols[j].offset, index.cols[j].len);
                    offset += index.cols[j].len;
                }
                ih->insert_entry(key, rid, context_->txn_);
            }
        }
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }// 表示当前记录的位置标识

    // 返回记录的长度，对于更新操作，可以直接返回 0
    size_t tupleLen() const override { return 0; };
    // 返回表的列元数据
    const std::vector<ColMeta> &cols() const override { return cols_; };
    // 对于更新操作，这两个函数没有具体实现，可以直接空实现
    void beginTuple() override{};
    void nextTuple() override{};
    // 对于更新操作，可以直接返回 true，表示执行一次更新后即结束
    bool is_end() const override { return true; };
    std::string getType() override { return "UpdateExecutor"; };
    ColMeta get_col_offset(const TabCol &target) override { return ColMeta(); };
};