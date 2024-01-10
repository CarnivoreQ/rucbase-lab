/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

// DeleteExecutor 类的实现通过遍历指定的记录位置，并根据删除条件判断记录是否满足删除要求，实现了删除操作
#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"
// 定义了一个类，继承自 AbstractExecutor 基类，表示删除操作的执行器
class DeleteExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;                   // 表的元数据
    std::vector<Condition> conds_;  // delete的条件
    RmFileHandle *fh_;              // 表的数据文件句柄
    std::vector<Rid> rids_;         // 需要删除的记录的位置
    std::string tab_name_;          // 表名称
    SmManager *sm_manager_;         // SmManager 的指针

    std::vector<ColMeta> cols_;  // 自定义的列元数据

   public:
   // 构造函数接受 SmManager、表名、删除条件、要删除的记录位置和上下文
    DeleteExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Condition> conds,
                   std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }
    // 用于删除操作
    std::unique_ptr<RmRecord> Next() override {
        for (Rid rid : rids_) {          // 扫描记录
            if (!fh_->is_record(rid)) {  // 无记录扫描下一条
                continue;
            }

            if (!condCheck(fh_->get_record(rid, context_).get())) {  // 记录检查是否符合where语句
                continue;
            }

            // 满足删除条件，删除
            fh_->delete_record(rid, context_);
        }
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }
    // 用于检查删除条件是否满足
    bool condCheck(const RmRecord *l_record) {
        char *l_val_buf, *r_val_buf;
        const RmRecord *r_record;

        for (auto &condition : conds_) {  // 遍历删除条件 conds_，逐一判断记录的字段和条件是否满足
            CompOp op = condition.op;
            int cmp;

            // record和col确定数据位置
            auto l_col = get_col(cols_, condition.lhs_col);  // 左列元数据
            l_val_buf = l_record->data + l_col->offset;      // 确定左数据起点

            if (condition.is_rhs_val) {  // 值
                r_record = condition.rhs_val.raw.get();
                r_val_buf = r_record->data;

                cmp = cond_compare(l_val_buf, r_val_buf, condition.rhs_val.type, l_col->len);
            } else {  // 列
                auto r_col = get_col(cols_, condition.rhs_col);
                r_val_buf = l_record->data + r_col->offset;

                cmp = cond_compare(l_val_buf, r_val_buf, r_col->type, l_col->len);
            }
            if (!op_compare(op, cmp))  // 不满足条件
                return false;
        }
        return true;
    }
    // 用于比较两个字段的值
    int cond_compare(const char *l_val_buf, const char *r_val_buf, ColType type, int col_len) const {
        int cmp = ix_compare(l_val_buf, r_val_buf, type, col_len);
        return cmp;
    }
    // 用于比较两个值满足条件的情况
    bool op_compare(CompOp op, int cmp) const {
        if (op == OP_EQ) {
            return cmp == 0;
        } else if (op == OP_NE) {
            return cmp != 0;
        } else if (op == OP_LT) {
            return cmp < 0;
        } else if (op == OP_GT) {
            return cmp > 0;
        } else if (op == OP_LE) {
            return cmp <= 0;
        } else if (op == OP_GE) {
            return cmp >= 0;
        } else {
            throw InternalError("Invalid CompOp");
        }
    }
};