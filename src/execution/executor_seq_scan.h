/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

// SeqScanExecutor 的实现通过使用 RmScan 进行表的扫描，同时根据查询条件过滤满足条件的记录
#pragma once

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"
// 定义了一个类，继承自 AbstractExecutor 基类，表示表扫描算子的执行器
class SeqScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;              // 表的名称
    std::vector<Condition> conds_;      // scan的条件，即where子句中的条件
    RmFileHandle *fh_;                  // 表的数据文件句柄
    std::vector<ColMeta> cols_;         // scan后生成的记录的字段
    size_t len_;                        // scan后生成的每条记录的长度
    std::vector<Condition> fed_conds_;  // 同conds_，两个字段相同

    Rid rid_;
    std::unique_ptr<RecScan> scan_;  // table_iterator

    SmManager *sm_manager_;

   public:
    SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab.cols;
        len_ = cols_.back().offset + cols_.back().len;
        context_ = context;
        fed_conds_ = conds_;
    }

    void beginTuple() override {// 用于初始化迭代器并开始扫描
        scan_ = std::make_unique<RmScan>(fh_);  // 创建 RmScan 对象 scan_，用于表的扫描
        rid_ = scan_->rid();

        while (!scan_->is_end()) {
            // 使用 condCheck 函数检查满足条件的记录，并将扫描位置移动到满足条件的下一个记录
            if (condCheck(fh_->get_record(rid_, context_).get())) break;
            scan_->next();
            rid_ = scan_->rid();
        }
    }

    void nextTuple() override {// 用于移动到下一个满足条件的记录
        // 将扫描位置移动到下一个记录，并使用 condCheck 函数检查是否满足条件
        for (scan_->next(); !scan_->is_end(); scan_->next()) {
            rid_ = scan_->rid();
            if (condCheck(fh_->get_record(rid_, context_).get())) break;
        }
    }

    bool condCheck(const RmRecord *l_record) {// 用于检查给定的记录是否满足查询条件
        char *l_val_buf, *r_val_buf;
        const RmRecord *r_record;

        for (auto &condition : conds_) {  // 遍历扫描条件 conds_，对每个条件进行比较
            CompOp op = condition.op;
            int cmp;

            // record和col确定数据位置
            auto l_col = get_col(cols_, condition.lhs_col);  // 左列元数据
            l_val_buf = l_record->data + l_col->offset;      // 确定左数据起点
            //根据条件的左值、右值（值或者列）以及比较操作符，使用 cond_compare 函数比较
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

    // 用于比较两个值或列，返回比较的结果
    int cond_compare(const char *l_val_buf, const char *r_val_buf, ColType type, int col_len) const {
        int cmp = ix_compare(l_val_buf, r_val_buf, type, col_len);
        return cmp;
    }
    // 用于根据比较操作符判断比较结果是否满足条件
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
    // 判断是否到达扫描结束位置
    bool is_end() const override { return scan_->is_end(); }

    // 返回当前扫描位置的记录
    std::unique_ptr<RmRecord> Next() override {
        return fh_->get_record(rid_, context_);
    }
    // 返回当前记录的位置标识 Rid
    Rid &rid() override { return rid_; }
    // 返回 scan 后生成的每条记录的长度
    size_t tupleLen() const override { return len_; };
    // 返回执行器的类型名称
    std::string getType() override { return "SeqScanExecutor"; };
    // 返回执行器生成的记录的字段信息
    const std::vector<ColMeta> &cols() const override { return cols_; };
};

