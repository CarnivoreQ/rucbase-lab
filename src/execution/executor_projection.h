/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

/*ProjectionExecutor 通过调用源执行器的接口获取记录，然后按照投影字段的要求构建新的记录。
这样实现了投影操作的功能，只返回被选中的字段*/
#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

// 定义了一个类，继承自 AbstractExecutor 基类，表示投影操作的执行器
class ProjectionExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;        // 投影节点的儿子节点
    std::vector<ColMeta> cols_;                     // 需要投影的字段
    size_t len_;                                    // 字段总长度，即投影后每条记录的长度
    std::vector<size_t> sel_idxs_;                  // 记录在源执行器中需要被投影的字段的索引

   public:
   // 构造函数接受一个源执行器和一个包含被投影字段信息的向量
    ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol> &sel_cols) {
        prev_ = std::move(prev);

        size_t curr_offset = 0;
        auto &prev_cols = prev_->cols();
        // 对于每个被投影字段，调用 get_col 函数获取其在源执行器的索引，并构建新的字段元数据
        for (auto &sel_col : sel_cols) {
            auto pos = get_col(prev_cols, sel_col);
            sel_idxs_.push_back(pos - prev_cols.begin());
            auto col = *pos;
            col.offset = curr_offset;
            curr_offset += col.len;
            cols_.push_back(col);
        }
        len_ = curr_offset;// 计算字段总长度
    }
    // 调用源执行器的 beginTuple 函数开始新的记录的处理
    void beginTuple() override {prev_->beginTuple();}
    // 调用源执行器的 nextTuple 函数移动到下一条记录
    void nextTuple() override {prev_->nextTuple();}

    // 获取投影后的下一条记录
    std::unique_ptr<RmRecord> Next() override {
        auto prev_record = prev_->Next();
        auto rt_record = std::make_unique<RmRecord>(len_);// 构建新的记录 rt_record

        // 遍历被投影字段，逐个复制源记录中对应字段的值到新的记录中
        for (ColMeta col : cols_) {
            TabCol tabcol = {col.tab_name, col.name};
            ColMeta prev_col = *prev_->get_col(prev_->cols(), tabcol);
            memcpy(rt_record->data + col.offset, prev_record->data + prev_col.offset, col.len);
        }

        return rt_record;
    }
    //返回 _abstract_rid，表示当前记录的位置标识
    Rid &rid() override { return _abstract_rid; }
    // 调用源执行器的 is_end 函数判断是否到达末尾
    bool is_end() const override { return prev_->is_end(); };
    //返回字段总长度 len_
    size_t tupleLen() const override { return len_; };
    // 返回执行器的类型名称
    std::string getType() override { return "ProjectionExecutor"; };
    // 返回投影后记录的字段信息
    const std::vector<ColMeta> &cols() const override { return cols_; };
};