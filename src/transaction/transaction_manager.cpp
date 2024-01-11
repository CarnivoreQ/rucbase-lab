/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "transaction_manager.h"
#include "record/rm_file_handle.h"
#include "system/sm_manager.h"

std::unordered_map<txn_id_t, Transaction *> TransactionManager::txn_map = {};

/**
 * @description: 事务的开始方法
 * @return {Transaction*} 开始事务的指针
 * @param {Transaction*} txn 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
Transaction * TransactionManager::begin(Transaction* txn, LogManager* log_manager) {
    // Todo:
    // 1. 判断传入事务参数是否为空指针
    // 2. 如果为空指针，创建新事务
    // 3. 把开始事务加入到全局事务表中
    // 4. 返回当前事务指针
    
    if( !txn ) { // 2事务指针为空，创建新事务
        txn = new Transaction(next_txn_id_, IsolationLevel::SERIALIZABLE);
        next_txn_id_ +=1 ;
        txn->set_state(TransactionState::DEFAULT);
    }
    txn_map[txn->get_transaction_id()] = txn; // 3开始事务加入到全局事务表中
    return txn; // 4返回当前事务指针
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::commit(Transaction* txn, LogManager* log_manager) {
    // Todo:
    // 1. 如果存在未提交的写操作，提交所有的写操作
    // 2. 释放所有锁
    // 3. 释放事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态
    if(!txn) return;

    auto write_set = txn->get_write_set();
    while( !write_set->empty() ) // 1 存在未提交的写操作
        write_set->pop_back();//直接pop_back()，似乎不需要提交写操作？

    auto lock_set = txn->get_lock_set();
    for(auto it = lock_set->begin(); it != lock_set->end(); it++ ) // 2
        lock_manager_->unlock(txn, *it);
    lock_set->clear();

    txn->set_state(TransactionState::COMMITTED); // 4

}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 */
void TransactionManager::abort(Transaction * txn, LogManager *log_manager) {
    // Todo:
    // 1. 回滚所有写操作
    // 2. 释放所有锁
    // 3. 清空事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态
    if(!txn) return;

    auto write_set = txn->get_write_set();
    //做反向迭代，直到遍历write_set，使用iterator记录下write_set的内容，然后传给自定义参数
    while(!write_set->empty()) {
        auto context = new Context(lock_manager_, log_manager, txn);
        auto &item = write_set->back();
        auto &type = item->GetWriteType();//写类型
        auto &rid = item->GetRid();//写ID
        auto buf = item->GetRecord().data;//写数据
        auto fh = sm_manager_->fhs_.at(item->GetTableName()).get();
        switch (type) {//匹配情况进行回滚
            case WType::INSERT_TUPLE:
                fh->delete_record(rid, context); break;//rollback_delete
            case WType::DELETE_TUPLE:
                fh->insert_record(buf, context); break;//rollback_insert
            case WType::UPDATE_TUPLE:
                fh->update_record(rid, buf, context); break;//rollback_update
        }
        //回滚完成，将write_set进行pop_back
        write_set->pop_back();
    }
    //3.清空写集
    write_set->clear();
    
    auto lock_set = txn->get_lock_set();
    for(auto it = lock_set->begin(); it != lock_set->end(); it++ ) // 释放所有锁
        lock_manager_->unlock(txn, *it);
    lock_set->clear();

    txn->set_state(TransactionState::ABORTED); //更新事务状态

}