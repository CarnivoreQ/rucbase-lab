/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "lock_manager.h"
//找锁类型
#define group_mode(id) lock_table_[id].group_lock_mode_
/**
 * @description: 申请行级共享锁/行级读锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID 记录所在的表的fd
 * @param {int} tab_fd
 */
bool LockManager::lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
     // Todo:
    // 1. 通过mutex申请访问全局锁表
    // 2. 检查事务的状态
    // 3. 查找当前事务是否已经申请了目标数据项上的锁，如果存在则根据锁类型进行操作，否则执行下一步操作
    // 4. 将要申请的锁放入到全局锁表中，并通过组模式来判断是否可以成功授予锁
    // 5. 如果成功，更新目标数据项在全局锁表中的信息，否则阻塞当前操作
    // 提示：步骤5中的阻塞操作可以通过条件变量来完成，所有加锁操作都遵循上述步骤，在下面的加锁操作中不再进行注释提示

    std::unique_lock<std::mutex> lock{latch_}; // 1

    if( txn->get_isolation_level()==IsolationLevel::READ_UNCOMMITTED || // 2
        txn->get_state()==TransactionState::SHRINKING
    )
        txn->set_state(TransactionState::ABORTED);
    if(txn->get_state()==TransactionState::ABORTED)
        return false;

    txn->set_state(TransactionState::GROWING);
    LockDataId newid(tab_fd, rid, LockDataType::RECORD);

    if (txn->get_lock_set()->find(newid)!=txn->get_lock_set()->end()) { // 3
        for (auto i=lock_table_[newid].request_queue_.begin(); i!=lock_table_[newid].request_queue_.end(); i++)
            if (i->txn_id_ == txn->get_transaction_id())
                lock_table_[newid].cv_.notify_all();
        lock.unlock();
        return true;
    }
    txn->get_lock_set()->insert(newid); // 4.1 put into lock_table
    LockRequest *newquest = new LockRequest(txn->get_transaction_id(), LockMode::SHARED);
    lock_table_[newid].request_queue_.push_back(*newquest);
    lock_table_[newid].shared_lock_num_++;

    while( group_mode(newid) != GroupLockMode::S &&
           group_mode(newid) != GroupLockMode::IS &&
           group_mode(newid) != GroupLockMode::NON_LOCK ) // 4.2&5 group mode judgement
        lock_table_[newid].cv_.wait(lock);
    newquest->granted_ = true;
    lock_table_[newid].cv_.notify_all();
    lock.unlock();
    return true;
}

/**
 * @description: 申请行级排他锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID
 * @param {int} tab_fd 记录所在的表的fd
 */
bool LockManager::lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    std::unique_lock<std::mutex> lock{latch_}; // 1

    if(  txn->get_state()==TransactionState::SHRINKING ) // 2
        txn->set_state(TransactionState::ABORTED);
    if(txn->get_state()==TransactionState::ABORTED)
        return false;

    txn->set_state(TransactionState::GROWING);
    LockDataId newid(tab_fd, rid, LockDataType::RECORD);

    if (txn->get_lock_set()->find(newid)!=txn->get_lock_set()->end()) { // 3
        for (auto i=lock_table_[newid].request_queue_.begin(); i!=lock_table_[newid].request_queue_.end(); i++)
            if (i->txn_id_ == txn->get_transaction_id()) {
                group_mode(newid) = GroupLockMode::X;
                lock_table_[newid].cv_.notify_all();
            }
        lock.unlock();
        return true;
    }
    txn->get_lock_set()->insert(newid); // 4.1 put into lock_table
    LockRequest *newquest = new LockRequest(txn->get_transaction_id(), LockMode::SHARED);
    lock_table_[newid].request_queue_.push_back(*newquest);
    lock_table_[newid].shared_lock_num_++;

    while(group_mode(newid)!= GroupLockMode::NON_LOCK) // 4.2&5 group mode judgement
        lock_table_[newid].cv_.wait(lock);
    newquest->granted_ = true;
    group_mode(newid) = GroupLockMode::X;
    lock_table_[newid].cv_.notify_all();
    lock.unlock();
    return true;
}

/**
 * @description: 申请表级读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_shared_on_table(Transaction* txn, int tab_fd) {
    std::unique_lock<std::mutex> lock{latch_};

    if( txn->get_isolation_level()==IsolationLevel::READ_UNCOMMITTED ||
        txn->get_state()==TransactionState::SHRINKING
    )
        txn->set_state(TransactionState::ABORTED);
    if(txn->get_state()==TransactionState::ABORTED)
        return false;

    txn->set_state(TransactionState::GROWING);
    LockDataId newid(tab_fd, LockDataType::TABLE);

    if (txn->get_lock_set()->find(newid)!=txn->get_lock_set()->end()) {
        for (auto i=lock_table_[newid].request_queue_.begin(); i!=lock_table_[newid].request_queue_.end(); i++)
            if (i->txn_id_ == txn->get_transaction_id()) {
                if (group_mode(newid) == GroupLockMode::IX)
                    group_mode(newid) = GroupLockMode::SIX;
                else if (group_mode(newid)==GroupLockMode::IS || group_mode(newid)==GroupLockMode::NON_LOCK)
                    group_mode(newid) = GroupLockMode::S;
                lock_table_[newid].cv_.notify_all();
            }
        lock.unlock();
        return true;
    }
    txn->get_lock_set()->insert(newid);
    LockRequest *newquest = new LockRequest(txn->get_transaction_id(), LockMode::SHARED);
    lock_table_[newid].request_queue_.push_back(*newquest);
    lock_table_[newid].shared_lock_num_++;

    while( group_mode(newid)!=GroupLockMode::S &&
           group_mode(newid)!=GroupLockMode::IS &&
           group_mode(newid)!=GroupLockMode::NON_LOCK)
        lock_table_[newid].cv_.wait(lock);
    newquest->granted_ = true;
    if (group_mode(newid) == GroupLockMode::IX)
        group_mode(newid) = GroupLockMode::SIX;
    else if (group_mode(newid)==GroupLockMode::IS || group_mode(newid)==GroupLockMode::NON_LOCK)
        group_mode(newid) = GroupLockMode::S;
    lock_table_[newid].cv_.notify_all();
    lock.unlock();
    return true;
}

/**
 * @description: 申请表级写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_exclusive_on_table(Transaction* txn, int tab_fd) {
    std::unique_lock<std::mutex> lock{latch_}; // 1

    if(  txn->get_state()==TransactionState::SHRINKING ) // 2
        txn->set_state(TransactionState::ABORTED);
    if(txn->get_state()==TransactionState::ABORTED)
        return false;

    txn->set_state(TransactionState::GROWING);
    LockDataId newid(tab_fd, LockDataType::TABLE);

    if (txn->get_lock_set()->find(newid)!=txn->get_lock_set()->end()) { // 3
        for (auto i=lock_table_[newid].request_queue_.begin(); i!=lock_table_[newid].request_queue_.end(); i++)
            if (i->txn_id_ == txn->get_transaction_id()) {
                group_mode(newid) = GroupLockMode::X;
                lock_table_[newid].cv_.notify_all();
            }
        lock.unlock();
        return true;
    }
    txn->get_lock_set()->insert(newid); // 4.1 put into lock_table
    LockRequest *newquest = new LockRequest(txn->get_transaction_id(), LockMode::SHARED);
    lock_table_[newid].request_queue_.push_back(*newquest);
    lock_table_[newid].shared_lock_num_++;

    while(group_mode(newid)!=GroupLockMode::NON_LOCK) // 4.2&5 group mode judgement
        lock_table_[newid].cv_.wait(lock);
    newquest->granted_ = true;
    group_mode(newid) = GroupLockMode::X;
    lock_table_[newid].cv_.notify_all();
    lock.unlock();
    return true;
}

/**
 * @description: 申请表级意向读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IS_on_table(Transaction* txn, int tab_fd) {
    std::unique_lock<std::mutex> lock{latch_}; // 1

    if( txn->get_isolation_level()==IsolationLevel::READ_UNCOMMITTED || // 2
        txn->get_state()==TransactionState::SHRINKING
    )
        txn->set_state(TransactionState::ABORTED);
    if(txn->get_state()==TransactionState::ABORTED)
        return false;

    txn->set_state(TransactionState::GROWING);
    LockDataId newid(tab_fd, LockDataType::TABLE);

    if (txn->get_lock_set()->find(newid)!=txn->get_lock_set()->end()) { // 3
        for (auto i=lock_table_[newid].request_queue_.begin(); i!=lock_table_[newid].request_queue_.end(); i++)
            if (i->txn_id_ == txn->get_transaction_id()) {
                if( group_mode(newid)==GroupLockMode::NON_LOCK)
                    group_mode(newid) = GroupLockMode::IS;
                lock_table_[newid].cv_.notify_all();
            }
        lock.unlock();
        return true;
    }
    txn->get_lock_set()->insert(newid); // 4.1 put into lock_table
    LockRequest *newquest = new LockRequest(txn->get_transaction_id(), LockMode::SHARED);
    lock_table_[newid].request_queue_.push_back(*newquest);
    lock_table_[newid].shared_lock_num_++;

    while(group_mode(newid)==GroupLockMode::X) // 4.2&5 group mode judgement
        lock_table_[newid].cv_.wait(lock);
    newquest->granted_ = true;
    if( group_mode(newid)==GroupLockMode::NON_LOCK)
        group_mode(newid) = GroupLockMode::IS;
    lock_table_[newid].cv_.notify_all();
    lock.unlock();
    return true;

}

/**
 * @description: 申请表级意向写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IX_on_table(Transaction* txn, int tab_fd) {
    std::unique_lock<std::mutex> lock{latch_}; // 1

    if(  txn->get_state()==TransactionState::SHRINKING ) // 2
        txn->set_state(TransactionState::ABORTED);
    if(txn->get_state()==TransactionState::ABORTED)
        return false;

    txn->set_state(TransactionState::GROWING);
    LockDataId newid(tab_fd, LockDataType::TABLE);

    if (txn->get_lock_set()->find(newid)!=txn->get_lock_set()->end()) { // 3
        for (auto i=lock_table_[newid].request_queue_.begin(); i!=lock_table_[newid].request_queue_.end(); i++)
            if (i->txn_id_ == txn->get_transaction_id()) {
                if(group_mode(newid)==GroupLockMode::S)
                    group_mode(newid) = GroupLockMode::SIX;
                else if(group_mode(newid)==GroupLockMode::IS || group_mode(newid)==GroupLockMode::NON_LOCK)
                    group_mode(newid) = GroupLockMode::IX;
                lock_table_[newid].cv_.notify_all();
            }
        lock.unlock();
        return true;
    }
    txn->get_lock_set()->insert(newid); // 4.1 put into lock_table
    LockRequest *newquest = new LockRequest(txn->get_transaction_id(), LockMode::SHARED);
    lock_table_[newid].request_queue_.push_back(*newquest);
    lock_table_[newid].shared_lock_num_++;

    while(group_mode(newid)==GroupLockMode::X &&
           group_mode(newid)==GroupLockMode::S &&
           group_mode(newid)==GroupLockMode::SIX) // 4.2&5 group mode judgement
        lock_table_[newid].cv_.wait(lock);
    newquest->granted_ = true;
    if(group_mode(newid)==GroupLockMode::S)
        group_mode(newid) = GroupLockMode::SIX;
    else if(group_mode(newid)==GroupLockMode::IS || group_mode(newid)==GroupLockMode::NON_LOCK)
        group_mode(newid) = GroupLockMode::IX;
    lock_table_[newid].cv_.notify_all();
    lock.unlock();
    return true;
}

/**
 * @description: 释放锁
 * @return {bool} 返回解锁是否成功
 * @param {Transaction*} txn 要释放锁的事务对象指针
 * @param {LockDataId} lock_data_id 要释放的锁ID
 */
bool LockManager::unlock(Transaction* txn, LockDataId lock_data_id) {
   
    std::unique_lock<std::mutex> lock(latch_);

    txn->set_state(TransactionState::SHRINKING); //将事务状态设置为收缩期
    if (txn->get_lock_set()->find(lock_data_id) == txn->get_lock_set()->end())
        return false; //未找到该锁
    auto it = lock_table_[lock_data_id].request_queue_.begin();
    while (it != lock_table_[lock_data_id].request_queue_.end()) {
        if (it->txn_id_ == txn->get_transaction_id()){
            it = lock_table_[lock_data_id].request_queue_.erase(it); 
        }
        else 
            it++;
    }
    //修改后的组模式
    GroupLockMode mode = GroupLockMode::NON_LOCK;
    //遍历queue
    for (it = lock_table_[lock_data_id].request_queue_.begin();it != lock_table_[lock_data_id].request_queue_.end(); ++it) {
            if (it->granted_ == true) {
                if (it->lock_mode_ == LockMode::EXLUCSIVE) {
                    mode = GroupLockMode::X;
                    break;

                } else if (it->lock_mode_ == LockMode::S_IX) {
                    mode = GroupLockMode::SIX;
                
                } else if (it->lock_mode_ == LockMode::SHARED && mode != GroupLockMode::SIX) {
                    if (mode == GroupLockMode::IX)
                        mode = GroupLockMode::SIX;
                    else
                        mode = GroupLockMode::S;

                } else if (it->lock_mode_ == LockMode::INTENTION_EXCLUSIVE && mode != GroupLockMode::SIX) {
                    if (mode == GroupLockMode::S)
                        mode = GroupLockMode::SIX;
                    else
                        mode = GroupLockMode::IX;

                } else if (it->lock_mode_ == LockMode::INTENTION_SHARED &&
                           (mode == GroupLockMode::NON_LOCK || mode == GroupLockMode::IS)) {
                    mode = GroupLockMode::IS;
                }
            }
        }
    
    lock_table_[lock_data_id].group_lock_mode_ = mode;
    lock_table_[lock_data_id].cv_.notify_all();
    return true;
}