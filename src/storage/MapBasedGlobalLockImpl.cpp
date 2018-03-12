#include "MapBasedGlobalLockImpl.h"

#include <mutex>

namespace Afina {
namespace Backend {

// See MapBasedGlobalLockImpl.h
bool MapBasedGlobalLockImpl::Put(const std::string &key, const std::string &value) {
	std::lock_guard<std::mutex> lock(_mutex);

	size_t record_size = key.size() + value.size();
	if(record_size > _max_size)
		return false;
	auto exists = _backend.find(key);
	if(exists == _backend.end()){
		_curr_size += record_size;
		
		_priority.push_front(key);
		DeleteOldData(key);

		auto inserted = _backend.insert(std::pair<std::string, std::string>(key, value));
		if(!inserted.second){
			_curr_size -= record_size;
			//wtf
			throw std::underflow_error("in priority exist but int backend not");
		}

	} else {
		_curr_size -= exists->second.size();
		_curr_size += value.size();

		_priority.remove(key);
		_priority.push_front(key);
		DeleteOldData(key);

		exists->second = value;

	}
	return true; 
}

// See MapBasedGlobalLockImpl.h
bool MapBasedGlobalLockImpl::PutIfAbsent(const std::string &key, const std::string &value) { 
	std::lock_guard<std::mutex> lock(_mutex);

	size_t record_size = key.size() + value.size();
	if(record_size > _max_size)
		return false;
	auto exists = _backend.find(key);
	if(exists != _backend.end()){
		return false;
	}
	_curr_size += record_size;

	_priority.push_front(key);
	DeleteOldData(key);
	
	auto inserted = _backend.insert(std::pair<std::string, std::string>(key, value));
	if(!inserted.second){
		_curr_size -= record_size;
		//wtf
		throw std::underflow_error("in priority exist but int backend not");
	}

	return inserted.second;
}

// See MapBasedGlobalLockImpl.h
bool MapBasedGlobalLockImpl::Set(const std::string &key, const std::string &value) {
	std::lock_guard<std::mutex> lock(_mutex);

	auto position = _backend.find(key); 
	if(position != _backend.end()){
		_curr_size -= position->second.size();
		_curr_size += value.size();

		_priority.remove(key);
		_priority.push_front(key);
		DeleteOldData(key); 

		position->second = value;

		return true;
	} else {
		return false;
	}
}

// See MapBasedGlobalLockImpl.h
bool MapBasedGlobalLockImpl::Delete(const std::string &key) {
	std::lock_guard<std::mutex> lock(_mutex);

	auto position = _backend.find(key); 
	if(position != _backend.end()){
		_curr_size -= position->second.size() + key.size();

		_priority.remove(key);

		_backend.erase(key);
		return true;
	} else {
		return false;
	} 
}

// See MapBasedGlobalLockImpl.h
bool MapBasedGlobalLockImpl::Get(const std::string &key, std::string &value) const {
	//std::lock_guard<std::mutex> lock(_mutex);

	auto position = _backend.find(key); 
	if(position != _backend.end()){

		_priority.remove(key);//
		_priority.push_front(key);
		
		value = position->second;
		return true;
	} else {
		return false;
	}
}

// See MapBasedGlobalLockImpl.h
void MapBasedGlobalLockImpl::DeleteOldData(const std::string &key){
	while(_curr_size > _max_size){
		if(	_priority.back() == key){

			_priority.pop_back();
			_priority.push_front(key);

			continue;
		}
		auto exists = _backend.find(_priority.back());
		if(exists != _backend.end()){
			size_t record_size = _priority.back().size() + exists->second.size();
			_curr_size -= record_size;
			
			_backend.erase(_priority.back());
			_priority.pop_back();
			
		}else{
			throw std::underflow_error("in priority exist but int backend not");
		}
	}
}


} // namespace Backend
} // namespace Afina
