#pragma once

#include <string>

class Engine {
public:
  virtual ~Engine() = default;

  virtual void Put(const std::string& key, const std::string& value) = 0;
  virtual void Scan(const std::string& start_key, const std::string& end_key) = 0;

  virtual std::string GetName() = 0;
  virtual std::string GetActiveBackend() { return GetName(); }
};
