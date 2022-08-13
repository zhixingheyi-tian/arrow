// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "gandiva/json_holder.h"

#include <regex>

#include "gandiva/node.h"
#include "gandiva/regex_util.h"
#include <sstream>

using namespace simdjson;

namespace gandiva {

Status JsonHolder::Make(const FunctionNode& node, std::shared_ptr<JsonHolder>* holder) {
  return Make(holder);
}

Status JsonHolder::Make(std::shared_ptr<JsonHolder>* holder) {
  *holder = std::shared_ptr<JsonHolder>(new JsonHolder());
  return Status::OK();
}

error_code handle_types(simdjson_result<ondemand::value> raw_res, std::vector<std::string> fields,
 std::string* res) {
 switch (raw_res.type()) {
   case ondemand::json_type::number: {
      std::stringstream ss;
      double num_res;
      auto error = raw_res.get_double().get(num_res);
      if (!error) {
        ss << num_res;
        *res = ss.str();
      }
      return error;
    }
   case ondemand::json_type::string: {
     std::string_view res_view;
     auto error = raw_res.get_string().get(res_view);
     *res = std::string(res_view);
     return error;
    }
   case ondemand::json_type::boolean: {
     bool bool_res = false;
     raw_res.get_bool().get(bool_res);
     if (bool_res) {
       *res = "true";
     } else {
       *res = "false";
     }
     return error_code::SUCCESS;
    }
   case ondemand::json_type::object: {
     // For nested case, e.g., for "{"my": {"hello": 10}}", ".$my" will return an object type.
     auto obj = raw_res.get_object();
     // For the case that result is a json object.
     if (fields.empty()) {
       std::stringstream ss;
       ss << obj;
       *res = ss.str();
       return error_code::SUCCESS;
     }
     auto inner_result = obj[fields[0]];
     fields.erase(fields.begin());
     return handle_types(inner_result, fields, res);
    }
   case ondemand::json_type::array: {
     // Not supported.
     return error_code::UNSUPPORTED_ARCHITECTURE;
    }
   case ondemand::json_type::null: {
     return error_code::UNSUPPORTED_ARCHITECTURE;
    }
  }
}

const uint8_t* JsonHolder::operator()(gandiva::ExecutionContext* ctx, const std::string& json_str,
 const std::string& json_path, int32_t* out_len) {
  padded_string padded_input(json_str);
  ondemand::parser parser;
  ondemand::document doc;
  try {
    doc = parser.iterate(padded_input);
  } catch (simdjson_error& e) {
    return nullptr;
  }
  if (json_path.length() < 3) {
    return nullptr;
  }
  // Follow spark's format for specifying a field, e.g., ".$a.b".
  auto raw_field_name = json_path.substr(2);
  std::vector<std::string> fields;
  while (raw_field_name.find(".") != std::string::npos) {
    auto ind = raw_field_name.find(".");
    fields.push_back(raw_field_name.substr(0, ind));
    raw_field_name = raw_field_name.substr(ind + 1);
  }
  fields.push_back(raw_field_name);

  // Illegal case.
  if (fields.size() < 1) {
    return nullptr;
  }

  auto raw_res = doc.find_field(fields[0]);
  error_code error;
  std::string res;
  fields.erase(fields.begin());
  try {
    error = handle_types(raw_res, fields, &res);
  } catch(...) {
    return nullptr;
  }
  if (error) {
   return nullptr;
  }

  *out_len = res.length();
  if (*out_len == 0) {
    return reinterpret_cast<const uint8_t*>("");
  }
  uint8_t* result_buffer = reinterpret_cast<uint8_t*>(ctx->arena()->Allocate(*out_len));
  memcpy(result_buffer, res.data(), *out_len);
  return result_buffer;
}

}  // namespace gandiva
