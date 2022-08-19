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

#include "gandiva/gdv_function_stubs.h"

#include <string>
#include <vector>

#include "arrow/util/formatting.h"
#include "arrow/util/value_parsing.h"
#include "gandiva/engine.h"
#include "gandiva/exported_funcs.h"
#include "gandiva/formatting_utils.h"
#include "gandiva/hash_utils.h"
#include "gandiva/in_holder.h"
#include "gandiva/json_holder.h"
#include "gandiva/like_holder.h"
#include "gandiva/precompiled/types.h"
#include "gandiva/random_generator_holder.h"
#include "gandiva/replace_holder.h"
#include "gandiva/rlike_holder.h"
#include "gandiva/extract_holder.h"
#include "gandiva/parse_url_holder.h"
#include "gandiva/to_date_holder.h"
#include "gandiva/translate_holder.h"
#include "gandiva/substr_index_holder.h"

/// Stub functions that can be accessed from LLVM or the pre-compiled library.

extern "C" {

const uint8_t* gdv_fn_get_json_object_utf8_utf8(int64_t ptr, int64_t holder_ptr, const char* data, int data_len, bool in1_valid,
                           const char* pattern, int pattern_len, bool in2_valid, bool* out_valid, int32_t* out_len) {
  if (!in1_valid || !in2_valid) {
    *out_valid = false;
    *out_len = 0;
    return reinterpret_cast<const uint8_t*>("");
  }  
  gandiva::ExecutionContext* context = reinterpret_cast<gandiva::ExecutionContext*>(ptr);
  gandiva::JsonHolder* holder = reinterpret_cast<gandiva::JsonHolder*>(holder_ptr);
  auto res = (*holder)(context, std::string(data, data_len), std::string(pattern, pattern_len), out_len);
  if (res == nullptr) {
    *out_valid = false;
    *out_len = 0;
    return reinterpret_cast<const uint8_t*>("");
  }
  *out_valid = true;
  return res;
}

const uint8_t* gdv_fn_translate_utf8_utf8_utf8(int64_t ptr, int64_t holder_ptr, const char* text, 
                                               int text_len, const char* matching_str,
                                               int matching_str_len, const char* replace_str,
                                               int replace_str_len, int32_t* out_len) {
  gandiva::ExecutionContext* context = reinterpret_cast<gandiva::ExecutionContext*>(ptr);
  gandiva::TranslateHolder* holder = reinterpret_cast<gandiva::TranslateHolder*>(holder_ptr);
  auto res = (*holder)(context, std::string(text, text_len), std::string(matching_str, matching_str_len),
              std::string(replace_str, replace_str_len), out_len);
  return res;
}

const char* gdv_fn_substr_index_utf8_utf8_int32(int64_t ptr, int64_t holder_ptr, const char* input, int in_len,
                                         const char* delim, int delim_len, int count, int32_t* out_len) {
  gandiva::ExecutionContext* context = reinterpret_cast<gandiva::ExecutionContext*>(ptr);
  gandiva::SubstrIndexHolder* holder = reinterpret_cast<gandiva::SubstrIndexHolder*>(holder_ptr);
  auto res = (*holder)(context, std::string(input, in_len), std::string(delim, delim_len), count, out_len);
  return res;
}

bool gdv_fn_rlike_utf8_utf8(int64_t ptr, const char* data, int data_len,
                           const char* pattern, int pattern_len) {
  gandiva::RLikeHolder* holder = reinterpret_cast<gandiva::RLikeHolder*>(ptr);
  return (*holder)(std::string(data, data_len));
}

bool gdv_fn_like_utf8_utf8(int64_t ptr, const char* data, int data_len,
                           const char* pattern, int pattern_len) {
  gandiva::LikeHolder* holder = reinterpret_cast<gandiva::LikeHolder*>(ptr);
  return (*holder)(std::string(data, data_len));
}

bool gdv_fn_like_utf8_utf8_utf8(int64_t ptr, const char* data, int data_len,
                                const char* pattern, int pattern_len,
                                const char* escape_char, int escape_char_len) {
  gandiva::LikeHolder* holder = reinterpret_cast<gandiva::LikeHolder*>(ptr);
  return (*holder)(std::string(data, data_len));
}

bool gdv_fn_ilike_utf8_utf8(int64_t ptr, const char* data, int data_len,
                            const char* pattern, int pattern_len) {
  gandiva::LikeHolder* holder = reinterpret_cast<gandiva::LikeHolder*>(ptr);
  return (*holder)(std::string(data, data_len));
}

const char* gdv_fn_regexp_replace_utf8_utf8(
    int64_t ptr, int64_t holder_ptr, const char* data, int32_t data_len,
    const char* /*pattern*/, int32_t /*pattern_len*/, const char* replace_string,
    int32_t replace_string_len, int32_t* out_length) {
  gandiva::ExecutionContext* context = reinterpret_cast<gandiva::ExecutionContext*>(ptr);

  gandiva::ReplaceHolder* holder = reinterpret_cast<gandiva::ReplaceHolder*>(holder_ptr);

  return (*holder)(context, data, data_len, replace_string, replace_string_len,
                   out_length);
}

const char* gdv_fn_regexp_extract_utf8_utf8_int32(
    int64_t ptr, int64_t holder_ptr, const char* data, int32_t data_len,
    const char* /*pattern*/, int32_t /*pattern_len*/, const int32_t idx, int32_t* out_length) {
  gandiva::ExecutionContext* context = reinterpret_cast<gandiva::ExecutionContext*>(ptr);
  gandiva::ExtractHolder* holder = reinterpret_cast<gandiva::ExtractHolder*>(holder_ptr);
  return (*holder)(context, data, data_len, idx, out_length);
}

const char* gdv_fn_parse_url_utf8_utf8(
    int64_t ptr, int64_t holder_ptr, const char* data, int32_t data_len, bool in1_valid,
    const char* part, int32_t part_len, bool in2_valid, bool* out_valid, int32_t* out_length) {
  if (!in1_valid || !in2_valid) {
    *out_valid = false;
    *out_length = 0;
    return reinterpret_cast<const char*>("");
  }
  gandiva::ExecutionContext* context = reinterpret_cast<gandiva::ExecutionContext*>(ptr);
  gandiva::ParseUrlHolder* holder = reinterpret_cast<gandiva::ParseUrlHolder*>(holder_ptr);
  auto res = (*holder)(context, data, data_len, part, part_len, out_length);
  if (res == nullptr) {
    *out_valid = false;
    *out_length = 0;
    return reinterpret_cast<const char*>("");
  }
  *out_valid = true;
  return res;
}

const char* gdv_fn_parse_url_utf8_utf8_utf8(
    int64_t ptr, int64_t holder_ptr, const char* data, int32_t data_len, bool in1_valid,
    const char* part, int32_t part_len, bool in2_valid,
    const char* pattern, int32_t pattern_len, bool in3_valid,
    bool* out_valid, int32_t* out_length) {
  if (!in1_valid || !in2_valid || !in3_valid) {
    *out_valid = false;
    *out_length = 0;
    return reinterpret_cast<const char*>("");
  }
  gandiva::ExecutionContext* context = reinterpret_cast<gandiva::ExecutionContext*>(ptr);
  gandiva::ParseUrlHolder* holder = reinterpret_cast<gandiva::ParseUrlHolder*>(holder_ptr);
  auto res = (*holder)(context, data, data_len,  part, part_len, pattern, pattern_len, out_length);
  if (res == nullptr) {
    *out_valid = false;
    *out_length = 0;
    return reinterpret_cast<const char*>("");
  }
  *out_valid = true;
  return res;
}

double gdv_fn_random(int64_t ptr) {
  gandiva::RandomGeneratorHolder* holder =
      reinterpret_cast<gandiva::RandomGeneratorHolder*>(ptr);
  return (*holder)();
}

double gdv_fn_random_with_seed(int64_t ptr, int32_t seed, bool seed_validity) {
  gandiva::RandomGeneratorHolder* holder =
      reinterpret_cast<gandiva::RandomGeneratorHolder*>(ptr);
  return (*holder)();
}

double gdv_fn_random_with_seed64(int64_t ptr, int64_t seed, bool seed_validity) {
  gandiva::RandomGeneratorHolder* holder =
      reinterpret_cast<gandiva::RandomGeneratorHolder*>(ptr);
  return (*holder)();
}

double gdv_fn_random_with_seed64_offset(int64_t ptr, int64_t seed, bool seed_validity, 
                                        int32_t offset, bool offset_validity) {
  gandiva::RandomGeneratorHolder* holder =
      reinterpret_cast<gandiva::RandomGeneratorHolder*>(ptr);
  return (*holder)(); 
}

int64_t gdv_fn_to_date_utf8_utf8(int64_t context_ptr, int64_t holder_ptr,
                                 const char* data, int data_len, bool in1_validity,
                                 const char* pattern, int pattern_len, bool in2_validity,
                                 bool* out_valid) {
  gandiva::ExecutionContext* context =
      reinterpret_cast<gandiva::ExecutionContext*>(context_ptr);
  gandiva::ToDateHolder* holder = reinterpret_cast<gandiva::ToDateHolder*>(holder_ptr);
  return (*holder)(context, data, data_len, in1_validity, out_valid);
}

int64_t gdv_fn_to_date_utf8_utf8_int32(int64_t context_ptr, int64_t holder_ptr,
                                       const char* data, int data_len, bool in1_validity,
                                       const char* pattern, int pattern_len,
                                       bool in2_validity, int32_t suppress_errors,
                                       bool in3_validity, bool* out_valid) {
  gandiva::ExecutionContext* context =
      reinterpret_cast<gandiva::ExecutionContext*>(context_ptr);
  gandiva::ToDateHolder* holder = reinterpret_cast<gandiva::ToDateHolder*>(holder_ptr);
  return (*holder)(context, data, data_len, in1_validity, out_valid);
}

bool gdv_fn_in_expr_lookup_int32(int64_t ptr, int32_t value, bool in_validity) {
  if (!in_validity) {
    return false;
  }
  gandiva::InHolder<int32_t>* holder = reinterpret_cast<gandiva::InHolder<int32_t>*>(ptr);
  return holder->HasValue(value);
}

bool gdv_fn_in_expr_lookup_int64(int64_t ptr, int64_t value, bool in_validity) {
  if (!in_validity) {
    return false;
  }
  gandiva::InHolder<int64_t>* holder = reinterpret_cast<gandiva::InHolder<int64_t>*>(ptr);
  return holder->HasValue(value);
}

bool gdv_fn_in_expr_lookup_decimal(int64_t ptr, int64_t value_high, int64_t value_low,
                                   int32_t precision, int32_t scale, bool in_validity) {
  if (!in_validity) {
    return false;
  }
  gandiva::DecimalScalar128 value(value_high, value_low, precision, scale);
  gandiva::InHolder<gandiva::DecimalScalar128>* holder =
      reinterpret_cast<gandiva::InHolder<gandiva::DecimalScalar128>*>(ptr);
  return holder->HasValue(value);
}

bool gdv_fn_in_expr_lookup_utf8(int64_t ptr, const char* data, int data_len,
                                bool in_validity) {
  if (!in_validity) {
    return false;
  }
  gandiva::InHolder<std::string>* holder =
      reinterpret_cast<gandiva::InHolder<std::string>*>(ptr);
  return holder->HasValue(arrow::util::string_view(data, data_len));
}

int32_t gdv_fn_populate_varlen_vector(int64_t context_ptr, int8_t* data_ptr,
                                      int32_t* offsets, int64_t slot,
                                      const char* entry_buf, int32_t entry_len) {
  auto buffer = reinterpret_cast<arrow::ResizableBuffer*>(data_ptr);
  int32_t offset = static_cast<int32_t>(buffer->size());

  // This also sets the size in the buffer.
  auto status = buffer->Resize(offset + entry_len, false /*shrink*/);
  if (!status.ok()) {
    gandiva::ExecutionContext* context =
        reinterpret_cast<gandiva::ExecutionContext*>(context_ptr);

    context->set_error_msg(status.message().c_str());
    return -1;
  }

  // append the new entry.
  memcpy(buffer->mutable_data() + offset, entry_buf, entry_len);

  // update offsets buffer.
  offsets[slot] = offset;
  offsets[slot + 1] = offset + entry_len;
  return 0;
}

#define MD5_HASH_FUNCTION(TYPE)                                                    \
  GANDIVA_EXPORT                                                                   \
  const char* gdv_fn_md5_##TYPE(int64_t context, gdv_##TYPE value, bool validity,  \
                                int32_t* out_length) {                             \
    if (!validity) {                                                               \
      return gandiva::gdv_md5_hash(context, NULLPTR, 0, out_length);               \
    }                                                                              \
    auto value_as_long = gandiva::gdv_double_to_long((double)value);               \
    const char* result = gandiva::gdv_md5_hash(context, &value_as_long,            \
                                               sizeof(value_as_long), out_length); \
                                                                                   \
    return result;                                                                 \
  }

#define MD5_HASH_FUNCTION_BUF(TYPE)                                                      \
  GANDIVA_EXPORT                                                                         \
  const char* gdv_fn_md5_##TYPE(int64_t context, gdv_##TYPE value, int32_t value_length, \
                                bool value_validity, int32_t* out_length) {              \
    if (!value_validity) {                                                               \
      return gandiva::gdv_md5_hash(context, NULLPTR, 0, out_length);                     \
    }                                                                                    \
    return gandiva::gdv_md5_hash(context, value, value_length, out_length);              \
  }

#define SHA1_HASH_FUNCTION(TYPE)                                                    \
  GANDIVA_EXPORT                                                                    \
  const char* gdv_fn_sha1_##TYPE(int64_t context, gdv_##TYPE value, bool validity,  \
                                 int32_t* out_length) {                             \
    if (!validity) {                                                                \
      return gandiva::gdv_sha1_hash(context, NULLPTR, 0, out_length);               \
    }                                                                               \
    auto value_as_long = gandiva::gdv_double_to_long((double)value);                \
    const char* result = gandiva::gdv_sha1_hash(context, &value_as_long,            \
                                                sizeof(value_as_long), out_length); \
                                                                                    \
    return result;                                                                  \
  }

#define SHA1_HASH_FUNCTION_BUF(TYPE)                                         \
  GANDIVA_EXPORT                                                             \
  const char* gdv_fn_sha1_##TYPE(int64_t context, gdv_##TYPE value,          \
                                 int32_t value_length, bool value_validity,  \
                                 int32_t* out_length) {                      \
    if (!value_validity) {                                                   \
      return gandiva::gdv_sha1_hash(context, NULLPTR, 0, out_length);        \
    }                                                                        \
    return gandiva::gdv_sha1_hash(context, value, value_length, out_length); \
  }

#define SHA2_HASH_FUNCTION(TYPE)                                                           \
  GANDIVA_EXPORT                                                                           \
  const char* gdv_fn_sha2_##TYPE##_int32(int64_t context, gdv_##TYPE value, bool validity, \
                                 int32_t bits_length, bool bits_len_validity,              \
                                 int32_t* out_length) {                                    \
    if (!bits_len_validity) {                                                              \
      gdv_fn_context_set_error_msg(context, "The bits length should be specified!");       \
      *out_length = 0;                                                                     \
      return "";                                                                           \
    }                                                                                      \
    if (!validity) {                                                                       \
      return gandiva::gdv_sha2_hash(context, NULLPTR, 0, bits_length, out_length);         \
    }                                                                                      \
    auto value_as_long = gandiva::gdv_double_to_long((double)value);                       \
    const char* result = gandiva::gdv_sha2_hash(context, &value_as_long,                   \
                                                sizeof(value_as_long),                     \
                                                bits_length, out_length);                  \
    return result;                                                                         \
  }

// For string/binary type.
#define SHA2_HASH_FUNCTION_BUF(TYPE)                                                      \
  GANDIVA_EXPORT                                                                          \
  const char* gdv_fn_sha2_##TYPE##_int32(int64_t context, gdv_##TYPE value,               \
                                 int32_t value_length, bool value_validity,               \
                                 int32_t bits_length, bool bits_len_validity,             \
                                 int32_t* out_length) {                                   \
    if (!bits_len_validity) {                                                             \
      gdv_fn_context_set_error_msg(context, "The bits length should be specified!");      \
      *out_length = 0;                                                                    \
      return "";                                                                          \
    }                                                                                     \
    if (!value_validity) {                                                                \
      return gandiva::gdv_sha2_hash(context, NULLPTR, 0, bits_length, out_length);        \
    }                                                                                     \
                                                                                          \
    return gandiva::gdv_sha2_hash(context, value, value_length, bits_length, out_length); \
  }

#define SHA256_HASH_FUNCTION(TYPE)                                                    \
  GANDIVA_EXPORT                                                                      \
  const char* gdv_fn_sha256_##TYPE(int64_t context, gdv_##TYPE value, bool validity,  \
                                   int32_t* out_length) {                             \
    if (!validity) {                                                                  \
      return gandiva::gdv_sha256_hash(context, NULLPTR, 0, out_length);               \
    }                                                                                 \
    auto value_as_long = gandiva::gdv_double_to_long((double)value);                  \
    const char* result = gandiva::gdv_sha256_hash(context, &value_as_long,            \
                                                  sizeof(value_as_long), out_length); \
    return result;                                                                    \
  }

#define SHA256_HASH_FUNCTION_BUF(TYPE)                                         \
  GANDIVA_EXPORT                                                               \
  const char* gdv_fn_sha256_##TYPE(int64_t context, gdv_##TYPE value,          \
                                   int32_t value_length, bool value_validity,  \
                                   int32_t* out_length) {                      \
    if (!value_validity) {                                                     \
      return gandiva::gdv_sha256_hash(context, NULLPTR, 0, out_length);        \
    }                                                                          \
                                                                               \
    return gandiva::gdv_sha256_hash(context, value, value_length, out_length); \
  }

// Expand inner macro for all numeric types.
#define SHA_NUMERIC_BOOL_DATE_PARAMS(INNER) \
  INNER(int8)                               \
  INNER(int16)                              \
  INNER(int32)                              \
  INNER(int64)                              \
  INNER(uint8)                              \
  INNER(uint16)                             \
  INNER(uint32)                             \
  INNER(uint64)                             \
  INNER(float32)                            \
  INNER(float64)                            \
  INNER(boolean)                            \
  INNER(date64)                             \
  INNER(date32)                             \
  INNER(time32)                             \
  INNER(timestamp)                          \
  INNER(timestampusutc)

// Expand inner macro for all numeric types.
#define SHA_VAR_LEN_PARAMS(INNER) \
  INNER(utf8)                     \
  INNER(binary)

SHA_NUMERIC_BOOL_DATE_PARAMS(MD5_HASH_FUNCTION)
SHA_VAR_LEN_PARAMS(MD5_HASH_FUNCTION_BUF)

SHA_NUMERIC_BOOL_DATE_PARAMS(SHA2_HASH_FUNCTION)
// For string/binary type.
SHA_VAR_LEN_PARAMS(SHA2_HASH_FUNCTION_BUF)
SHA_NUMERIC_BOOL_DATE_PARAMS(SHA256_HASH_FUNCTION)
SHA_VAR_LEN_PARAMS(SHA256_HASH_FUNCTION_BUF)

SHA_NUMERIC_BOOL_DATE_PARAMS(SHA1_HASH_FUNCTION)
SHA_VAR_LEN_PARAMS(SHA1_HASH_FUNCTION_BUF)

#undef SHA_NUMERIC_BOOL_DATE_PARAMS
#undef SHA_VAR_LEN_PARAMS

// Add functions for decimal128
GANDIVA_EXPORT
const char* gdv_fn_md5_decimal128(int64_t context, int64_t x_high, uint64_t x_low,
                                  int32_t /*x_precision*/, int32_t /*x_scale*/,
                                  gdv_boolean x_isvalid, int32_t* out_length) {
  if (!x_isvalid) {
    return gandiva::gdv_md5_hash(context, NULLPTR, 0, out_length);
  }

  const gandiva::BasicDecimal128 decimal_128(x_high, x_low);
  return gandiva::gdv_md5_hash(context, decimal_128.ToBytes().data(), 16, out_length);
}

GANDIVA_EXPORT
const char* gdv_fn_sha256_decimal128(int64_t context, int64_t x_high, uint64_t x_low,
                                     int32_t /*x_precision*/, int32_t /*x_scale*/,
                                     gdv_boolean x_isvalid, int32_t* out_length) {
  if (!x_isvalid) {
    return gandiva::gdv_sha256_hash(context, NULLPTR, 0, out_length);
  }

  const gandiva::BasicDecimal128 decimal_128(x_high, x_low);
  return gandiva::gdv_sha256_hash(context, decimal_128.ToBytes().data(), 16, out_length);
}

GANDIVA_EXPORT
const char* gdv_fn_sha1_decimal128(int64_t context, int64_t x_high, uint64_t x_low,
                                   int32_t /*x_precision*/, int32_t /*x_scale*/,
                                   gdv_boolean x_isvalid, int32_t* out_length) {
  if (!x_isvalid) {
    return gandiva::gdv_sha1_hash(context, NULLPTR, 0, out_length);
  }

  const gandiva::BasicDecimal128 decimal_128(x_high, x_low);
  return gandiva::gdv_sha1_hash(context, decimal_128.ToBytes().data(), 16, out_length);
}

int32_t gdv_fn_dec_from_string(int64_t context, const char* in, int32_t in_length,
                               int32_t* precision_from_str, int32_t* scale_from_str,
                               int64_t* dec_high_from_str, uint64_t* dec_low_from_str) {
  arrow::Decimal128 dec;
  auto status = arrow::Decimal128::FromString(std::string(in, in_length), &dec,
                                              precision_from_str, scale_from_str);
  if (!status.ok()) {
    gdv_fn_context_set_error_msg(context, status.message().data());
    return -1;
  }
  *dec_high_from_str = dec.high_bits();
  *dec_low_from_str = dec.low_bits();
  return 0;
}

char* gdv_fn_dec_to_string(int64_t context, int64_t x_high, uint64_t x_low,
                           int32_t x_scale, int32_t* dec_str_len) {
  arrow::Decimal128 dec(arrow::BasicDecimal128(x_high, x_low));
  std::string dec_str = dec.ToString(x_scale);
  *dec_str_len = static_cast<int32_t>(dec_str.length());
  char* ret = reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, *dec_str_len));
  if (ret == nullptr) {
    std::string err_msg = "Could not allocate memory for string: " + dec_str;
    gdv_fn_context_set_error_msg(context, err_msg.data());
    return nullptr;
  }
  memcpy(ret, dec_str.data(), *dec_str_len);
  return ret;
}

#define CAST_NUMERIC_FROM_STRING(OUT_TYPE, ARROW_TYPE, TYPE_NAME)                    \
  GANDIVA_EXPORT                                                                     \
  OUT_TYPE gdv_fn_cast##TYPE_NAME##_utf8(int64_t context, const char* data,          \
                                         int32_t len) {                              \
    OUT_TYPE val = 0;                                                                \
    /* trim leading and trailing spaces */                                           \
    int32_t trimmed_len;                                                             \
    int32_t start = 0, end = len - 1;                                                \
    while (start <= end && data[start] == ' ') {                                     \
      ++start;                                                                       \
    }                                                                                \
    while (end >= start && data[end] == ' ') {                                       \
      --end;                                                                         \
    }                                                                                \
    trimmed_len = end - start + 1;                                                   \
    const char* trimmed_data = data + start;                                         \
    if (!arrow::internal::ParseValue<ARROW_TYPE>(trimmed_data, trimmed_len, &val)) { \
      std::string err =                                                              \
          "Failed to cast the string " + std::string(data, len) + " to " #OUT_TYPE;  \
      gdv_fn_context_set_error_msg(context, err.c_str());                            \
    }                                                                                \
    return val;                                                                      \
  }

CAST_NUMERIC_FROM_STRING(int32_t, arrow::Int32Type, INT)
CAST_NUMERIC_FROM_STRING(int64_t, arrow::Int64Type, BIGINT)
CAST_NUMERIC_FROM_STRING(float, arrow::FloatType, FLOAT4)
CAST_NUMERIC_FROM_STRING(double, arrow::DoubleType, FLOAT8)

#undef CAST_NUMERIC_FROM_STRING

#define CAST_NUMERIC_OR_NULL_FROM_STRING(OUT_TYPE, ARROW_TYPE, TYPE_NAME)            \
  GANDIVA_EXPORT                                                                     \
  OUT_TYPE gdv_fn_cast##TYPE_NAME##_or_null_utf8(int64_t context, const char* data,  \
                                    int32_t len, bool in_valid, bool* out_valid) {   \
    OUT_TYPE val = 0;                                                                \
    *out_valid = true;                                                               \
    if (!in_valid) {                                                                 \
      *out_valid = false;                                                            \
      return val;                                                                    \
    }                                                                                \
    /* trim leading and trailing spaces */                                           \
    int32_t trimmed_len;                                                             \
    int32_t start = 0, end = len - 1;                                                \
    while (start <= end && data[start] == ' ') {                                     \
      ++start;                                                                       \
    }                                                                                \
    while (end >= start && data[end] == ' ') {                                       \
      --end;                                                                         \
    }                                                                                \
    trimmed_len = end - start + 1;                                                   \
    const char* trimmed_data = data + start;                                         \
    if (!arrow::internal::ParseValue<ARROW_TYPE>(trimmed_data, trimmed_len, &val)) { \
      *out_valid = false;                                                            \
    }                                                                                \
    return val;                                                                      \
  }

CAST_NUMERIC_OR_NULL_FROM_STRING(int32_t, arrow::Int32Type, INT)
CAST_NUMERIC_OR_NULL_FROM_STRING(int64_t, arrow::Int64Type, BIGINT)
CAST_NUMERIC_OR_NULL_FROM_STRING(float, arrow::FloatType, FLOAT4)
CAST_NUMERIC_OR_NULL_FROM_STRING(double, arrow::DoubleType, FLOAT8)

#undef CAST_NUMERIC_OR_NULL_FROM_STRING

#define GDV_FN_CAST_VARCHAR_INTEGER(IN_TYPE, ARROW_TYPE)                                 \
  GANDIVA_EXPORT                                                                         \
  const char* gdv_fn_castVARCHAR_##IN_TYPE##_int64(int64_t context, gdv_##IN_TYPE value, \
                                                   int64_t len, int32_t * out_len) {     \
    if (len < 0) {                                                                       \
      gdv_fn_context_set_error_msg(context, "Buffer length can not be negative");        \
      *out_len = 0;                                                                      \
      return "";                                                                         \
    }                                                                                    \
    if (len == 0) {                                                                      \
      *out_len = 0;                                                                      \
      return "";                                                                         \
    }                                                                                    \
    arrow::internal::StringFormatter<arrow::ARROW_TYPE> formatter;                       \
    char* ret = reinterpret_cast<char*>(                                                 \
        gdv_fn_context_arena_malloc(context, static_cast<int32_t>(len)));                \
    if (ret == nullptr) {                                                                \
      gdv_fn_context_set_error_msg(context, "Could not allocate memory");                \
      *out_len = 0;                                                                      \
      return "";                                                                         \
    }                                                                                    \
    arrow::Status status = formatter(value, [&](arrow::util::string_view v) {            \
      int64_t size = static_cast<int64_t>(v.size());                                     \
      *out_len = static_cast<int32_t>(len < size ? len : size);                          \
      memcpy(ret, v.data(), *out_len);                                                   \
      return arrow::Status::OK();                                                        \
    });                                                                                  \
    if (!status.ok()) {                                                                  \
      std::string err = "Could not cast " + std::to_string(value) + " to string";        \
      gdv_fn_context_set_error_msg(context, err.c_str());                                \
      *out_len = 0;                                                                      \
      return "";                                                                         \
    }                                                                                    \
    return ret;                                                                          \
  }

#define GDV_FN_CAST_VARCHAR_REAL(IN_TYPE, ARROW_TYPE)                                    \
  GANDIVA_EXPORT                                                                         \
  const char* gdv_fn_castVARCHAR_##IN_TYPE##_int64(int64_t context, gdv_##IN_TYPE value, \
                                                   int64_t len, int32_t * out_len) {     \
    if (len < 0) {                                                                       \
      gdv_fn_context_set_error_msg(context, "Buffer length can not be negative");        \
      *out_len = 0;                                                                      \
      return "";                                                                         \
    }                                                                                    \
    if (len == 0) {                                                                      \
      *out_len = 0;                                                                      \
      return "";                                                                         \
    }                                                                                    \
    gandiva::GdvStringFormatter<arrow::ARROW_TYPE> formatter;                            \
    char* ret = reinterpret_cast<char*>(                                                 \
        gdv_fn_context_arena_malloc(context, static_cast<int32_t>(len)));                \
    if (ret == nullptr) {                                                                \
      gdv_fn_context_set_error_msg(context, "Could not allocate memory");                \
      *out_len = 0;                                                                      \
      return "";                                                                         \
    }                                                                                    \
    arrow::Status status = formatter(value, [&](arrow::util::string_view v) {            \
      int64_t size = static_cast<int64_t>(v.size());                                     \
      *out_len = static_cast<int32_t>(len < size ? len : size);                          \
      memcpy(ret, v.data(), *out_len);                                                   \
      return arrow::Status::OK();                                                        \
    });                                                                                  \
    if (!status.ok()) {                                                                  \
      std::string err = "Could not cast " + std::to_string(value) + " to string";        \
      gdv_fn_context_set_error_msg(context, err.c_str());                                \
      *out_len = 0;                                                                      \
      return "";                                                                         \
    }                                                                                    \
    return ret;                                                                          \
  }

GDV_FN_CAST_VARCHAR_INTEGER(int32, Int32Type)
GDV_FN_CAST_VARCHAR_INTEGER(int64, Int64Type)
GDV_FN_CAST_VARCHAR_REAL(float32, FloatType)
GDV_FN_CAST_VARCHAR_REAL(float64, DoubleType)

#undef GDV_FN_CAST_VARCHAR_INTEGER
#undef GDV_FN_CAST_VARCHAR_REAL
}

namespace gandiva {

void ExportedStubFunctions::AddMappings(Engine* engine) const {
  std::vector<llvm::Type*> args;
  auto types = engine->types();

  // gdv_fn_dec_from_string
  args = {
      types->i64_type(),      // context
      types->i8_ptr_type(),   // const char* in
      types->i32_type(),      // int32_t in_length
      types->i32_ptr_type(),  // int32_t* precision_from_str
      types->i32_ptr_type(),  // int32_t* scale_from_str
      types->i64_ptr_type(),  // int64_t* dec_high_from_str
      types->i64_ptr_type(),  // int64_t* dec_low_from_str
  };

  engine->AddGlobalMappingForFunc("gdv_fn_dec_from_string",
                                  types->i32_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_dec_from_string));

  // gdv_fn_dec_to_string
  args = {
      types->i64_type(),      // context
      types->i64_type(),      // int64_t x_high
      types->i64_type(),      // int64_t x_low
      types->i32_type(),      // int32_t x_scale
      types->i64_ptr_type(),  // int64_t* dec_str_len
  };

  engine->AddGlobalMappingForFunc("gdv_fn_dec_to_string",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_dec_to_string));

  // gdv_fn_get_json_object_utf8_utf8
  args = {types->i64_type(),     // int64_t ptr
          types->i64_type(),     // int64_t holder_ptr
          types->i8_ptr_type(),  // const char* data
          types->i32_type(),     // int data_len
          types->i1_type(),      // bool in1_validity
          types->i8_ptr_type(),  // const char* pattern
          types->i32_type(),     // int pattern_len
          types->i1_type(),      // bool in2_validity
          types->ptr_type(types->i8_type()),  // bool* out_valid
          types->i32_ptr_type()};  // int out_len
  engine->AddGlobalMappingForFunc("gdv_fn_get_json_object_utf8_utf8",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_get_json_object_utf8_utf8));
  
  // gdv_fn_translate_utf8_utf8_utf8
  args = {types->i64_type(),      // int64_t ptr
          types->i64_type(),      // int64_t holder_ptr
          types->i8_ptr_type(),   // const char* text
          types->i32_type(),      // int text_len
          types->i8_ptr_type(),   // const char* matching_str
          types->i32_type(),      // int matching_str_len
          types->i8_ptr_type(),   // const char* replace_str
          types->i32_type(),      // int replace_str_len
          types->i32_ptr_type()}; // int* out_len
  engine->AddGlobalMappingForFunc("gdv_fn_translate_utf8_utf8_utf8", 
                                  types->i8_ptr_type() /*return types*/, args, 
                                  reinterpret_cast<void*>(gdv_fn_translate_utf8_utf8_utf8));

  // gdv_fn_substr_index_utf8_utf8_int32
  args = {types->i64_type(),      // int64_t ptr
          types->i64_type(),      // int64_t holder_ptr
          types->i8_ptr_type(),   // const char* input
          types->i32_type(),      // int input_len
          types->i8_ptr_type(),   // const char* delim_str
          types->i32_type(),      // int delim_str_len
          types->i32_type(),      // int count
          types->i32_ptr_type()}; // int* out_len
  engine->AddGlobalMappingForFunc("gdv_fn_substr_index_utf8_utf8_int32",
                                  types->i8_ptr_type() /*return types*/, args, 
                                  reinterpret_cast<void*>(gdv_fn_substr_index_utf8_utf8_int32));

  // gdv_fn_rlike_utf8_utf8
  args = {types->i64_type(),     // int64_t ptr
          types->i8_ptr_type(),  // const char* data
          types->i32_type(),     // int data_len
          types->i8_ptr_type(),  // const char* pattern
          types->i32_type()};    // int pattern_len

  engine->AddGlobalMappingForFunc("gdv_fn_rlike_utf8_utf8",
                                  types->i1_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_rlike_utf8_utf8));

  // gdv_fn_like_utf8_utf8
  args = {types->i64_type(),     // int64_t ptr
          types->i8_ptr_type(),  // const char* data
          types->i32_type(),     // int data_len
          types->i8_ptr_type(),  // const char* pattern
          types->i32_type()};    // int pattern_len

  engine->AddGlobalMappingForFunc("gdv_fn_like_utf8_utf8",
                                  types->i1_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_like_utf8_utf8));

  // gdv_fn_like_utf8_utf8_utf8
  args = {types->i64_type(),     // int64_t ptr
          types->i8_ptr_type(),  // const char* data
          types->i32_type(),     // int data_len
          types->i8_ptr_type(),  // const char* pattern
          types->i32_type(),     // int pattern_len
          types->i8_ptr_type(),  // const char* escape_char
          types->i32_type()};    // int escape_char_len

  engine->AddGlobalMappingForFunc("gdv_fn_like_utf8_utf8_utf8",
                                  types->i1_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_like_utf8_utf8_utf8));

  // gdv_fn_ilike_utf8_utf8
  args = {types->i64_type(),     // int64_t ptr
          types->i8_ptr_type(),  // const char* data
          types->i32_type(),     // int data_len
          types->i8_ptr_type(),  // const char* pattern
          types->i32_type()};    // int pattern_len

  engine->AddGlobalMappingForFunc("gdv_fn_ilike_utf8_utf8",
                                  types->i1_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_ilike_utf8_utf8));

  // gdv_fn_regexp_replace_utf8_utf8
  args = {types->i64_type(),       // int64_t ptr
          types->i64_type(),       // int64_t holder_ptr
          types->i8_ptr_type(),    // const char* data
          types->i32_type(),       // int data_len
          types->i8_ptr_type(),    // const char* pattern
          types->i32_type(),       // int pattern_len
          types->i8_ptr_type(),    // const char* replace_string
          types->i32_type(),       // int32_t replace_string_len
          types->i32_ptr_type()};  // int32_t* out_length

  engine->AddGlobalMappingForFunc(
      "gdv_fn_regexp_replace_utf8_utf8", types->i8_ptr_type() /*return_type*/, args,
      reinterpret_cast<void*>(gdv_fn_regexp_replace_utf8_utf8));

  // gdv_fn_regexp_extract_utf8_utf8_int32
  args = {types->i64_type(),       // int64_t ptr
          types->i64_type(),       // int64_t holder_ptr
          types->i8_ptr_type(),    // const char* data
          types->i32_type(),       // int data_len
          types->i8_ptr_type(),    // const char* pattern
          types->i32_type(),       // int pattern_len
          types->i32_type(),       // int32_t regex group index
          types->i32_ptr_type()};  // int32_t* out_length

  engine->AddGlobalMappingForFunc(
      "gdv_fn_regexp_extract_utf8_utf8_int32", types->i8_ptr_type() /*return_type*/, args,
      reinterpret_cast<void*>(gdv_fn_regexp_extract_utf8_utf8_int32));

  // gdv_fn_parse_url_utf8_utf8
  args = {types->i64_type(),       // int64_t ptr
          types->i64_type(),       // int64_t holder_ptr
          types->i8_ptr_type(),    // const char* data
          types->i32_type(),       // int data_len
          types->i1_type(),        // bool in1_validity
          types->i8_ptr_type(),    // const char* part
          types->i32_type(),       // int part_len
          types->i1_type(),      // bool in2_validity
          types->ptr_type(types->i8_type()),  // bool* out_valid
          types->i32_ptr_type()};  // int32_t* out_length

  engine->AddGlobalMappingForFunc(
      "gdv_fn_parse_url_utf8_utf8", types->i8_ptr_type() /*return_type*/, args,
      reinterpret_cast<void*>(gdv_fn_parse_url_utf8_utf8));

  // gdv_fn_parse_url_utf8_utf8_utf8
  args = {types->i64_type(),       // int64_t ptr
          types->i64_type(),       // int64_t holder_ptr
          types->i8_ptr_type(),    // const char* data
          types->i32_type(),       // int data_len
          types->i1_type(),        // bool in1_validity
          types->i8_ptr_type(),    // const char* part
          types->i32_type(),       // int part_len
          types->i1_type(),        // bool in2_validity
          types->i8_ptr_type(),    // const char* pattern
          types->i32_type(),       // int pattern_len
          types->i1_type(),        // bool in3_validity
          types->ptr_type(types->i8_type()),  // bool* out_valid
          types->i32_ptr_type()};  // int32_t* out_length

  engine->AddGlobalMappingForFunc(
      "gdv_fn_parse_url_utf8_utf8_utf8", types->i8_ptr_type() /*return_type*/, args,
      reinterpret_cast<void*>(gdv_fn_parse_url_utf8_utf8_utf8));

  // gdv_fn_to_date_utf8_utf8
  args = {types->i64_type(),                   // int64_t execution_context
          types->i64_type(),                   // int64_t holder_ptr
          types->i8_ptr_type(),                // const char* data
          types->i32_type(),                   // int data_len
          types->i1_type(),                    // bool in1_validity
          types->i8_ptr_type(),                // const char* pattern
          types->i32_type(),                   // int pattern_len
          types->i1_type(),                    // bool in2_validity
          types->ptr_type(types->i8_type())};  // bool* out_valid

  engine->AddGlobalMappingForFunc("gdv_fn_to_date_utf8_utf8",
                                  types->i64_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_to_date_utf8_utf8));

  // gdv_fn_to_date_utf8_utf8_int32
  args = {types->i64_type(),                   // int64_t execution_context
          types->i64_type(),                   // int64_t holder_ptr
          types->i8_ptr_type(),                // const char* data
          types->i32_type(),                   // int data_len
          types->i1_type(),                    // bool in1_validity
          types->i8_ptr_type(),                // const char* pattern
          types->i32_type(),                   // int pattern_len
          types->i1_type(),                    // bool in2_validity
          types->i32_type(),                   // int32_t suppress_errors
          types->i1_type(),                    // bool in3_validity
          types->ptr_type(types->i8_type())};  // bool* out_valid

  engine->AddGlobalMappingForFunc(
      "gdv_fn_to_date_utf8_utf8_int32", types->i64_type() /*return_type*/, args,
      reinterpret_cast<void*>(gdv_fn_to_date_utf8_utf8_int32));

  // gdv_fn_in_expr_lookup_int32
  args = {types->i64_type(),  // int64_t in holder ptr
          types->i32_type(),  // int32 value
          types->i1_type()};  // bool in_validity

  engine->AddGlobalMappingForFunc("gdv_fn_in_expr_lookup_int32",
                                  types->i1_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_in_expr_lookup_int32));

  // gdv_fn_in_expr_lookup_int64
  args = {types->i64_type(),  // int64_t in holder ptr
          types->i64_type(),  // int64 value
          types->i1_type()};  // bool in_validity

  engine->AddGlobalMappingForFunc("gdv_fn_in_expr_lookup_int64",
                                  types->i1_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_in_expr_lookup_int64));

  // gdv_fn_in_expr_lookup_decimal
  args = {types->i64_type(),  // int64_t in holder ptr
          types->i64_type(),  // high decimal value
          types->i64_type(),  // low decimal value
          types->i32_type(),  // decimal precision value
          types->i32_type(),  // decimal scale value
          types->i1_type()};  // bool in_validity

  engine->AddGlobalMappingForFunc("gdv_fn_in_expr_lookup_decimal",
                                  types->i1_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_in_expr_lookup_decimal));

  // gdv_fn_in_expr_lookup_utf8
  args = {types->i64_type(),     // int64_t in holder ptr
          types->i8_ptr_type(),  // const char* value
          types->i32_type(),     // int value_len
          types->i1_type()};     // bool in_validity

  engine->AddGlobalMappingForFunc("gdv_fn_in_expr_lookup_utf8",
                                  types->i1_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_in_expr_lookup_utf8));

  // gdv_fn_populate_varlen_vector
  args = {types->i64_type(),      // int64_t execution_context
          types->i8_ptr_type(),   // int8_t* data ptr
          types->i32_ptr_type(),  // int32_t* offsets ptr
          types->i64_type(),      // int64_t slot
          types->i8_ptr_type(),   // const char* entry_buf
          types->i32_type()};     // int32_t entry__len

  engine->AddGlobalMappingForFunc("gdv_fn_populate_varlen_vector",
                                  types->i32_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_populate_varlen_vector));

  // gdv_fn_random
  args = {types->i64_type()};
  engine->AddGlobalMappingForFunc("gdv_fn_random", types->double_type(), args,
                                  reinterpret_cast<void*>(gdv_fn_random));

  args = {types->i64_type(), types->i32_type(), types->i1_type()};
  engine->AddGlobalMappingForFunc("gdv_fn_random_with_seed", types->double_type(), args,
                                  reinterpret_cast<void*>(gdv_fn_random_with_seed));

  args = {types->i64_type(), types->i64_type(), types->i1_type()};
  engine->AddGlobalMappingForFunc("gdv_fn_random_with_seed64", types->double_type(), args,
                                  reinterpret_cast<void*>(gdv_fn_random_with_seed64));

  args = {types->i64_type(),     // int64_t context_ptr
          types->i8_ptr_type(),  // const char* data
          types->i32_type()};    // int32_t lenr

  engine->AddGlobalMappingForFunc("gdv_fn_castINT_utf8", types->i32_type(), args,
                                  reinterpret_cast<void*>(gdv_fn_castINT_utf8));

  args = {types->i64_type(),     // int64_t context_ptr
          types->i8_ptr_type(),  // const char* data
          types->i32_type(),    // int32_t lenr
          types->i1_type(),    // bool in1_validity
          types->ptr_type(types->i8_type())};  // bool* out_valid

  engine->AddGlobalMappingForFunc("gdv_fn_castINT_or_null_utf8", types->i32_type(), args,
                                  reinterpret_cast<void*>(gdv_fn_castINT_or_null_utf8));

  args = {types->i64_type(),     // int64_t context_ptr
          types->i8_ptr_type(),  // const char* data
          types->i32_type()};    // int32_t lenr

  engine->AddGlobalMappingForFunc("gdv_fn_castBIGINT_utf8", types->i64_type(), args,
                                  reinterpret_cast<void*>(gdv_fn_castBIGINT_utf8));
  
  args = {types->i64_type(),     // int64_t context_ptr
          types->i8_ptr_type(),  // const char* data
          types->i32_type(),    // int32_t lenr
          types->i1_type(),    // bool in1_validity
          types->ptr_type(types->i8_type())};  // bool* out_valid

  engine->AddGlobalMappingForFunc("gdv_fn_castBIGINT_or_null_utf8", types->i64_type(), args,
                                  reinterpret_cast<void*>(gdv_fn_castBIGINT_or_null_utf8));

  args = {types->i64_type(),     // int64_t context_ptr
          types->i8_ptr_type(),  // const char* data
          types->i32_type()};    // int32_t lenr

  engine->AddGlobalMappingForFunc("gdv_fn_castFLOAT4_utf8", types->float_type(), args,
                                  reinterpret_cast<void*>(gdv_fn_castFLOAT4_utf8));

  args = {types->i64_type(),     // int64_t context_ptr
          types->i8_ptr_type(),  // const char* data
          types->i32_type(),    // int32_t lenr
          types->i1_type(),    // bool in1_validity
          types->ptr_type(types->i8_type())};  // bool* out_valid

  engine->AddGlobalMappingForFunc("gdv_fn_castFLOAT4_or_null_utf8", types->float_type(), args,
                                  reinterpret_cast<void*>(gdv_fn_castFLOAT4_or_null_utf8));

  args = {types->i64_type(),     // int64_t context_ptr
          types->i8_ptr_type(),  // const char* data
          types->i32_type()};    // int32_t lenr

  engine->AddGlobalMappingForFunc("gdv_fn_castFLOAT8_utf8", types->double_type(), args,
                                  reinterpret_cast<void*>(gdv_fn_castFLOAT8_utf8));
  
  args = {types->i64_type(),     // int64_t context_ptr
          types->i8_ptr_type(),  // const char* data
          types->i32_type(),    // int32_t lenr
          types->i1_type(),    // bool in1_validity
          types->ptr_type(types->i8_type())};  // bool* out_valid

  engine->AddGlobalMappingForFunc("gdv_fn_castFLOAT8_or_null_utf8", types->double_type(), args,
                                  reinterpret_cast<void*>(gdv_fn_castFLOAT8_or_null_utf8));

  // gdv_fn_castVARCHAR_int32_int64
  args = {types->i64_type(),       // int64_t execution_context
          types->i32_type(),       // int32_t value
          types->i64_type(),       // int64_t len
          types->i32_ptr_type()};  // int32_t* out_len
  engine->AddGlobalMappingForFunc(
      "gdv_fn_castVARCHAR_int32_int64", types->i8_ptr_type() /*return_type*/, args,
      reinterpret_cast<void*>(gdv_fn_castVARCHAR_int32_int64));

  // gdv_fn_castVARCHAR_int64_int64
  args = {types->i64_type(),       // int64_t execution_context
          types->i64_type(),       // int64_t value
          types->i64_type(),       // int64_t len
          types->i32_ptr_type()};  // int32_t* out_len
  engine->AddGlobalMappingForFunc(
      "gdv_fn_castVARCHAR_int64_int64", types->i8_ptr_type() /*return_type*/, args,
      reinterpret_cast<void*>(gdv_fn_castVARCHAR_int64_int64));

  // gdv_fn_castVARCHAR_float32_int64
  args = {types->i64_type(),       // int64_t execution_context
          types->float_type(),     // float value
          types->i64_type(),       // int64_t len
          types->i32_ptr_type()};  // int32_t* out_len
  engine->AddGlobalMappingForFunc(
      "gdv_fn_castVARCHAR_float32_int64", types->i8_ptr_type() /*return_type*/, args,
      reinterpret_cast<void*>(gdv_fn_castVARCHAR_float32_int64));

  // gdv_fn_castVARCHAR_float64_int64
  args = {types->i64_type(),       // int64_t execution_context
          types->double_type(),    // double value
          types->i64_type(),       // int64_t len
          types->i32_ptr_type()};  // int32_t* out_len
  engine->AddGlobalMappingForFunc(
      "gdv_fn_castVARCHAR_float64_int64", types->i8_ptr_type() /*return_type*/, args,
      reinterpret_cast<void*>(gdv_fn_castVARCHAR_float64_int64));

  // gdv_fn_md5_int8
  args = {
      types->i64_type(),     // context
      types->i8_type(),      // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_md5_int8", types->i8_ptr_type() /*return_type*/,
                                  args, reinterpret_cast<void*>(gdv_fn_md5_int8));

  // gdv_fn_md5_int16
  args = {
      types->i64_type(),     // context
      types->i16_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_md5_int16",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_md5_int16));

  // gdv_fn_md5_int32
  args = {
      types->i64_type(),     // context
      types->i32_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_md5_int32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_md5_int32));

  // gdv_fn_md5_int32
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_md5_int64",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_md5_int64));

  // gdv_fn_md5_uint8
  args = {
      types->i64_type(),     // context
      types->i8_type(),      // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_md5_uint8",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_md5_uint8));

  // gdv_fn_md5_uint16
  args = {
      types->i64_type(),     // context
      types->i16_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_md5_uint16",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_md5_uint16));

  // gdv_fn_md5_uint32
  args = {
      types->i64_type(),     // context
      types->i32_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_md5_uint32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_md5_uint32));

  // gdv_fn_md5_uint64
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_md5_uint64",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_md5_uint64));

  // gdv_fn_md5_float32
  args = {
      types->i64_type(),     // context
      types->float_type(),   // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_md5_float32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_md5_float32));

  // gdv_fn_md5_float64
  args = {
      types->i64_type(),     // context
      types->double_type(),  // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_md5_float64",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_md5_float64));

  // gdv_fn_md5_boolean
  args = {
      types->i64_type(),     // context
      types->i1_type(),      // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_md5_boolean",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_md5_boolean));

  // gdv_fn_md5_date64
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_md5_date64",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_md5_date64));

  // gdv_fn_md5_date32
  args = {
      types->i64_type(),     // context
      types->i32_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_md5_date32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_md5_date32));

  // gdv_fn_md5_time32
  args = {
      types->i64_type(),     // context
      types->i32_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_md5_time32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_md5_time32));

  // gdv_fn_md5_timestamp
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_md5_timestamp",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_md5_timestamp));

  // gdv_fn_md5_utf8
  args = {
      types->i64_type(),     // context
      types->i8_ptr_type(),  // const char*
      types->i32_type(),     // value_length
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out
  };

  engine->AddGlobalMappingForFunc("gdv_fn_md5_utf8", types->i8_ptr_type() /*return_type*/,
                                  args, reinterpret_cast<void*>(gdv_fn_md5_utf8));

  // gdv_fn_md5_from_binary
  args = {
      types->i64_type(),     // context
      types->i8_ptr_type(),  // const char*
      types->i32_type(),     // value_length
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out
  };

  engine->AddGlobalMappingForFunc("gdv_fn_md5_binary",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_md5_binary));

  // gdv_fn_sha1_int8
  args = {
      types->i64_type(),     // context
      types->i8_type(),      // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha1_int8",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha1_int8));

  // gdv_fn_sha1_int16
  args = {
      types->i64_type(),     // context
      types->i16_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha1_int16",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha1_int16));

  // gdv_fn_sha1_int32
  args = {
      types->i64_type(),     // context
      types->i32_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha1_int32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha1_int32));

  // gdv_fn_sha1_int32
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha1_int64",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha1_int64));

  // gdv_fn_sha1_uint8
  args = {
      types->i64_type(),     // context
      types->i8_type(),      // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha1_uint8",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha1_uint8));

  // gdv_fn_sha1_uint16
  args = {
      types->i64_type(),     // context
      types->i16_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha1_uint16",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha1_uint16));

  // gdv_fn_sha1_uint32
  args = {
      types->i64_type(),     // context
      types->i32_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha1_uint32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha1_uint32));

  // gdv_fn_sha1_uint64
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha1_uint64",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha1_uint64));

  // gdv_fn_sha1_float32
  args = {
      types->i64_type(),     // context
      types->float_type(),   // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha1_float32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha1_float32));

  // gdv_fn_sha1_float64
  args = {
      types->i64_type(),     // context
      types->double_type(),  // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha1_float64",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha1_float64));

  // gdv_fn_sha1_boolean
  args = {
      types->i64_type(),     // context
      types->i1_type(),      // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha1_boolean",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha1_boolean));

  // gdv_fn_sha1_date64
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha1_date64",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha1_date64));

  // gdv_fn_sha1_date32
  args = {
      types->i64_type(),     // context
      types->i32_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha1_date32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha1_date32));

  // gdv_fn_sha1_time32
  args = {
      types->i64_type(),     // context
      types->i32_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha1_time32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha1_time32));

  // gdv_fn_sha1_timestamp
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha1_timestamp",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha1_timestamp));

  // gdv_fn_sha1_from_utf8
  args = {
      types->i64_type(),     // context
      types->i8_ptr_type(),  // const char*
      types->i32_type(),     // value_length
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out
  };

  engine->AddGlobalMappingForFunc("gdv_fn_sha1_utf8",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha1_utf8));

  // gdv_fn_sha1_from_binary
  args = {
      types->i64_type(),     // context
      types->i8_ptr_type(),  // const char*
      types->i32_type(),     // value_length
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out
  };

  engine->AddGlobalMappingForFunc("gdv_fn_sha1_binary",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha1_binary));

  // gdv_fn_sha256_int8
  args = {
      types->i64_type(),     // context
      types->i8_type(),      // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_int8",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha256_int8));

  // gdv_fn_sha256_int16
  args = {
      types->i64_type(),     // context
      types->i16_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_int16",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha256_int16));

  // gdv_fn_sha256_int32
  args = {
      types->i64_type(),     // context
      types->i32_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_int32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha256_int32));

  // gdv_fn_sha256_int32
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_int64",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha256_int64));

  // gdv_fn_sha256_uint8
  args = {
      types->i64_type(),     // context
      types->i8_type(),      // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_uint8",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha256_uint8));

  // gdv_fn_sha256_uint16
  args = {
      types->i64_type(),     // context
      types->i16_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_uint16",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha256_uint16));

  // gdv_fn_sha256_uint32
  args = {
      types->i64_type(),     // context
      types->i32_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_uint32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha256_uint32));

  // gdv_fn_sha256_uint64
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_uint64",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha256_uint64));

  // gdv_fn_sha256_float32
  args = {
      types->i64_type(),     // context
      types->float_type(),   // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_float32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha256_float32));

  // gdv_fn_sha256_float64
  args = {
      types->i64_type(),     // context
      types->double_type(),  // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_float64",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha256_float64));

  // gdv_fn_sha256_boolean
  args = {
      types->i64_type(),     // context
      types->i1_type(),      // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_boolean",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha256_boolean));

  // gdv_fn_sha256_date64
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_date64",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha256_date64));

  // gdv_fn_sha256_date32
  args = {
      types->i64_type(),     // context
      types->i32_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_date32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha256_date32));

  // gdv_fn_sha256_time32
  args = {
      types->i64_type(),     // context
      types->i32_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_time32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha256_time32));

  // gdv_fn_sha256_timestamp
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // value
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha256_timestamp",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha256_timestamp));

  // gdv_fn_hash_sha256_from_utf8
  args = {
      types->i64_type(),     // context
      types->i8_ptr_type(),  // const char*
      types->i32_type(),     // value_length
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out
  };

  engine->AddGlobalMappingForFunc("gdv_fn_sha256_utf8",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha256_utf8));

  // gdv_fn_hash_sha256_from_binary
  args = {
      types->i64_type(),     // context
      types->i8_ptr_type(),  // const char*
      types->i32_type(),     // value_length
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out
  };

  engine->AddGlobalMappingForFunc("gdv_fn_sha256_binary",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha256_binary));

  // gdv_fn_sha1_decimal128
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // high_bits
      types->i64_type(),     // low_bits
      types->i32_type(),     // precision
      types->i32_type(),     // scale
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out length
  };

  engine->AddGlobalMappingForFunc("gdv_fn_sha1_decimal128",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha1_decimal128));
  // gdv_fn_sha256_decimal128
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // high_bits
      types->i64_type(),     // low_bits
      types->i32_type(),     // precision
      types->i32_type(),     // scale
      types->i1_type(),      // validity
      types->i32_ptr_type()  // out length
  };

  engine->AddGlobalMappingForFunc("gdv_fn_sha256_decimal128",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha256_decimal128));
  
  // gdv_fn_sha2_int8_int32
  args = {
      types->i64_type(),     // context
      types->i8_type(),      // value
      types->i1_type(),      // validity
      types->i32_type(),     // bits length
      types->i1_type(),      // bits length validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha2_int8_int32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha2_int8_int32));

  // gdv_fn_sha2_int16_int32
  args = {
      types->i64_type(),     // context
      types->i16_type(),     // value
      types->i1_type(),      // validity
      types->i32_type(),     // bits length
      types->i1_type(),      // bits length validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha2_int16_int32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha2_int16_int32));

  // gdv_fn_sha2_int32_int32
  args = {
      types->i64_type(),     // context
      types->i32_type(),     // value
      types->i1_type(),      // validity
      types->i32_type(),     // bits length
      types->i1_type(),      // bits length validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha2_int32_int32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha2_int32_int32));

  // gdv_fn_sha2_int32_int32
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // value
      types->i1_type(),      // validity
      types->i32_type(),     // bits length
      types->i1_type(),      // bits length validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha2_int64_int32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha2_int64_int32));

  // gdv_fn_sha2_uint8_int32
  args = {
      types->i64_type(),     // context
      types->i8_type(),      // value
      types->i1_type(),      // validity
      types->i32_type(),     // bits length
      types->i1_type(),      // bits length validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha2_uint8_int32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha2_uint8_int32));

  // gdv_fn_sha2_uint16_int32
  args = {
      types->i64_type(),     // context
      types->i16_type(),     // value
      types->i1_type(),      // validity
      types->i32_type(),     // bits length
      types->i1_type(),      // bits length validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha2_uint16_int32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha2_uint16_int32));

  // gdv_fn_sha2_uint32_int32
  args = {
      types->i64_type(),     // context
      types->i32_type(),     // value
      types->i1_type(),      // validity
      types->i32_type(),     // bits length
      types->i1_type(),      // bits length validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha2_uint32_int32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha2_uint32_int32));

  // gdv_fn_sha2_uint64_int32
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // value
      types->i1_type(),      // validity
      types->i32_type(),     // bits length
      types->i1_type(),      // bits length validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha2_uint64_int32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha2_uint64_int32));

  // gdv_fn_sha256_float32_int32
  args = {
      types->i64_type(),     // context
      types->float_type(),   // value
      types->i1_type(),      // validity
      types->i32_type(),     // bits length
      types->i1_type(),      // bits length validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha2_float32_int32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha2_float32_int32));

  // gdv_fn_sha2_float64_int32
  args = {
      types->i64_type(),     // context
      types->double_type(),  // value
      types->i1_type(),      // validity
      types->i32_type(),     // bits length
      types->i1_type(),      // bits length validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha2_float64_int32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha2_float64_int32));

  // gdv_fn_sha2_boolean_int32
  args = {
      types->i64_type(),     // context
      types->i1_type(),      // value
      types->i1_type(),      // validity
      types->i32_type(),     // bits length
      types->i1_type(),      // bits length validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha2_boolean_int32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha2_boolean_int32));

  // gdv_fn_sha2_date64_int32
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // value
      types->i1_type(),      // validity
      types->i32_type(),     // bits length
      types->i1_type(),      // bits length validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha2_date64_int32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha2_date64_int32));

  // gdv_fn_sha2_date32_int32
  args = {
      types->i64_type(),     // context
      types->i32_type(),     // value
      types->i1_type(),      // validity
      types->i32_type(),     // bits length
      types->i1_type(),      // bits length validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha2_date32_int32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha2_date32_int32));

  // gdv_fn_sha2_time32_int32
  args = {
      types->i64_type(),     // context
      types->i32_type(),     // value
      types->i1_type(),      // validity
      types->i32_type(),     // bits length
      types->i1_type(),      // bits length validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha2_time32_int32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha2_time32_int32));

  // gdv_fn_sha2_timestamp_int32
  args = {
      types->i64_type(),     // context
      types->i64_type(),     // value
      types->i1_type(),      // validity
      types->i32_type(),     // bits length
      types->i1_type(),      // bits length validity
      types->i32_ptr_type()  // out_length
  };
  engine->AddGlobalMappingForFunc("gdv_fn_sha2_timestamp_int32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha2_timestamp_int32));

  // gdv_fn_sha2_utf8_int32
  args = {
      types->i64_type(),     // context
      types->i8_ptr_type(),  // const char*
      types->i32_type(),     // value_length
      types->i1_type(),      // validity
      types->i32_type(),     // bits length
      types->i1_type(),      // bits length validity
      types->i32_ptr_type()  // out
  };

  engine->AddGlobalMappingForFunc("gdv_fn_sha2_utf8_int32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha2_utf8_int32));

  // gdv_fn_hash_sha2_from_binary_int32
  args = {
      types->i64_type(),     // context
      types->i8_ptr_type(),  // const char*
      types->i32_type(),     // value_length
      types->i1_type(),      // validity
      types->i32_type(),     // bits length
      types->i1_type(),      // bits length validity
      types->i32_ptr_type()  // out
  };

  engine->AddGlobalMappingForFunc("gdv_fn_sha2_binary_int32",
                                  types->i8_ptr_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(gdv_fn_sha2_binary_int32));
}
}  // namespace gandiva
