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

#include "gandiva/parse_url_holder.h"
#include "gandiva/node.h"

namespace gandiva {

  Status ParseUrlHolder::Make(const FunctionNode &node, std::shared_ptr <ParseUrlHolder> *holder) {
    return Make(holder);
  }

  Status ParseUrlHolder::Make(std::shared_ptr<ParseUrlHolder>* holder) {
    *holder = std::shared_ptr<ParseUrlHolder>(new ParseUrlHolder());
    return Status::OK();
  }
}  // namespace gandiva
