/*
    Proteus -- High-performance query processing on heterogeneous hardware.

                            Copyright (c) 2025
        Data Intensive Applications and Systems Laboratory (DIAS)
                École Polytechnique Fédérale de Lausanne

                            All Rights Reserved.

    Permission to use, copy, modify and distribute this software and
    its documentation is hereby granted, provided that both the
    copyright notice and this permission notice appear in all copies of
    the software, derivative works or modified versions, and any
    portions thereof, and that both notices appear in supporting
    documentation.

    This code is distributed in the hope that it will be useful, but
    WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS
    DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER
    RESULTING FROM THE USE OF THIS SOFTWARE.
*/

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>

#include <fstream>
#include <optional>
#include <sstream>
#include <taxi/query.hpp>

std::optional<int64_t> getLinehintFromJson(const char* json_string) {
  // get the value of the 'linehint' key from the 'plugin' object
  rapidjson::Document doc;
  doc.Parse(json_string);

  if (doc.HasParseError()) {
    LOG(ERROR) << "JSON parse error: "
               << rapidjson::GetParseError_En(doc.GetParseError())
               << " at offset " << doc.GetErrorOffset();
    return std::nullopt;
  }

  if (!doc.IsObject()) {
    LOG(ERROR) << "Error: JSON root is not an object.";
    return std::nullopt;
  }

  for (auto& top_member : doc.GetObject()) {
    if (!top_member.value.IsObject()) continue;  // Skip if not an object

    const rapidjson::Value& inner_obj = top_member.value;

    // Check if the "plugin" member exists and is an object
    if (inner_obj.HasMember("plugin") && inner_obj["plugin"].IsObject()) {
      const rapidjson::Value& plugin_obj = inner_obj["plugin"];

      // Check if the "linehint" member exists and is an integer
      if (plugin_obj.HasMember("linehint") &&
          plugin_obj["linehint"].IsInt64()) {
        return plugin_obj["linehint"].GetInt64();
      } else {
        LOG(ERROR) << "Error: 'linehint' not found or not an Int64 within "
                      "'plugin' object.";
      }
    } else {
      LOG(ERROR) << "Error: 'plugin' object not found or not an object.";
    }
    // Assuming only one top-level entry matters for this specific task
    break;
  }

  LOG(ERROR)
      << "Error: Could not find the expected structure to extract 'linehint'.";
  return std::nullopt;  // Return empty optional if not found or structure is
                        // wrong
}

std::optional<int64_t> getLinehintFromFile(const std::string& file_path) {
  std::ifstream file_stream(file_path);
  if (!file_stream.is_open()) {
    LOG(ERROR) << "Error: Could not open file: " << file_path;
    return std::nullopt;
  }

  std::stringstream buffer;
  buffer << file_stream.rdbuf();
  std::string file_content = buffer.str();

  if (file_content.empty()) {
    LOG(ERROR) << "Error: File is empty or could not be read: " << file_path;
    return std::nullopt;
  }

  return getLinehintFromJson(file_content.c_str());
}

std::map<std::string, std::function<double(proteus::InputPrefixQueryShaper&)>>
taxi::Query::getStats() {
  std::optional<int64_t> row_hint =
      getLinehintFromFile("inputs/taxi/catalog.json");
  if (!row_hint) {
    LOG(FATAL) << "Error: Could not get row hint from file. Does "
                  "inputs/taxi/catalog.json exist and is it valid?";
  }
  // sf is not meaningful for taxi, but the shapers currently expect it
  return {
      {"sf", [](auto&) { return 1.0; }},
      {
          "yellow_tripdata",
          [row_hint](auto&) { return row_hint.value(); },
      },
      {
          "zone_lookup",
          [](auto&) { return 265; },
      },
  };
}
