#include "JsonParser.hpp"

#include <iostream>
#include <stack>

#include "JsonFileIterator.hpp"

namespace clp_structured {
JsonParser::JsonParser(JsonParserOption const& option)
        : m_archive_dir(option.archive_dir),
          m_num_messages(0),
          m_compression_level(option.compression_level),
          m_max_encoding_size(option.max_encoding_size),
          m_timestamp_column(option.timestamp_column) {
    if (false == boost::filesystem::create_directory(m_archive_dir)) {
        SPDLOG_ERROR("The output directory '{}' already exists", m_archive_dir);
        exit(1);
    }

    if (false == FileUtils::validate_path(option.file_paths)) {
        exit(1);
    }

    for (auto& file_path : option.file_paths) {
        FileUtils::find_all_files(file_path, m_file_paths);
    }

    m_schema_tree = std::make_shared<SchemaTree>();
    m_schema_tree_path = m_archive_dir + "/schema_tree";

    m_schema_map = std::make_shared<SchemaMap>(m_archive_dir, m_compression_level);

    m_timestamp_dictionary = std::make_shared<TimestampDictionaryWriter>();
    m_timestamp_dictionary->open(m_archive_dir + "/timestamp.dict", option.compression_level);

    ArchiveWriterOption archive_writer_option;
    archive_writer_option.archive_dir = m_archive_dir;
    archive_writer_option.id = m_generator();
    archive_writer_option.compression_level = option.compression_level;

    m_archive_writer
            = std::make_unique<ArchiveWriter>(m_schema_tree, m_timestamp_dictionary, m_schema_map);
    m_archive_writer->open(archive_writer_option);
}

void JsonParser::parse_line(ondemand::value line, int32_t parent_node_id, std::string const& key) {
    int32_t node_id;
    std::stack<ondemand::object> object_stack;
    std::stack<int32_t> node_id_stack;
    std::stack<ondemand::object_iterator> object_it_stack;

    ondemand::field cur_field;

    std::string cur_key = key;
    node_id_stack.push(parent_node_id);

    bool can_match_timestamp = !m_timestamp_column.empty();
    bool may_match_timestamp = can_match_timestamp;
    int longest_matching_timestamp_prefix = 0;
    bool matches_timestamp = false;

    do {
        if (false == object_stack.empty()) {
            cur_field = *object_it_stack.top();
            cur_key = std::string(std::string_view(cur_field.unescaped_key(true)));
            line = cur_field.value();
            if (may_match_timestamp) {
                if (object_stack.size() <= m_timestamp_column.size()
                    && cur_key == m_timestamp_column[object_stack.size() - 1])
                {
                    if (object_stack.size() == m_timestamp_column.size()) {
                        // FIXME: technically need to handle the case where this
                        // isn't a string or number column by resetting matches_timestamp
                        // to false
                        matches_timestamp = true;
                    }
                } else {
                    longest_matching_timestamp_prefix = object_stack.size() - 1;
                    may_match_timestamp = false;
                }
            }
        }

        switch (line.type()) {
            case ondemand::json_type::object: {
                node_id = m_schema_tree->add_node(node_id_stack.top(), NodeType::OBJECT, cur_key);
                object_stack.push(std::move(line.get_object()));
                auto objref = object_stack.top();
                auto it = ondemand::object_iterator(objref.begin());
                if (it == objref.end()) {
                    m_current_schema.insert(node_id);
                    object_stack.pop();
                    break;
                } else {
                    object_it_stack.push(it);
                    node_id_stack.push(node_id);
                    continue;
                }
            }
            case ondemand::json_type::array: {
                std::string value = std::string(std::string_view(simdjson::to_json_string(line)));
                node_id = m_schema_tree->add_node(node_id_stack.top(), NodeType::ARRAY, cur_key);
                m_current_parsed_message.add_value(node_id, value);
                m_current_schema.insert(node_id);
                break;
            }
            case ondemand::json_type::number: {
                NodeType type;
                ondemand::number number_value = line.get_number();
                if (false == number_value.is_double()) {
                    // FIXME: should have separate integer and unsigned
                    // integer types to handle values greater than max int64
                    type = NodeType::INTEGER;
                } else {
                    type = NodeType::FLOAT;
                }
                node_id = m_schema_tree->add_node(node_id_stack.top(), type, cur_key);

                if (type == NodeType::INTEGER) {
                    int64_t i64_value;
                    if (number_value.is_uint64()) {
                        i64_value = static_cast<int64_t>(number_value.get_uint64());
                    } else {
                        i64_value = line.get_int64();
                    }

                    m_current_parsed_message.add_value(node_id, i64_value);
                    if (matches_timestamp) {
                        m_timestamp_dictionary->ingest_entry(cur_key, i64_value);
                        matches_timestamp = may_match_timestamp = can_match_timestamp = false;
                    }
                } else {
                    double double_value = line.get_double();
                    m_current_parsed_message.add_value(node_id, double_value);
                    if (matches_timestamp) {
                        m_timestamp_dictionary->ingest_entry(cur_key, double_value);
                        matches_timestamp = may_match_timestamp = can_match_timestamp = false;
                    }
                }
                m_current_schema.insert(node_id);
                break;
            }
            case ondemand::json_type::string: {
                // TODO (Rui): Take a look
                std::string value = std::string(
                        line.raw_json_token().substr(1, line.raw_json_token().size() - 2)
                );
                if (matches_timestamp) {
                    double ret_double;
                    if (StringUtils::convert_string_to_double(value, ret_double)) {
                        node_id = m_schema_tree->add_node(
                                node_id_stack.top(),
                                NodeType::FLOATDATESTRING,
                                cur_key
                        );
                        m_current_parsed_message.add_value(node_id, ret_double);
                    } else {
                        node_id = m_schema_tree->add_node(
                                node_id_stack.top(),
                                NodeType::DATESTRING,
                                cur_key
                        );
                        m_current_parsed_message.add_value(node_id, value);
                    }
                    matches_timestamp = may_match_timestamp = can_match_timestamp = false;
                } else if (value.find(' ') != std::string::npos) {
                    node_id = m_schema_tree
                                      ->add_node(node_id_stack.top(), NodeType::CLPSTRING, cur_key);
                    m_current_parsed_message.add_value(node_id, value);
                } else {
                    node_id = m_schema_tree
                                      ->add_node(node_id_stack.top(), NodeType::VARSTRING, cur_key);
                    m_current_parsed_message.add_value(node_id, value);
                }

                m_current_schema.insert(node_id);
                break;
            }
            case ondemand::json_type::boolean: {
                bool value = line.get_bool();
                node_id = m_schema_tree->add_node(node_id_stack.top(), NodeType::BOOLEAN, cur_key);

                m_current_parsed_message.add_value(node_id, value);
                m_current_schema.insert(node_id);
                break;
            }
            case ondemand::json_type::null: {
                node_id = m_schema_tree
                                  ->add_node(node_id_stack.top(), NodeType::NULLVALUE, cur_key);
                m_current_schema.insert(node_id);
                break;
            }
        }

        if (object_stack.empty()) {
            break;
        }

        bool hit_end;
        do {
            hit_end = false;
            ++object_it_stack.top();
            if (object_it_stack.top() == object_stack.top().end()) {
                object_it_stack.pop();
                object_stack.pop();
                node_id_stack.pop();
                hit_end = true;
            }
            if (can_match_timestamp
                && (object_it_stack.size() - 1) <= longest_matching_timestamp_prefix)
            {
                may_match_timestamp = true;
            }
        } while (!object_it_stack.empty() && hit_end);
    }

    while (!object_stack.empty());
}

void JsonParser::parse() {
    for (auto& file_path : m_file_paths) {
        JsonFileIterator json_file_iterator(file_path);
        if (false == json_file_iterator.is_open()) {
            return;
        }

        simdjson::ondemand::document_stream::iterator json_it;

        m_num_messages = 0;

        while (json_file_iterator.get_json(json_it)) {
            m_current_schema.clear();

            parse_line((*json_it).value(), -1, "root");
            m_num_messages++;

            int32_t current_schema_id = m_schema_map->add_schema(m_current_schema);
            m_current_parsed_message.set_id(current_schema_id);

            if (m_archive_writer->get_data_size() >= m_max_encoding_size) {
                split_archive();
            }

            m_archive_writer
                    ->append_message(current_schema_id, m_current_schema, m_current_parsed_message);
            m_current_parsed_message.clear();
        }

        if (json_file_iterator.truncated_bytes() > 0) {
            SPDLOG_ERROR(
                    "Truncated JSON  ({} bytes) at end of file {}",
                    json_file_iterator.truncated_bytes(),
                    file_path.c_str()
            );
        }
    }
}

std::string
get_full_path(std::shared_ptr<SchemaTree> const& tree, std::shared_ptr<SchemaNode> const& node) {
    if (node->get_id() == 0) {
        return "";
    }

    return get_full_path(tree, tree->get_node(node->get_parent_id())) + "." + node->get_key_name();
}

std::string get_type_name(NodeType type) {
    switch (type) {
        case NodeType::OBJECT:
            return "obj";
        case NodeType::TRUNCATEDOBJECT:
            return "tobj";
        case NodeType::TRUNCATEDCHILDREN:
            return "tchild";
        case NodeType::INTEGER:
            return "int";
        case NodeType::FLOAT:
            return "float";
        case NodeType::CLPSTRING:
            return "clpstring";
        case NodeType::VARSTRING:
            return "varstring";
        case NodeType::VARVALUE:
            return "varvalue";
        case NodeType::BOOLEAN:
            return "bool";
        case NodeType::ARRAY:
            return "array";
        case NodeType::NULLVALUE:
            return "null";
        case NodeType::DATESTRING:
            return "date";
        case NodeType::FLOATDATESTRING:
            return "float";
        default:
            return "unknown";
    }
}

void JsonParser::store() {
    FileWriter schema_tree_writer;
    ZstdCompressor schema_tree_compressor;

    // archive contents must be stored before anything else, because
    // closing an archive can mute the MST and SchemaMap
    // the mutation and storage steps can be easily separated if it becomes
    // necessary for archive packing
    m_archive_writer->close();

    // double tot_records = m_archive_writer->get_num_records();
    // size_t count_before_trunc = 0;
    size_t count_after_trunc = 0;
    for (auto node : m_schema_tree->get_nodes()) {
        /*if (node->get_type() != NodeType::TRUNCATEDOBJECT
            && node->get_type() != NodeType::TRUNCATEDCHILDREN)
        {
            count_before_trunc += 1;
        } else {
            std::cout << "TRUNCATED " << get_full_path(m_schema_tree, node) << " <"
                      << get_type_name(node->get_type()) << ">" << std::endl;
        }
        if (node->get_type() == NodeType::VARVALUE) {
            std::cout << "VARVALUE " << get_full_path(m_schema_tree, node) << " <"
                      << get_type_name(node->get_type()) << ">" << std::endl;
        }*/
        if (node->get_state() != NodeValueState::TRUNCATED) {
            count_after_trunc += 1;
        }
    }

    // std::cout << "Nodes Before Trunc: " << count_before_trunc
    //           << " -> Nodes After Trunc: " << count_after_trunc << std::endl;
    /*for (auto node : m_schema_tree->get_nodes()) {
        if (node->get_state() == NodeValueState::TRUNCATED) {
            std::cout << (node->get_count() / tot_records) << " " << get_full_path(m_schema_tree,
    node) << " <" << get_type_name(node->get_type()) << ">" << std::endl;
        }
    }*/

    schema_tree_writer.open(m_schema_tree_path, FileWriter::OpenMode::CreateForWriting);
    schema_tree_compressor.open(schema_tree_writer, m_compression_level);

    auto nodes = m_schema_tree->get_nodes();
    schema_tree_compressor.write_numeric_value(count_after_trunc);
    for (auto const& node : nodes) {
        if (node->get_state() != NodeValueState::TRUNCATED) {
            schema_tree_compressor.write_numeric_value(node->get_id());
            schema_tree_compressor.write_numeric_value(node->get_parent_id());

            std::string const& key = node->get_key_name();
            schema_tree_compressor.write_numeric_value(key.size());
            schema_tree_compressor.write_string(key);
            schema_tree_compressor.write_numeric_value(node->get_type());
        }
    }

    schema_tree_compressor.close();
    schema_tree_writer.close();

    m_schema_map->store();

    m_timestamp_dictionary->close();
}

void JsonParser::split_archive() {
    m_archive_writer->close();

    ArchiveWriterOption archive_writer_option;
    archive_writer_option.archive_dir = m_archive_dir;
    archive_writer_option.id = m_generator();
    archive_writer_option.compression_level = m_compression_level;

    m_archive_writer->open(archive_writer_option);
}

void JsonParser::close() {
    return;
}
}  // namespace clp_structured
