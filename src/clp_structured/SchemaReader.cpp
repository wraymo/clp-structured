#include "SchemaReader.hpp"

#include "mimalloc.h"

namespace clp_structured {
void SchemaReader::open(std::string path) {
    m_path = std::move(path);
    m_local_schema_tree = std::make_shared<SchemaTree>();
}

void SchemaReader::close() {
    for (auto& i : m_columns) {
        delete i;
    }

    m_columns.clear();
    m_global_id_to_local_id.clear();
}

void SchemaReader::append_column(BaseColumnReader* column_reader) {
    m_columns.push_back(column_reader);
    generate_local_tree(column_reader->get_id());
}

void SchemaReader::append_column(int32_t id) {
    generate_local_tree(id);
}

void SchemaReader::load() {
    constexpr size_t cDecompressorFileReadBufferCapacity = 64 * 1024;  // 64 KB

    m_file_reader.open(m_path);
    m_decompressor.open(m_file_reader, cDecompressorFileReadBufferCapacity);

    m_file_reader.seek_from_begin(0);
    m_file_reader.read_numeric_value(m_num_messages, false);

    for (auto& reader : m_columns) {
        reader->load(m_decompressor, m_num_messages);
    }

    m_decompressor.close();
    m_file_reader.close();

    std::string root_string = "/";
    generate_json_template(0, root_string);
}

bool SchemaReader::get_next_message(std::string& message) {
    if (m_cur_message >= m_num_messages) {
        return false;
    }

    message.clear();

    for (auto& column : m_columns) {
        std::string type = column->get_type();
        auto pointer = m_pointers[m_global_id_to_local_id[column->get_id()]];

        if (type == "string") {
            yyjson_mut_doc_ptr_set(
                    m_doc,
                    pointer.c_str(),
                    yyjson_mut_str(
                            m_doc,
                            std::get<std::string>(column->extract_value(m_cur_message)).c_str()
                    )
            );
        } else if (type == "int") {
            yyjson_mut_doc_ptr_set(
                    m_doc,
                    pointer.c_str(),
                    yyjson_mut_int(m_doc, std::get<int64_t>(column->extract_value(m_cur_message)))
            );
        } else if (type == "float") {
            yyjson_mut_doc_ptr_set(
                    m_doc,
                    pointer.c_str(),
                    yyjson_mut_real(m_doc, std::get<double>(column->extract_value(m_cur_message)))
            );
        } else if (type == "bool") {
            yyjson_mut_doc_ptr_set(
                    m_doc,
                    pointer.c_str(),
                    yyjson_mut_bool(
                            m_doc,
                            std::get<uint8_t>(column->extract_value(m_cur_message)) != 0
                    )
            );
        } else if (type == "array") {
            std::string json_string = std::get<std::string>(column->extract_value(m_cur_message));
            yyjson_doc* doc = yyjson_read(json_string.c_str(), json_string.length(), 0);
            yyjson_val* root = yyjson_doc_get_root(doc);
            yyjson_mut_doc_ptr_set(m_doc, pointer.c_str(), yyjson_val_mut_copy(m_doc, root));
        }
    }

    message = yyjson_mut_write(m_doc, 0, nullptr);

    if (message.back() != '\n') {
        message += '\n';
    }

    m_cur_message++;
    return true;
}

// same as malloc(size)
static void* priv_malloc(void* ctx, size_t size) {
    return mi_malloc(size);
}

// same as realloc(ptr, size)
// `old_size` is the size of the originally allocated memory
static void* priv_realloc(void* ctx, void* ptr, size_t old_size, size_t size) {
    return mi_realloc(ptr, size);
}

// same as free(ptr)
static void priv_free(void* ctx, void* ptr) {
    mi_free(ptr);
}

// the allocator object
static yyjson_alc const PRIV_ALC = {
        priv_malloc,
        priv_realloc,
        priv_free,
        NULL  // `ctx` which will be passed into the functions above
};

bool SchemaReader::get_next_message(std::string& message, FilterClass* filter) {
    while (m_cur_message < m_num_messages) {
        if (false == filter->filter(m_cur_message, m_extracted_values)) {
            m_cur_message++;
            continue;
        }

        for (auto& column : m_columns) {
            std::string type = column->get_type();
            auto pointer = m_pointers[m_global_id_to_local_id[column->get_id()]];

            if (type == "string") {
                yyjson_mut_doc_ptr_set(
                        m_doc,
                        pointer.c_str(),
                        yyjson_mut_str(
                                m_doc,
                                std::get<std::string>(m_extracted_values.at(column->get_id()))
                                        .c_str()
                        )
                );
            } else if (type == "int") {
                yyjson_mut_doc_ptr_set(
                        m_doc,
                        pointer.c_str(),
                        yyjson_mut_int(
                                m_doc,
                                std::get<int64_t>(m_extracted_values.at(column->get_id()))
                        )
                );
            } else if (type == "float") {
                yyjson_mut_doc_ptr_set(
                        m_doc,
                        pointer.c_str(),
                        yyjson_mut_real(
                                m_doc,
                                std::get<double>(m_extracted_values.at(column->get_id()))
                        )
                );
            } else if (type == "bool") {
                yyjson_mut_doc_ptr_set(
                        m_doc,
                        pointer.c_str(),
                        yyjson_mut_bool(
                                m_doc,
                                std::get<uint8_t>(m_extracted_values.at(column->get_id())) != 0
                        )
                );
            } else if (type == "array") {
                std::string json_string
                        = std::get<std::string>(m_extracted_values.at(column->get_id()));
                yyjson_doc* doc = yyjson_read(json_string.c_str(), json_string.length(), 0);
                yyjson_val* root = yyjson_doc_get_root(doc);
                yyjson_mut_doc_ptr_set(m_doc, pointer.c_str(), yyjson_val_mut_copy(m_doc, root));
            }
        }
        
        message = yyjson_mut_write_opts(m_doc, 0, &PRIV_ALC, nullptr, nullptr);

        if (message.back() != '\n') {
            message += '\n';
        }

        m_cur_message++;
        return true;
    }

    return false;
}

void SchemaReader::initialize_filter(FilterClass* filter) {
    filter->init(this, m_schema_id, m_columns);
}

void SchemaReader::generate_local_tree(int32_t global_id) {
    auto node = m_global_schema_tree->get_node(global_id);
    int32_t parent_id = node->get_parent_id();

    if (parent_id != -1 && m_global_id_to_local_id.find(parent_id) == m_global_id_to_local_id.end())
    {
        generate_local_tree(parent_id);
    }

    int32_t local_id = m_local_schema_tree->add_node(
            parent_id == -1 ? -1 : m_global_id_to_local_id[parent_id],
            node->get_type(),
            node->get_key_name()
    );
    m_global_id_to_local_id[global_id] = local_id;
}

void SchemaReader::generate_json_template(int32_t id, std::string& json_pointer) {
    auto node = m_local_schema_tree->get_node(id);
    auto children_ids = node->get_children_ids();

    for (int32_t child_id : children_ids) {
        auto child_node = m_local_schema_tree->get_node(child_id);
        std::string const& key = child_node->get_key_name();
        switch (child_node->get_type()) {
            case NodeType::OBJECT: {
                std::string json_pointer_string = get_json_pointer_string(key);
                json_pointer += json_pointer_string + "/";
                generate_json_template(child_id, json_pointer);
                json_pointer.erase(json_pointer.length() - json_pointer_string.length() - 1);
                break;
            }
            case NodeType::ARRAY: {
                std::string json_pointer_string = get_json_pointer_string(key);
                m_pointers[child_id] = json_pointer + json_pointer_string;
                break;
            }
            case NodeType::INTEGER: {
                std::string json_pointer_string = get_json_pointer_string(key);
                m_pointers[child_id] = json_pointer + json_pointer_string;
                break;
            }
            case NodeType::FLOAT: {
                std::string json_pointer_string = get_json_pointer_string(key);
                m_pointers[child_id] = json_pointer + json_pointer_string;
                break;
            }
            case NodeType::BOOLEAN: {
                std::string json_pointer_string = get_json_pointer_string(key);
                m_pointers[child_id] = json_pointer + json_pointer_string;
                break;
            }
            case NodeType::CLPSTRING: {
                std::string json_pointer_string = get_json_pointer_string(key);
                m_pointers[child_id] = json_pointer + json_pointer_string;
                break;
            }
            case NodeType::VARSTRING: {
                std::string json_pointer_string = get_json_pointer_string(key);
                m_pointers[child_id] = json_pointer + json_pointer_string;
                break;
            }
            case NodeType::DATESTRING: {
                std::string json_pointer_string = get_json_pointer_string(key);
                m_pointers[child_id] = json_pointer + json_pointer_string;
                break;
            }
            case NodeType::FLOATDATESTRING: {
                std::string json_pointer_string = get_json_pointer_string(key);
                m_pointers[child_id] = json_pointer + json_pointer_string;
                break;
            }
            case NodeType::NULLVALUE: {
                std::string json_pointer_string = json_pointer + get_json_pointer_string(key);
                yyjson_mut_doc_ptr_set(m_doc, json_pointer_string.c_str(), yyjson_mut_null(m_doc));
                break;
            }
        }
    }
}

std::string SchemaReader::get_json_pointer_string(std::string const& s) {
    std::string res = s;
    size_t start_pos = 0;
    while ((start_pos = res.find('~', start_pos)) != std::string::npos) {
        res.replace(start_pos, 1, "~0");
        start_pos += 2;
    }

    start_pos = 0;
    while ((start_pos = res.find('/', start_pos)) != std::string::npos) {
        res.replace(start_pos, 1, "~1");
        start_pos += 2;
    }

    return res;
}
}  // namespace clp_structured
