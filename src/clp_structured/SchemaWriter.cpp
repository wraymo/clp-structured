#include "SchemaWriter.hpp"

#include <utility>

namespace clp_structured {
void SchemaWriter::open(std::string path, int compression_level) {
    m_path = std::move(path);
    m_compression_level = compression_level;
}

void SchemaWriter::close() {
    m_compressor.close();
    m_file_writer.close();

    for (auto i : m_columns) {
        delete i;
    }

    m_columns.clear();
}

void SchemaWriter::append_column(BaseColumnWriter* column_writer) {
    m_columns.push_back(column_writer);
}

size_t SchemaWriter::append_message(ParsedMessage& message) {
    int count = 0;
    size_t size, total_size;
    size = total_size = 0;
    for (auto& i : message.get_content()) {
        m_columns[count]->add_value(i.second, size);
        total_size += size;
        count++;
    }

    m_num_messages++;
    return total_size;
}

void SchemaWriter::store() {
    m_file_writer.open(m_path, FileWriter::OpenMode::CreateForWriting);
    m_file_writer.write_numeric_value(m_num_messages);
    m_compressor.open(m_file_writer, m_compression_level);

    for (auto& writer : m_columns) {
        writer->store(m_compressor);
        //        compressor_.Write(writer->GetData(), writer->GetSize());
    }
}

SchemaWriter::~SchemaWriter() {
    for (auto i : m_columns) {
        delete i;
    }
}

void SchemaWriter::update_schema(std::vector<std::pair<int32_t, int32_t>> const& updates) {
    std::vector<BaseColumnWriter*> new_columns;
    for (BaseColumnWriter* writer : m_columns) {
        int32_t column_id = writer->get_id();
        int32_t new_column_id = -1;
        for (auto& update : updates) {
            if (update.first == column_id) {
                new_column_id = update.second;
                break;
            }
        }

        if (new_column_id != -1) {
            // for now all updates are cardinality one updates
            // which means we can simply remove the column
            delete writer;
            continue;
        }
        new_columns.push_back(writer);
    }

    m_columns = std::move(new_columns);
}

}  // namespace clp_structured
