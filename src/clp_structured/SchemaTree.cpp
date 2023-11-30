#include "SchemaTree.hpp"

#include <stack>

namespace clp_structured {
void SchemaNode::mark_node_value(uint64_t value, std::string const& string_value) {
    switch (m_value_state) {
        case NodeValueState::UNINITIALIZED:
            m_value = value;
            m_string_value = string_value;
            m_value_state = NodeValueState::CARDINALITY_ONE;
            break;
        case NodeValueState::CARDINALITY_ONE:
            if (m_value != value) {
                m_value_state = NodeValueState::CARDINALITY_MANY;
            }
            break;
        default:
            break;
    }
}

int32_t SchemaTree::add_node(int32_t parent_node_id, NodeType type, std::string const& key) {
    std::tuple<int32_t, std::string, NodeType> node_key = {parent_node_id, key, type};
    auto node_it = m_node_map.find(node_key);
    if (node_it != m_node_map.end()) {
        auto node_id = node_it->second;
        m_nodes[node_id]->increase_count();
        return node_id;
    }

    auto node = std::make_shared<SchemaNode>(parent_node_id, m_nodes.size(), key, type);
    node->increase_count();
    m_nodes.push_back(node);
    int32_t node_id = node->get_id();
    if (parent_node_id >= 0) {
        auto parent_node = m_nodes[parent_node_id];
        parent_node->add_child(node_id);
    }
    m_node_map[node_key] = node_id;

    return node_id;
}

std::vector<std::pair<int32_t, int32_t>> SchemaTree::modify_nodes_based_on_frequency() {
    int32_t root_node_id = get_root_node_id();
    auto root_node = get_node(root_node_id);

    std::vector<int32_t> const& children = root_node->get_children_ids();
    auto it = children.begin();
    auto it_end = children.end();
    std::stack<std::vector<int32_t>::const_iterator> it_stack;
    std::stack<std::vector<int32_t>::const_iterator> it_end_stack;

    std::vector<std::pair<int32_t, int32_t>> updates;

    for (; it != it_end || it_stack.size() > 0;) {
        if (it == it_end) {
            it = it_stack.top();
            it_stack.pop();
            it_end = it_end_stack.top();
            it_end_stack.pop();
            continue;
        }

        auto node = get_node(*it);

        if (node->get_state() == NodeValueState::CARDINALITY_ONE) {
            int32_t var_node_id = add_node(*it, NodeType::VARVALUE, node->get_string_var_value());
            updates.push_back({*it, var_node_id});
            ++it;
            continue;
        }

        ++it;
        std::vector<int32_t> const& children = node->get_children_ids();
        if (children.size() > 0) {
            it_stack.push(it);
            it_end_stack.push(it_end);
            it = children.begin();
            it_end = children.end();
        }
    }

    return updates;
}

}  // namespace clp_structured
