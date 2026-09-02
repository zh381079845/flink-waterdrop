const state = {
  nodes: [],
  connections: [],
  selectedNode: null,
  flowConfiguration: null
}

const mutations = {
  ADD_NODE(state, node) {
    state.nodes.push(node)
  },
  
  REMOVE_NODE(state, nodeId) {
    state.nodes = state.nodes.filter(node => node.id !== nodeId)
    // 同时移除相关的连接线
    state.connections = state.connections.filter(
      conn => conn.sourceNodeId !== nodeId && conn.targetNodeId !== nodeId
    )
  },
  
  UPDATE_NODE_CONFIG(state, { nodeId, config }) {
    const node = state.nodes.find(n => n.id === nodeId)
    if (node) {
      node.config = { ...node.config, ...config }
    }
  },
  
  ADD_CONNECTION(state, connection) {
    state.connections.push(connection)
  },
  
  REMOVE_CONNECTION(state, connectionId) {
    state.connections = state.connections.filter(conn => conn.id !== connectionId)
  },
  
  SET_SELECTED_NODE(state, node) {
    state.selectedNode = node
  },
  
  SET_FLOW_CONFIGURATION(state, config) {
    state.flowConfiguration = config
  },
  
  CLEAR_DESIGNER(state) {
    state.nodes = []
    state.connections = []
    state.selectedNode = null
    state.flowConfiguration = null
  }
}

const actions = {
  addNode({ commit }, node) {
    commit('ADD_NODE', node)
  },
  
  removeNode({ commit }, nodeId) {
    commit('REMOVE_NODE', nodeId)
  },
  
  updateNodeConfig({ commit }, payload) {
    commit('UPDATE_NODE_CONFIG', payload)
  },
  
  addConnection({ commit }, connection) {
    commit('ADD_CONNECTION', connection)
  },
  
  removeConnection({ commit }, connectionId) {
    commit('REMOVE_CONNECTION', connectionId)
  },
  
  selectNode({ commit }, node) {
    commit('SET_SELECTED_NODE', node)
  },
  
  saveFlowConfiguration({ commit }, config) {
    commit('SET_FLOW_CONFIGURATION', config)
  },
  
  clearDesigner({ commit }) {
    commit('CLEAR_DESIGNER')
  }
}

const getters = {
  allNodes: state => state.nodes,
  allConnections: state => state.connections,
  selectedNode: state => state.selectedNode,
  flowConfiguration: state => state.flowConfiguration,
  getNodeById: (state) => (id) => {
    return state.nodes.find(node => node.id === id)
  },
  getConnectionsByNode: (state) => (nodeId) => {
    return state.connections.filter(
      conn => conn.sourceNodeId === nodeId || conn.targetNodeId === nodeId
    )
  }
}

export default {
  namespaced: true,
  state,
  mutations,
  actions,
  getters
}