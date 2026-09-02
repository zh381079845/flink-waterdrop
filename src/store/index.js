import { createStore } from 'vuex'
import designer from './modules/designer'
import jobs from './modules/jobs'
import templates from './modules/templates'

export default createStore({
  modules: {
    designer,
    jobs,
    templates
  }
})