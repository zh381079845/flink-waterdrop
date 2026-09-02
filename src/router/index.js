import { createRouter, createWebHistory } from 'vue-router'
import Home from '../views/Home.vue'
import Designer from '../views/Designer.vue'
import JobList from '../views/JobList.vue'
import JobDetail from '../views/JobDetail.vue'

const routes = [
  {
    path: '/',
    name: 'Home',
    component: Home
  },
  {
    path: '/designer',
    name: 'Designer',
    component: Designer
  },
  {
    path: '/jobs',
    name: 'JobList',
    component: JobList
  },
  {
    path: '/job/:id',
    name: 'JobDetail',
    component: JobDetail
  }
]

const router = createRouter({
  history: createWebHistory(),
  routes
})

export default router