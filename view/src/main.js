import Vue from 'vue'
import App from './App.vue'
import ViewUI from 'view-design';
import VueRouter from 'vue-router'
import Routers from './config/routerConfig'
import Vuex from 'vuex'

Vue.use(ViewUI);
Vue.use(VueRouter);
Vue.use(Vuex);

import 'view-design/dist/styles/iview.css';
const RouterConfig = {
  mode: 'history',
  routes: Routers
};
const router = new VueRouter(RouterConfig);

const store = new Vuex.Store({
  state: {
    //
  }
});

Vue.config.productionTip = false

new Vue({
  render: h => h(App),
  router: router,
  store: store
}).$mount('#app')
