// The Vue build version to load with the `import` command
// (runtime-only or standalone) has been set in webpack.base.conf with an alias.
import Vue from 'vue'
import App from './App'
import router from './router'



import Vuex from 'vuex';

Vue.config.productionTip = false

//设置 ElementUI 引用 ===============================
import ElemtntUI from 'element-ui';
import 'element-ui/lib/theme-chalk/index.css';
Vue.use(ElemtntUI,{size:'mini'});
//==================================================

//设置 axios 引用 ===================================
//import axios from 'axios';
import Api from './js/axiosConfig'
Vue.prototype.$api = Api;
//==================================================

Vue.use(Vuex);
const store = new Vuex.Store({
  state:{
    //
  }
});

/* eslint-disable no-new */
new Vue({
  el: '#app',
  router,
  store:store,
  components: { App },
  template: '<App/>'
})
