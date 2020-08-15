import axios from 'axios'

let http = axios.create({
  withCredentials: true,
  headers:{
    'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8'
  },
  transformRequest:[function (data) {
    let newData = '';
    for(let k in data){
      if(data.hasOwnProperty(k)===true){
        newData+=encodeURIComponent(k)+'='+encodeURIComponent(data[k])+'&';
      }
    }
    return newData;
  }]
});

function apiAxios(method,url,params,response) {
  http({
    method:method,
    url:url,
    data: method==='POST' || method==='PUT'? params:null,
    params: method==='GET'|| method==='DELETE'? params:null,
  }).then(function (res) {
    response(res);
  }).catch(function (err) {
    response(err);
  })
}

export default {
  get:function (url,param,response) {
    return apiAxios('GET',url,param,response)
  },
  post:function (url,param,response) {
    return apiAxios('POST',url,param,response)
  },
  put:function (url,param,response) {
    return apiAxios('PUT',url,param,response)
  },
  delete:function (url,param,response) {
    return apiAxios('DELETE',url,param,response)
  }
}
