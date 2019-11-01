const routers = [
    {
        path: '/',
        meta: {
            title: '文章列表'
        },
        component: (resolve)=>require(['../view/bList.vue'],resolve)
    },
    {
        path: '*',
        redirect: '/'
    }
];
export default routers;
