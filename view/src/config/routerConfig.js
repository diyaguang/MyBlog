const routers = [
    {
        name: 'Home',
        path: '/',
        meta: {
            title: '文章列表'
        },
        component: (resolve)=>require(['../view/bList.vue'],resolve)
    },
    {
        name:'article',
        path:'/article/:id',
        meta:{
            title:'文章内容'
        },
        component:(resolve)=>require(['../view/bContent.vue'],resolve)
    },
    {
        name:'RobotTest',
        path: '/RobotTest',
        meta:{
            title: '测试自动回复机器人'
        },
        component:(resolve) => require(['../components/RobotTest.vue'],resolve)
    },
    {
        name:'default',
        path: '*',
        redirect: '/RobotTest',
        meta: {
            title: '文章列表'
        }
    }
];
export default routers;
