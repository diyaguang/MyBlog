const routers = [
    {
        path: '/',
        meta: {
            title: '文章列表'
        },
        component: (resolve)=>require(['../view/bList.vue'],resolve)
    },
    {
        path: '/RobotTest',
        meta:{
            title: '测试自动回复机器人'
        },
        component:(resolve) => require(['../components/RobotTest.vue'],resolve)
    },
    {
        path: '*',
        redirect: '/RobotTest',
        meta: {
            title: '文章列表'
        }
    }
];
export default routers;
