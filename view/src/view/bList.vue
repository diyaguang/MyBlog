<template>
    <card class="card" shadow>
    <List item-layout="vertical" split="false">
        <ListItem v-for="item in articleList" :key="item.title">
            <ListItemMeta :avatar="item.avatar" :title="item.title" :description="item.description" />
            {{ item.content }}
            <template slot="action">
                <li>
                    <Icon type="ios-star-outline" /> 123
                </li>
                <li>
                    <Icon type="ios-thumbs-up-outline" /> 234
                </li>
                <li>
                    <Icon type="ios-chatbubbles-outline" /> 345
                </li>
            </template>
            <template slot="extra">
                <img src="https://dev-file.iviewui.com/5wxHCQMUyrauMCGSVEYVxHR5JmvS7DpH/large" style="width: 280px">
            </template>
        </ListItem>
    </List>
        <Divider />
        <Page :total="100" show-sizer show-elevator  />
    </card>
</template>

<script>
    export default {
        name: "bList",
        data(){
            return {
                articleList: [{
                    title:'测试数据1',
                    avatar: 'avatar',
                    description: '描述信息',
                    content: '测试内容'
                },
                    {
                        title:'测试数据2',
                        avatar: 'avatar',
                        description: '描述信息',
                        content: '测试内容'
                    }
                    ,
                    {
                        title:'测试数据3',
                        avatar: 'avatar',
                        description: '描述信息',
                        content: '测试内容'
                    }
                ]
            }
        }
    }
</script>

<style scoped>
    .card{
        margin: 10px 10px;
    }
</style>
