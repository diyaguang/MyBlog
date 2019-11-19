<template>
    <card class="card" shadow>
    <List item-layout="vertical" split="false">
        <div slot="header">这里是列表的，想用来显示类别</div>
        <ListItem v-for="item in articleList" :key="item.articleID">
            <ListItemMeta :avatar="item.avatar"  :description="item.description">
                <div slot="title"><router-link :to="{name:'article',params:{id:item.articleID}}">{{ item.title }}</router-link></div>
            </ListItemMeta>
            {{ item.content }}'
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
            <template v-if="item.showPic" slot="extra">
                <img src="https://dev-file.iviewui.com/5wxHCQMUyrauMCGSVEYVxHR5JmvS7DpH/large" style="width: 280px">
            </template>
        </ListItem>
    </List>
        <Divider />
        <Page :total=recordTotal show-sizer show-elevator  />
    </card>
</template>

<script>
    export default {
        name: "bList",
        data() {
            return {
                articleList: [
                    {
                        articleID: '1',
                        title: '测试数据1',
                        avatar: 'avatar',
                        description: '描述信息',
                        content: '测试内容',
                        showPic: false
                    },
                    {
                        articleID: '2',
                        title: '测试数据2',
                        avatar: 'avatar',
                        description: '描述信息',
                        content: '测试内容',
                        showPic: true
                    },
                    {
                        articleID: '3',
                        title: '测试数据3',
                        avatar: 'avatar',
                        description: '描述信息',
                        content: '测试内容',
                        showPic: false
                    }
                ],
                recordTotal: 20,
                currentPage: 0
            }
        },
        computed:{
            /*
            recordTotal:function(){
                return this.articleList.size;
            }
            */
        },
        method:{
            getListData:function(){
                //
            }
        }
    }
</script>

<style scoped>
    .card{
        margin: 10px 10px;
    }
</style>
