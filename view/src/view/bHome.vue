<template>
    <card class="card" shadow>
        <List item-layout="vertical">
            <div slot="header">
                <div class="swiper-container" ref="slider">
                    <div class="swiper-wrapper" >
                        <router-link class="swiper-slide" v-for="slide in slides" :key="slide.id" tag="div" :to="{name: 'article',params:{id:slide.id} }">
                            <img :src="slide.img_url"/>
                        </router-link>
                    </div>
                    <!-- 如果需要分页器 -->
                    <div class="swiper-pagination" ref="pagination"></div>
                </div>
            </div>
            <ListItem v-for="item in articleList" :key="item.articleID">
                <ListItemMeta :avatar="item.avatar" :description="item.description">
                    <div slot="title">
                        <router-link :to="{name:'article',params:{id:item.articleID}}">{{ item.title }}</router-link>
                    </div>
                </ListItemMeta>
                {{ item.content }}'
                <template slot="action">
                    <li>
                        <Icon type="ios-star-outline"/>
                        123
                    </li>
                    <li>
                        <Icon type="ios-thumbs-up-outline"/>
                        234
                    </li>
                    <li>
                        <Icon type="ios-chatbubbles-outline"/>
                        345
                    </li>
                </template>
                <template v-if="item.showPic" slot="extra">
                    <img src="https://dev-file.iviewui.com/5wxHCQMUyrauMCGSVEYVxHR5JmvS7DpH/large" style="width: 280px">
                </template>
            </ListItem>
        </List>
        <Divider/>
        <Page :total=recordTotal show-sizer show-elevator/>
    </card>
</template>

<script>
    import swiper from 'swiper'
    import 'swiper/css/swiper.css'

    export default {
        name: "bHome",
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
                slides:[
                    {id:1,img_url:'https://dev-file.iviewui.com/5wxHCQMUyrauMCGSVEYVxHR5JmvS7DpH/large'},
                    {id:2,img_url:'https://dev-file.iviewui.com/5wxHCQMUyrauMCGSVEYVxHR5JmvS7DpH/large'},
                    {id:3,img_url:'https://dev-file.iviewui.com/5wxHCQMUyrauMCGSVEYVxHR5JmvS7DpH/large'}
                ],
                recordTotal: 20,
                currentPage: 0
            }
        },
        computed: {
            /*
            recordTotal:function(){
                return this.articleList.size;
            }
            */
        },
        method: {
            getListData: function () {
                //
            }
        },
        mounted() {
            new swiper(this.$refs.slider, {
                pagination: this.$refs.pagination,
                paginationClickable: true,
                spaceBetween: 30,
                centeredSlides: true,
                autoplay: 2500,
                autoplayDisableOnInteraction: false
            })
        }
    }
</script>

<style scoped>
    .card {
        margin: 10px 10px;
    }
</style>
