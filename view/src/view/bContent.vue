<template>
    <Card class="card" shadow>
        <div slot="title">
            <Row>
                <i-col style="text-align: center"><p>这里显示标题内容</p></i-col>
            </Row>
            <Row>
                <i-col>
                    <div style="margin-top: 10px; text-align: right">
                        <template>
                            <Icon type="md-star" />收藏
                            <Divider type="vertical" />
                            <a href="#"><Icon type="md-thumbs-up" />点赞</a>
                            <Divider type="vertical" />
                            <a href="#"><Icon type="md-chatboxes" />评论</a>
                        </template>
                    </div>
                </i-col>
            </Row>
        </div>
        <p>具体内容页面</p>
        <Divider orientation="right" dashed>相关连接</Divider>
        <Row>
            <i-col span="12" style="text-align: left"><p>上一篇</p></i-col>
            <i-col span="12" style="text-align: right"><p>下一篇</p></i-col>
        </Row>
        <Divider orientation="left">评论</Divider>
        <p>Content of card</p>
    </Card>
</template>

<script>
    export default {
        name: "bContent",
        props:{
            articleID:{
                type: String,
                required: true
            }
        }
    }
</script>

<style scoped>
    .card{
        margin: 10px 10px;
    }
</style>
