<template>
    <div style="background: #f8f8f9; padding: 20px; border-radius:5px">
        <div v-if="commentList.length === 0" style="text-align: center">暂时没有评论！还不赶紧抢沙发~</div>
        <div v-else>
            <Card style="margin: 10px" :bordered="false">
                <div slot="title">
                    <Row>
                        <i-col span="20">
                            <div style="text-align: left"><p>这里显示用户头像，名称</p></div>
                            <div>显示发表时间</div>
                        </i-col>
                        <i-col span="4" style="float: right">
                            <div style="text-align: right">显示楼层</div>
                            <div style="float: right; margin-top: 5px">
                                <Icon type="ios-undo"/>
                                回复 |
                                <Icon type="ios-create"/>
                                编辑
                            </div>
                        </i-col>
                    </Row>
                </div>

                <Row>
                    <i-col>显示用户回复内容</i-col>
                </Row>
            </Card>
        </div>
    </div>
</template>

<script>
    export default {
        name: "bComments",
        props: {
            articleID: {
                type: String,
                required: false
            }
        },
        data() {
            return {
                commentList: []
            }
        }
    }
</script>

<style scoped>

</style>
