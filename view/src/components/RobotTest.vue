<template>
    <div id="">
        <p>
            提问：
            <input v-model="question">
        </p>
        <p>{{ answer }}</p>
    </div>
</template>

<script>
    import axios from 'axios'
    export default {
        name: "RobotTest",
        data() {
            return {
                question: '',
                answer: '你还没有问人家问题呀~'
            }
        },
        watch: {
            question: function () {
                this.answer = '等待发问~~';
                this.getAnswer();
            }
        },
        methods: {
            getAnswer: function () {
                if(this.question.indexOf('?') !== -1){
                    this.answer = '思考中~';
                    let that = this;
                    axios.post('http://robottest.uneedzf.com/api/talk2Robot',{token: '5e727e1f6676a0d1c95d954b491f2d32',message: that.question}).then(function (res) {
                        if(res.data.code === 0){
                            that.answer = res.data.data;
                        }else{
                            that.answer = res.data.message;
                        }
                    }).catch(function (error) {
                        // eslint-disable-next-line no-console
                        console.log(error);
                    });
                }else{
                    this.answer = '一个问题一般由一个 ? 结尾喔~~';
                }
            }
        }
    }
</script>

<style scoped>

</style>