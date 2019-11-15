<template>
    <div id="app" class="layout">
        <Layout>
            <header :style="{width: '100%'}">
                <b-view></b-view>
            </header>
            <layout :style="{margin: '0px 0 0 0', background: '#fff', minHeight: '1500px'}">
                <content :style="{background: '#eee',width: '80%'}">
                    <transition name="slide-fade">
                        <router-view></router-view>
                    </transition>
                </content>
                <sider :style="{background: '#eee', position: 'initial', width: '20%', maxWidth: '400px', flex: '0 0 0 0'}">
                    <div>
                        <b-author></b-author>
                        <b-category></b-category>
                        <b-top-comments></b-top-comments>
                    </div>
                </sider>
            </layout>
        </Layout>
    </div>
</template>

<script>

    import BView from "@/view/bView";
    import BAuthor from "@/view/bAuthor";
    import BCategory from "@/view/bCategory";
    import BTopComments from "@/view/bTopComments";

    export default {
        name: 'app',
        components: {
            BTopComments,
            BCategory,
            BAuthor,
            BView

        }
    }
</script>

<style scoped>
    .layout {
        border: 0px solid #d7dde4;
        background: #f5f7f9;
        position: relative;
        border-radius: 4px;
        overflow: hidden;
    }

    .layout-footer-center {
        text-align: center;
    }
    .slide-fade-enter-active{
        transition: all .3s ease;
    }
    .slide-fade-leave-active{
        transition: all .3s cubic-bezier(1.0,0.5,0.8,1.0);
    }
    .slide-fade-enter,.slide-fade-leave-active{
        transform: translateX(-430px);
        opacity: 0;
    }
</style>
