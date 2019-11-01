<template>
  <div id="app" class="layout">
    <Layout>
      <header :style="{width: '100%'}">
        <b-view></b-view>
      </header>
      <layout :style="{margin: '0px 0 0 0', background: '#fff', minHeight: '1500px'}">
        <content :style="{background: '#eee',width: '80%'}">
          <router-view></router-view>
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
  .layout{
    border: 0px solid #d7dde4;
    background: #f5f7f9;
    position: relative;
    border-radius: 4px;
    overflow: hidden;
  }

  .layout-footer-center{
    text-align: center;
  }
</style>
