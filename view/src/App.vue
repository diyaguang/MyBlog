<template>
  <div id="app">

    <el-container>
      <el-header>
        <search-header></search-header>
      </el-header>
      <el-container>
        <el-main>
          <router-view/>xxx
        </el-main>
        <el-aside width="200px">Aside</el-aside>
      </el-container>
    </el-container>


  </div>
</template>

<script>
import SearchHeader from "./view/SearchHeader";
export default {
  name: 'App',
    components: {SearchHeader}
}
</script>

<style>
#app {
  font-family: 'Avenir', Helvetica, Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  text-align: center;
  color: #2c3e50;
  margin-top: 60px;
}

.el-row {
  margin-bottom: 20px;
}
</style>
