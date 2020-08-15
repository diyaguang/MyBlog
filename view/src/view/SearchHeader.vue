<template>
  <div>
    <el-form :inline="true">
      <el-form-item>
        <el-input placeholder="Search"></el-input>
      </el-form-item>
      <el-form-item>
        <el-button type="primary" >搜索</el-button>
      </el-form-item>
    </el-form>
  </div>
</template>

<script>
    export default {
        name: "SearchHeader"
    }
</script>

<style scoped>

</style>
